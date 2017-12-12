#include "aqo.h"

/*****************************************************************************
 *
 *	STORAGE INTERACTION
 *
 * This module is responsible for interaction with the storage of AQO data.
 * It does not provide information protection from concurrent updates.
 *
 *****************************************************************************/

/* For inner use, doesn't interfere with anything */
#define FLOAT8ARRAYOID (FLOAT4ARRAYOID + 1)

HTAB	   *deactivated_queries = NULL;

/*
 * Returns whether the query with given hash is in aqo_queries.
 * If yes, returns the content of the first line with given hash.
 */
bool
find_query(int query_hash,
		   Datum *search_values,
		   bool *search_nulls)
{
	RangeVar   *aqo_queries_table_rv;
	Relation	aqo_queries_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = AccessShareLock;

	Relation	query_index_rel;
	Oid			query_index_rel_oid;
	IndexScanDesc query_index_scan;
	ScanKeyData key;
	LOCKMODE	index_lock = AccessShareLock;

	bool		find_ok = false;

	query_index_rel_oid = RelnameGetRelid("aqo_queries_query_hash_idx");
	if (!OidIsValid(query_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}

	aqo_queries_table_rv = makeRangeVar("public", "aqo_queries", -1);
	aqo_queries_heap = heap_openrv(aqo_queries_table_rv, heap_lock);

	query_index_rel = index_open(query_index_rel_oid, index_lock);
	query_index_scan = index_beginscan(
									   aqo_queries_heap,
									   query_index_rel,
									   SnapshotSelf,
									   1,
									   0
		);
	ScanKeyInit(&key,
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(query_hash)
		);

	index_rescan(query_index_scan, &key, 1, NULL, 0);
	tuple = index_getnext(query_index_scan, ForwardScanDirection);

	find_ok = (tuple != NULL);

	if (find_ok)
		heap_deform_tuple(tuple, aqo_queries_heap->rd_att,
						  search_values, search_nulls);

	index_endscan(query_index_scan);
	index_close(query_index_rel, index_lock);
	heap_close(aqo_queries_heap, heap_lock);

	return find_ok;
}

/* Insert data into a table locally (mustn't be executed on replica) */
static bool
insert_data_for_update_local(Oid updates_heap_oid, Datum *values, bool *nulls)
{
	Relation	updates_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = RowExclusiveLock;

	updates_heap = heap_open(updates_heap_oid, heap_lock);

	if (!OidIsValid(updates_heap_oid))
		return false;

	tuple = heap_form_tuple(RelationGetDescr(updates_heap),
							values, nulls);
	simple_heap_insert(updates_heap, tuple);
	CommandCounterIncrement();

	heap_close(updates_heap, heap_lock);

	return true;
}

/* Create cstring and write there an array of doubles */
static char *
make_vector_string(int nelems, double *arr)
{
	int			reslen = 15 + (1 + 3 + 10) * nelems + 2 * Max(0, nelems - 1);
	char	   *res = palloc(reslen);
	int			curpos,
				printed,
				i;

	curpos = printed = snprintf(res, reslen, "{");
	for (i = 0; i < nelems; ++i)
	{
		printed = snprintf(&res[curpos], reslen - curpos, "%.8e", arr[i]);
		curpos += printed;
		if (i != nelems - 1)
		{
			printed = snprintf(&res[curpos], reslen - curpos, ", ");
			curpos += printed;
		}
	}
	printed = snprintf(&res[curpos], reslen - curpos, "}");
	curpos += printed;
	return res;
}

/* Insert data into a table on master using libpq */
static bool
insert_data_for_update_remote(Oid updates_heap_oid,
							  const char *updates_heap_name,
							  Oid *types, Datum *values, bool *nulls)
{
	Relation	updates_heap;
	PGconn	   *conn;
	PGresult   *res;
	char	   *cmd;
	int			nargs;
	int			i,
				j;
	int			curpos,
				curprinted;
	char	  **str_values;
	int			cmdlen;

	updates_heap = heap_open(updates_heap_oid, NoLock);

	if (!OidIsValid(updates_heap_oid))
		return false;

	nargs = RelationGetDescr(updates_heap)->natts;

	heap_close(updates_heap, NoLock);

	/* Make a connection to the database */
	conn = PQconnectdb(aqo_conninfo);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		PQfinish(conn);
		return false;
	}

	cmdlen = 30 + strlen(updates_heap_name) + 10 +
		2 * Max(0, nargs - 1) + 4 * nargs;
	cmd = palloc(cmdlen);

	curpos = curprinted = snprintf(cmd, cmdlen, "INSERT INTO %s VALUES (",
								   updates_heap_name);
	Assert(0 <= curprinted && curprinted < cmdlen);
	for (i = 0; i < nargs; ++i)
	{
		curprinted = snprintf(&cmd[curpos], cmdlen - curpos, "$%d", i + 1);
		Assert(0 <= curprinted && curprinted < cmdlen - curpos);
		curpos += curprinted;
		if (i == nargs - 1)
			curprinted = snprintf(&cmd[curpos], cmdlen - curpos, ");");
		else
			curprinted = snprintf(&cmd[curpos], cmdlen - curpos, ", ");
		Assert(0 <= curprinted && curprinted < cmdlen - curpos);
		curpos += curprinted;
	}

	str_values = palloc(nargs * sizeof(*str_values));
	for (i = 0; i < nargs; ++i)
	{
		if (nulls[i])
		{
			str_values[i] = palloc(10);
			snprintf(str_values[i], 10, "NULL");
			continue;
		}
		switch (types[i])
		{
			case FLOAT8ARRAYOID:
				if (ARR_NDIM(DatumGetArrayTypeP(values[i])) == 2)
				{
					double	  **arr;
					char	   *curstr;
					int			curlen = 3 + 5;
					int			pos,
								printed;
					int			dim0 = ARR_DIMS(DatumGetArrayTypeP(values[i]))[0];
					int			dim1 = ARR_DIMS(DatumGetArrayTypeP(values[i]))[1];

					if (ARR_DIMS(DatumGetArrayTypeP(values[i]))[1] == 0)
					{
						str_values[i] = palloc(10);
						snprintf(str_values[i], 10, "{}");
						break;
					}
					arr = palloc(sizeof(*arr) * dim0);
					for (j = 0; j < dim0; ++j)
					{
						arr[j] = palloc(sizeof(*arr[j]) * dim1);
					}
					deform_matrix(values[i], arr);
					for (j = 0; j < dim0; ++j)
					{
						curstr = make_vector_string(dim1, arr[j]);
						curlen += strlen(curstr);
						pfree(curstr);
					}
					str_values[i] = palloc(curlen);
					printed = pos = snprintf(str_values[i], curlen, "{");
					for (j = 0; j < dim0; ++j)
					{
						curstr = make_vector_string(dim1, arr[j]);
						printed = snprintf(&str_values[i][pos],
										   curlen - pos, "%s", curstr);
						pos += printed;
						pfree(curstr);
						if (j != dim0 - 1)
						{
							printed = snprintf(&str_values[i][pos],
											   curlen - pos, ", ");
							pos += printed;
						}
					}
					printed = snprintf(&str_values[i][pos], curlen - pos, "}");
					pos += printed;
				}
				else if (ARR_NDIM(DatumGetArrayTypeP(values[i])) == 1)
				{
					double	   *arr;
					int			dim0 = ARR_DIMS(DatumGetArrayTypeP(values[i]))[0];

					arr = palloc(sizeof(*arr) * dim0);
					deform_vector(values[i], arr, NULL);
					str_values[i] = make_vector_string(dim0, arr);
					pfree(arr);
				}
				else
				{
					elog(ERROR, "aqo, insert_data_for_update_remote, "
						 "arg %d: invalid number of dimensions %d", i,
						 ARR_NDIM(DatumGetArrayTypeP(values[i])));
				}
				break;
			case INT4OID:
				str_values[i] = palloc(11);
				snprintf(str_values[i], 11, "%d", DatumGetInt32(values[i]));
				break;
			case INT8OID:
				str_values[i] = palloc(22);
				snprintf(str_values[i], 22, INT64_FORMAT, DatumGetInt64(values[i]));
				break;
			case CSTRINGOID:
				str_values[i] = palloc0(strlen(TextDatumGetCString(values[i])) + 1);
				memcpy(str_values[i], TextDatumGetCString(values[i]),
					   strlen(TextDatumGetCString(values[i])));
				break;
			case BOOLOID:
				str_values[i] = palloc(6);
				if (values[i])
					snprintf(str_values[i], 6, "true");
				else
					snprintf(str_values[i], 6, "false");
				break;
			default:
				elog(ERROR, "aqo, insert_data_for_update_remote, "
					 "arg %d: invalid type %d", i, types[i]);
				break;
		}
		elog(WARNING, "arg %d: %s", i, str_values[i]);
	}

	res = PQexecParams(conn,
					   cmd,
					   nargs,	/* one param */
					   NULL,	/* let the backend deduce param type */
					   (const char *const *) str_values,
					   NULL,	/* don't need param lengths since text */
					   NULL,	/* default to all text params */
					   1);		/* ask for binary results */

	for (i = 0; i < nargs; ++i)
		pfree(str_values[i]);
	pfree(str_values);
	pfree(cmd);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		PQfinish(conn);
		return false;
	}

	PQclear(res);

	PQfinish(conn);

	return true;
}

/*
 * Generic interface for inserting data into a table directly if we are master
 * or remotely into a table on master via libpq if we are replica.
 */
static bool
insert_data_for_update(Oid updates_heap_oid, const char *updates_heap_name,
					   Oid *types, Datum *values, bool *nulls)
{
	if (!RecoveryInProgress())
		return insert_data_for_update_local(updates_heap_oid, values, nulls);
	else
		return insert_data_for_update_remote(updates_heap_oid,
											 updates_heap_name,
											 types, values, nulls);
}

/*
 * Updates entry for the query in aqo_queries table with given fields.
 * Actually we insert the new version into aqo_queries_updates and waiting
 * for background worker to push it into aqo_queries.
 * Returns false if the operation failed, true otherwise.
 */
bool
update_query(int query_hash, bool learn_aqo, bool use_aqo,
			 int fspace_hash, bool auto_tuning)
{
	Oid			aqo_queries_updates_rel_oid;

	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};
	Oid			types[5] = {INT4OID, BOOLOID, BOOLOID, INT4OID, BOOLOID};

	values[0] = Int32GetDatum(query_hash);
	values[1] = BoolGetDatum(learn_aqo);
	values[2] = BoolGetDatum(use_aqo);
	values[3] = Int32GetDatum(fspace_hash);
	values[4] = BoolGetDatum(auto_tuning);

	aqo_queries_updates_rel_oid = RelnameGetRelid("aqo_queries_updates");

	if (!insert_data_for_update(aqo_queries_updates_rel_oid,
								"aqo_queries_updates",
								types, values, nulls))
	{
		disable_aqo_for_query();
		return false;
	}

	return true;
}

/*
 * Creates entry for new query in aqo_queries table with given fields.
 * Actually now it do the same thing as update_query.
 * Returns false if the operation failed, true otherwise.
 */
bool
add_query(int query_hash, bool learn_aqo, bool use_aqo,
		  int fspace_hash, bool auto_tuning)
{
	return update_query(query_hash, learn_aqo, use_aqo,
						fspace_hash, auto_tuning);
}

/*
 * Creates entry for new query in aqo_query_texts table with given fields.
 * Actually we insert the text into aqo_query_texts_updates and waiting
 * for background worker to push it into aqo_query_texts.
 * Returns false if the operation failed, true otherwise.
 */
bool
add_query_text(int query_hash, const char *query_text)
{
	Oid			aqo_query_texts_updates_rel_oid;

	Datum		values[2];
	bool		nulls[2] = {false, false};
	Oid			types[2] = {INT4OID, CSTRINGOID};

	values[0] = Int32GetDatum(query_hash);
	values[1] = CStringGetTextDatum(query_text);

	aqo_query_texts_updates_rel_oid = RelnameGetRelid("aqo_query_texts_updates");
	if (!insert_data_for_update(aqo_query_texts_updates_rel_oid,
								"aqo_query_texts_updates",
								types, values, nulls))
	{
		disable_aqo_for_query();
		return false;
	}

	return true;
}

/*
 * Loads feature subspace (fss) from table aqo_data into memory.
 * The last column of the returned matrix is for target values of objects.
 * Returns false if the operation failed, true otherwise.
 *
 * 'fss_hash' is the hash of feature subspace which is supposed to be loaded
 * 'ncols' is the number of clauses in the feature subspace
 * 'matrix' is an allocated memory for matrix with the size of aqo_K rows
 *			and nhashes columns
 * 'targets' is an allocated memory with size aqo_K for target values
 *			of the objects
 * 'rows' is the pointer in which the function stores actual number of
 *			objects in the given feature space
 */
bool
load_fss(int fss_hash, int ncols,
		 double **matrix, double *targets, int *rows)
{
	RangeVar   *aqo_data_table_rv;
	Relation	aqo_data_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = AccessShareLock;

	Relation	data_index_rel;
	Oid			data_index_rel_oid;
	IndexScanDesc data_index_scan;
	ScanKeyData *key;
	LOCKMODE	index_lock = AccessShareLock;

	Datum		values[5];
	bool		nulls[5];

	bool		success = true;

	data_index_rel_oid = RelnameGetRelid("aqo_fss_access_idx");
	if (!OidIsValid(data_index_rel_oid))
	{
		disable_aqo_for_query();
		return false;
	}

	aqo_data_table_rv = makeRangeVar("public", "aqo_data", -1);
	aqo_data_heap = heap_openrv(aqo_data_table_rv, heap_lock);

	data_index_rel = index_open(data_index_rel_oid, index_lock);
	data_index_scan = index_beginscan(
									  aqo_data_heap,
									  data_index_rel,
									  SnapshotSelf,
									  2,
									  0
		);

	key = palloc(sizeof(*key) * 2);
	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(fspace_hash)
		);
	ScanKeyInit(&key[1],
				2,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(fss_hash)
		);

	index_rescan(data_index_scan, key, 2, NULL, 0);

	tuple = index_getnext(data_index_scan, ForwardScanDirection);

	if (tuple != NULL)
	{
		heap_deform_tuple(tuple, aqo_data_heap->rd_att, values, nulls);

		if (DatumGetInt32(values[2]) == ncols)
		{
			deform_matrix(values[3], matrix);
			deform_vector(values[4], targets, rows);
		}
		else
		{
			elog(WARNING, "unexpected number of features for hash (%d, %d):\
						   expected %d features, obtained %d", fspace_hash,
				 fss_hash, ncols, DatumGetInt32(values[2]));
			success = false;
		}
	}
	else
		success = false;

	index_endscan(data_index_scan);

	index_close(data_index_rel, index_lock);
	heap_close(aqo_data_heap, heap_lock);

	pfree(key);

	return success;
}

/*
 * Updates the specified line in the specified feature subspace.
 * Returns false if the operation failed, true otherwise.
 *
 * 'fss_hash' specifies the feature subspace
 * 'nrows' x 'ncols' is the shape of 'matrix'
 * 'targets' is vector of size 'nrows'
 * 'old_nrows' is previous number of rows in matrix
 * 'changed_rows' is an integer list of changed lines
 */
bool
update_fss(int fss_hash, int nrows, int ncols, double **matrix, double *targets,
		   int old_nrows, List *changed_rows)
{
	Oid			aqo_data_updates_rel_oid;

	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};
	Oid			types[5] = {INT4OID, INT4OID, INT4OID, FLOAT8ARRAYOID, FLOAT8ARRAYOID};

	values[0] = Int32GetDatum(fspace_hash);
	values[1] = Int32GetDatum(fss_hash);
	values[2] = Int32GetDatum(ncols);
	values[3] = PointerGetDatum(form_matrix(matrix, nrows, ncols));
	values[4] = PointerGetDatum(form_vector(targets, nrows));

	aqo_data_updates_rel_oid = RelnameGetRelid("aqo_data_updates");
	if (!insert_data_for_update(aqo_data_updates_rel_oid,
								"aqo_data_updates",
								types, values, nulls))
	{
		disable_aqo_for_query();
		return false;
	}

	return true;
}

/*
 * Returns QueryStat for the given query_hash. Returns empty QueryStat if
 * no statistics is stored for the given query_hash in table aqo_query_stat.
 * Returns NULL and executes disable_aqo_for_query if aqo_query_stat
 * is not found.
 */
QueryStat *
get_aqo_stat(int query_hash)
{
	RangeVar   *aqo_stat_table_rv;
	Relation	aqo_stat_heap;
	HeapTuple	tuple;
	LOCKMODE	heap_lock = AccessShareLock;

	Relation	stat_index_rel;
	Oid			stat_index_rel_oid;
	IndexScanDesc stat_index_scan;
	ScanKeyData key;
	LOCKMODE	index_lock = AccessShareLock;

	Datum		values[9];
	bool		nulls[9];

	QueryStat  *stat = palloc_query_stat();

	stat_index_rel_oid = RelnameGetRelid("aqo_query_stat_idx");
	if (!OidIsValid(stat_index_rel_oid))
	{
		disable_aqo_for_query();
		return NULL;
	}

	aqo_stat_table_rv = makeRangeVar("public", "aqo_query_stat", -1);
	aqo_stat_heap = heap_openrv(aqo_stat_table_rv, heap_lock);

	stat_index_rel = index_open(stat_index_rel_oid, index_lock);
	stat_index_scan = index_beginscan(
									  aqo_stat_heap,
									  stat_index_rel,
									  SnapshotSelf,
									  1,
									  0
		);

	ScanKeyInit(&key,
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(query_hash)
		);

	index_rescan(stat_index_scan, &key, 1, NULL, 0);

	tuple = index_getnext(stat_index_scan, ForwardScanDirection);

	if (tuple != NULL)
	{
		heap_deform_tuple(tuple, aqo_stat_heap->rd_att, values, nulls);

		deform_vector(values[1], stat->execution_time_with_aqo, &stat->execution_time_with_aqo_size);
		deform_vector(values[2], stat->execution_time_without_aqo, &stat->execution_time_without_aqo_size);
		deform_vector(values[3], stat->planning_time_with_aqo, &stat->planning_time_with_aqo_size);
		deform_vector(values[4], stat->planning_time_without_aqo, &stat->planning_time_without_aqo_size);
		deform_vector(values[5], stat->cardinality_error_with_aqo, &stat->cardinality_error_with_aqo_size);
		deform_vector(values[6], stat->cardinality_error_without_aqo, &stat->cardinality_error_without_aqo_size);
		stat->executions_with_aqo = DatumGetInt64(values[7]);
		stat->executions_without_aqo = DatumGetInt64(values[8]);
	}

	index_endscan(stat_index_scan);

	index_close(stat_index_rel, index_lock);
	heap_close(aqo_stat_heap, heap_lock);

	return stat;
}

/*
 * Saves given QueryStat for the given query_hash.
 * Executes disable_aqo_for_query if aqo_query_stat is not found.
 */
void
update_aqo_stat(int query_hash, QueryStat * stat)
{
	Oid			aqo_query_stat_updates_rel_oid;

	Datum		values[9];
	bool		nulls[9] = {false, false, false,
		false, false, false,
	false, false, false};
	Oid			types[9] = {INT4OID, FLOAT8ARRAYOID, FLOAT8ARRAYOID,
		FLOAT8ARRAYOID, FLOAT8ARRAYOID, FLOAT8ARRAYOID,
	FLOAT8ARRAYOID, INT8OID, INT8OID};

	values[0] = Int32GetDatum(query_hash);
	values[1] = PointerGetDatum(form_vector(stat->execution_time_with_aqo, stat->execution_time_with_aqo_size));
	values[2] = PointerGetDatum(form_vector(stat->execution_time_without_aqo, stat->execution_time_without_aqo_size));
	values[3] = PointerGetDatum(form_vector(stat->planning_time_with_aqo, stat->planning_time_with_aqo_size));
	values[4] = PointerGetDatum(form_vector(stat->planning_time_without_aqo, stat->planning_time_without_aqo_size));
	values[5] = PointerGetDatum(form_vector(stat->cardinality_error_with_aqo, stat->cardinality_error_with_aqo_size));
	values[6] = PointerGetDatum(form_vector(stat->cardinality_error_without_aqo, stat->cardinality_error_without_aqo_size));
	values[7] = Int64GetDatum(stat->executions_with_aqo);
	values[8] = Int64GetDatum(stat->executions_without_aqo);

	aqo_query_stat_updates_rel_oid = RelnameGetRelid("aqo_query_stat_updates");
	if (!insert_data_for_update(aqo_query_stat_updates_rel_oid, "aqo_query_stat_updates", types, values, nulls))
	{
		disable_aqo_for_query();
	}
}


/* Creates a storage for hashes of deactivated queries */
void
init_deactivated_queries_storage(void)
{
	HASHCTL		hash_ctl;

	/* Create the hashtable proper */
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int);
	hash_ctl.entrysize = sizeof(int);
	deactivated_queries = hash_create("aqo_deactivated_queries",
									  128,		/* start small and extend */
									  &hash_ctl,
									  HASH_ELEM);
}

/* Destroys the storage for hash of deactivated queries */
void
fini_deactivated_queries_storage(void)
{
	hash_destroy(deactivated_queries);
	deactivated_queries = NULL;
}

/* Checks whether the query with given hash is deactivated */
bool
query_is_deactivated(int query_hash)
{
	bool		found;

	hash_search(deactivated_queries, &query_hash, HASH_FIND, &found);
	return found;
}

/* Adds given query hash into the set of hashes of deactivated queries*/
void
add_deactivated_query(int query_hash)
{
	bool		found;

	hash_search(deactivated_queries, &query_hash, HASH_ENTER, &found);
}
