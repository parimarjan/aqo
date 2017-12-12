#include "aqo.h"

/*****************************************************************************
 *
 *	BACKGROUND WORKER
 *
 * This functions provide the registration and execution of background worker.
 * The background worker is responsible for the data aggregation.
 *
 *****************************************************************************/

 /* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

typedef void (*ScanKeyBuild) (int natts, int update_natts, Datum *values,
							   bool *isnull, ScanKeyData **keys, int *nkeys);

typedef void (*DoUpdate) (int natts, int update_natts, Datum *values,
						bool *isnull, bool *do_replace, Datum *update_values,
									  bool *update_isnull);


/* Signal handler for SIGTERM */
static void
worker_aqo_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* Signal handler for SIGHUP */
static void
worker_aqo_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* Apply updates from aqo_<sometable>_update to aqo_<sometable> */
static bool
update_aqo(Oid aqo_heap_oid, Oid aqo_update_heap_oid, Oid aqo_index_rel_oid,
		   ScanKeyBuild build_scan_keys, DoUpdate do_update)
{
	Relation	aqo_heap;
	Relation	aqo_update_heap;
	Relation	aqo_index_rel;

	HeapTuple	htup;
	HeapTuple	nw_htup;
	HeapTuple	htup_update;

	Snapshot	snapshot;
	HeapScanDesc scan;

	LOCKMODE	heap_lock = RowExclusiveLock;
	LOCKMODE	index_lock = RowExclusiveLock;

	int			update_natts;
	int			natts;
	Datum	   *update_values;
	bool	   *update_isnull;
	Datum	   *values;
	bool	   *isnull;
	bool	   *do_replace;

	bool		did_update = false;

	if (!OidIsValid(aqo_index_rel_oid) || !OidIsValid(aqo_heap_oid) ||
		!OidIsValid(aqo_update_heap_oid))
		return false;

	aqo_index_rel = index_open(aqo_index_rel_oid, index_lock);
	aqo_heap = heap_open(aqo_heap_oid, heap_lock);
	aqo_update_heap = heap_open(aqo_update_heap_oid, heap_lock);

	update_natts = RelationGetDescr(aqo_update_heap)->natts;
	update_values = palloc(sizeof(*update_values) * update_natts);
	update_isnull = palloc(sizeof(*update_isnull) * update_natts);

	natts = RelationGetDescr(aqo_heap)->natts;
	values = palloc(sizeof(*values) * natts);
	isnull = palloc(sizeof(*isnull) * natts);
	do_replace = palloc(sizeof(*do_replace) * natts);

	snapshot = RegisterSnapshot(GetTransactionSnapshot());

	scan = heap_beginscan(aqo_update_heap, snapshot, 0, NULL);

	while ((htup_update = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Snapshot	inner_snapshot;
		IndexScanDesc aqo_index_scan;
		ScanKeyData *keys;
		int			nkeys;

		heap_deform_tuple(htup_update, RelationGetDescr(aqo_update_heap),
						  update_values, update_isnull);

		build_scan_keys(natts, update_natts, update_values,
						update_isnull, &keys, &nkeys);

		inner_snapshot = RegisterSnapshot(GetTransactionSnapshot());
		aqo_index_scan = index_beginscan(
										 aqo_heap,
										 aqo_index_rel,
										 inner_snapshot,
										 nkeys,
										 0
			);

		index_rescan(aqo_index_scan, keys, nkeys, NULL, 0);
		htup = index_getnext(aqo_index_scan, ForwardScanDirection);

		if (htup == NULL)
		{
			do_update(natts, update_natts, values, isnull,
					  NULL, update_values, update_isnull);
			htup = heap_form_tuple(RelationGetDescr(aqo_heap),
								   values, isnull);
			simple_heap_insert(aqo_heap, htup);
			index_insert(aqo_index_rel,
						 values, isnull,
						 &(htup->t_self),
						 aqo_heap,
#if PG_VERSION_NUM < 100000
						 UNIQUE_CHECK_YES);
#else
						 UNIQUE_CHECK_YES,
						 NULL);
#endif
			CommandCounterIncrement();
		}
		else
		{
			heap_deform_tuple(htup, RelationGetDescr(aqo_heap), values, isnull);
			do_update(natts, update_natts, values, isnull,
					  do_replace, update_values, update_isnull);
			nw_htup = heap_modify_tuple(htup, RelationGetDescr(aqo_heap),
										values, isnull, do_replace);
			simple_heap_update(aqo_heap, &(nw_htup->t_self), nw_htup);
			CommandCounterIncrement();
		}

		simple_heap_delete(aqo_update_heap, &(htup_update->t_self));
		CommandCounterIncrement();

		did_update = true;

		index_endscan(aqo_index_scan);
		UnregisterSnapshot(inner_snapshot);

		pfree(keys);
	}

	heap_endscan(scan);

	index_close(aqo_index_rel, index_lock);
	heap_close(aqo_heap, heap_lock);
	heap_close(aqo_update_heap, heap_lock);

	UnregisterSnapshot(snapshot);

	pfree(update_values);
	pfree(update_isnull);
	pfree(values);
	pfree(isnull);
	pfree(do_replace);

	return did_update;
}

static void
build_query_scankey(int natts, int update_natts, Datum *values,
					bool *isnull, ScanKeyData **keys, int *nkeys)
{
	*keys = palloc(sizeof(**keys));
	ScanKeyInit(&(*keys)[0],
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				values[0]
		);
	*nkeys = 1;
}

static void
build_fss_scankeys(int natts, int update_natts, Datum *values,
				   bool *isnull, ScanKeyData **keys, int *nkeys)
{
	*keys = palloc(sizeof(**keys) * 2);
	ScanKeyInit(&(*keys)[0],
				1,
				BTEqualStrategyNumber,
				F_INT4EQ,
				values[0]
		);
	ScanKeyInit(&(*keys)[1],
				2,
				BTEqualStrategyNumber,
				F_INT4EQ,
				values[1]
		);
	*nkeys = 2;
}

static void
copy_query_info(int natts, int update_natts, Datum *values, bool *isnull,
				bool *do_replace, Datum *update_values, bool *update_isnull)
{
	int			i;

	for (i = 0; i < natts; ++i)
	{
		values[i] = update_values[i];
		isnull[i] = false;
		if (do_replace)
			do_replace[i] = (i != 0);
	}
}

/* Apply updates from aqo_queries_update to aqo_queries */
static bool
update_aqo_queries()
{
	Oid			aqo_queries_heap_oid;
	Oid			aqo_queries_update_heap_oid;
	Oid			aqo_queries_query_index_rel_oid;

	aqo_queries_query_index_rel_oid = RelnameGetRelid("aqo_queries_query_hash_idx");
	aqo_queries_heap_oid = RelnameGetRelid("aqo_queries");
	aqo_queries_update_heap_oid = RelnameGetRelid("aqo_queries_updates");

	return update_aqo(aqo_queries_heap_oid, aqo_queries_update_heap_oid,
					  aqo_queries_query_index_rel_oid, &build_query_scankey,
					  &copy_query_info);
}

/* Apply updates from aqo_data_update to aqo_data */
static bool
update_aqo_data()
{
	Oid			aqo_data_heap_oid;
	Oid			aqo_data_update_heap_oid;
	Oid			aqo_data_index_rel_oid;

	aqo_data_heap_oid = RelnameGetRelid("aqo_data");
	aqo_data_update_heap_oid = RelnameGetRelid("aqo_data_updates");
	aqo_data_index_rel_oid = RelnameGetRelid("aqo_fss_access_idx");

	return update_aqo(aqo_data_heap_oid, aqo_data_update_heap_oid,
					  aqo_data_index_rel_oid, &build_fss_scankeys,
					  &copy_query_info);
}

/* Apply updates from aqo_query_stat_update to aqo_query_stat */
static bool
update_aqo_query_stat()
{
	Oid			aqo_query_stat_heap_oid;
	Oid			aqo_query_stat_update_heap_oid;
	Oid			aqo_query_stat_index_rel_oid;

	aqo_query_stat_heap_oid = RelnameGetRelid("aqo_query_stat");
	aqo_query_stat_update_heap_oid = RelnameGetRelid("aqo_query_stat_updates");
	aqo_query_stat_index_rel_oid = RelnameGetRelid("aqo_query_stat_idx");

	return update_aqo(aqo_query_stat_heap_oid, aqo_query_stat_update_heap_oid,
					  aqo_query_stat_index_rel_oid, &build_query_scankey,
					  &copy_query_info);
}

/* Apply updates from aqo_query_texts_update to aqo_query_texts */
static bool
update_aqo_query_texts()
{
	Oid			aqo_query_texts_heap_oid;
	Oid			aqo_query_texts_update_heap_oid;
	Oid			aqo_query_texts_index_rel_oid;

	aqo_query_texts_heap_oid = RelnameGetRelid("aqo_query_texts");
	aqo_query_texts_update_heap_oid = RelnameGetRelid("aqo_query_texts_updates");
	aqo_query_texts_index_rel_oid = RelnameGetRelid("aqo_query_texts_query_hash_idx");

	return update_aqo(aqo_query_texts_heap_oid, aqo_query_texts_update_heap_oid,
					  aqo_query_texts_index_rel_oid, &build_query_scankey,
					  &copy_query_info);
}

/* Update aqo tables using the execution information */
static void
aqo_proceed_info(void)
{
	bool		all_updated = false;

	/* Do updates while not all update tables are empty */
	while (!all_updated)
	{
		all_updated = true;

		/*
		 * One update iteration must be inside one transaction, otherwise we
		 * may see inconsistent updates which ruins everything.
		 */
		StartTransactionCommand();
		all_updated &= !update_aqo_queries();
		all_updated &= !update_aqo_data();
		all_updated &= !update_aqo_query_texts();
		all_updated &= !update_aqo_query_stat();
		CommitTransactionCommand();
	}
}

/* Register aqo background worker */
void
start_background_worker(void)
{
	BackgroundWorker worker;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	worker.bgw_main = NULL;
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_extra[0] = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "aqo");
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "aqo");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "background_worker_main");

	RegisterBackgroundWorker(&worker);
}

/* Entry point for aqo background worker */
void
background_worker_main(Datum arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, worker_aqo_sighup);
	pqsignal(SIGTERM, worker_aqo_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(aqo_database, NULL);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		/*
		 * Waiting on their process latch, which sleeps as necessary, but is
		 * awakened if postmaster dies.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   worker_aqo_naptime);
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* In case of a SIGHUP, just reload the configuration */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Background worker has nothing to do on replica */
		if (RecoveryInProgress())
			continue;

		/* Otherwise we need to proceed new information */
		aqo_proceed_info();
	}

	proc_exit(0);
}
