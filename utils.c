#include "aqo.h"

/*****************************************************************************
 *
 *	UTILITIES
 *
 *****************************************************************************/

static void *argsort_a;
static size_t argsort_es;
static int	(*argsort_value_cmp) (const void *, const void *);

static int	argsort_cmp(const void *a, const void *b);


/*
 * Function for qsorting an integer arrays
 */
int
int_cmp(const void *a, const void *b)
{
	if (*(int *) a < *(int *) b)
		return -1;
	else if (*(int *) a > *(int *) b)
		return 1;
	else
		return 0;
}

/*
 * Function for qsorting an double arrays
 */
int
double_cmp(const void *a, const void *b)
{
	if (*(double *) a < *(double *) b)
		return -1;
	else if (*(double *) a > *(double *) b)
		return 1;
	else
		return 0;
}

/*
 * Compares elements for two given indexes
 */
int
argsort_cmp(const void *a, const void *b)
{
	return (*argsort_value_cmp) ((char *) argsort_a +
								 *((int *) a) * argsort_es,
								 (char *) argsort_a +
								 *((int *) b) * argsort_es);
}

/*
 * Returns array of indexes that makes given array sorted.
 */
int *
argsort(void *a, int n, size_t es, int (*cmp) (const void *, const void *))
{
	int		   *idx = palloc(n * sizeof(*idx));
	int			i;

	for (i = 0; i < n; ++i)
		idx[i] = i;
	argsort_value_cmp = cmp;
	argsort_a = a;
	argsort_es = es;
	qsort(idx, n, sizeof(*idx), argsort_cmp);

	return idx;
}

/*
 * Returns the inverse of given permutation.
 */
int *
inverse_permutation(int *idx, int n)
{
	int		   *inv = palloc(n * sizeof(*inv));
	int			i;

	for (i = 0; i < n; ++i)
		inv[idx[i]] = i;
	return inv;
}

/*
 * Allocates empty QueryStat object.
 */
QueryStat *
palloc_query_stat(void)
{
	QueryStat  *res = palloc0(sizeof(*res));

	res->execution_time_with_aqo = palloc(aqo_stat_size * sizeof(res->execution_time_with_aqo[0]));
	res->execution_time_without_aqo = palloc(aqo_stat_size * sizeof(res->execution_time_without_aqo[0]));
	res->planning_time_with_aqo = palloc(aqo_stat_size * sizeof(res->planning_time_with_aqo[0]));
	res->planning_time_without_aqo = palloc(aqo_stat_size * sizeof(res->planning_time_without_aqo[0]));
	res->cardinality_error_with_aqo = palloc(aqo_stat_size * sizeof(res->cardinality_error_with_aqo[0]));
	res->cardinality_error_without_aqo = palloc(aqo_stat_size * sizeof(res->cardinality_error_without_aqo[0]));
	return res;
}

/*
 * Frees QueryStat object.
 */
void
pfree_query_stat(QueryStat * stat)
{
	pfree(stat->execution_time_with_aqo);
	pfree(stat->execution_time_without_aqo);
	pfree(stat->planning_time_with_aqo);
	pfree(stat->planning_time_without_aqo);
	pfree(stat->cardinality_error_with_aqo);
	pfree(stat->cardinality_error_without_aqo);
	pfree(stat);
}

/*
 * Expands matrix from storage into simple C-array.
 */
void
deform_matrix(Datum datum, double **matrix)
{
	ArrayType  *array = DatumGetArrayTypePCopy(datum);
	int			nelems;
	Datum	   *values;
	int			rows;
	int			cols;
	int			i,
				j;

	deconstruct_array(array,
					  FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd',
					  &values, NULL, &nelems);
	rows = ARR_DIMS(array)[0];
	cols = ARR_DIMS(array)[1];
	for (i = 0; i < rows; ++i)
		for (j = 0; j < cols; ++j)
			matrix[i][j] = DatumGetFloat8(values[i * cols + j]);
	pfree(values);
	pfree(array);
}

/*
 * Expands vector from storage into simple C-array.
 * Also returns its number of elements.
 */
void
deform_vector(Datum datum, double *vector, int *nelems)
{
	ArrayType  *array = DatumGetArrayTypePCopy(datum);
	Datum	   *values;
	int			i;
	int			my_nelems;

	if (nelems == NULL)
		nelems = &my_nelems;

	deconstruct_array(array,
					  FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd',
					  &values, NULL, nelems);
	for (i = 0; i < *nelems; ++i)
		vector[i] = DatumGetFloat8(values[i]);
	pfree(values);
	pfree(array);
}

/*
 * Forms ArrayType object for storage from simple C-array matrix.
 */
ArrayType *
form_matrix(double **matrix, int nrows, int ncols)
{
	Datum	   *elems;
	ArrayType  *array;
	int			dims[2];
	int			lbs[2];
	int			i,
				j;

	dims[0] = nrows;
	dims[1] = ncols;
	lbs[0] = lbs[1] = 1;
	elems = palloc(sizeof(*elems) * nrows * ncols);
	for (i = 0; i < nrows; ++i)
		for (j = 0; j < ncols; ++j)
			elems[i * ncols + j] = Float8GetDatum(matrix[i][j]);
	array = construct_md_array(elems, NULL, 2, dims, lbs,
							   FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd');
	pfree(elems);
	return array;
}

/*
 * Forms ArrayType object for storage from simple C-array vector.
 */
ArrayType *
form_vector(double *vector, int nrows)
{
	Datum	   *elems;
	ArrayType  *array;
	int			dims[1];
	int			lbs[1];
	int			i;

	dims[0] = nrows;
	lbs[0] = 1;
	elems = palloc(sizeof(*elems) * nrows);
	for (i = 0; i < nrows; ++i)
		elems[i] = Float8GetDatum(vector[i]);
	array = construct_md_array(elems, NULL, 1, dims, lbs,
							   FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd');
	pfree(elems);
	return array;
}
