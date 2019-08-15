#include "aqo.h"
#include "utils/lsyscache.h"

/*#define JSMN_HEADER*/
/*#include "jsmn.h"*/

/*****************************************************************************
 *
 *	CARDINALITY ESTIMATION HOOKS
 *
 * This functions controls cardinality prediction in query optimization.
 * If use_aqo flag is false, then hooks just call default postgresql
 * cardinality estimator. Otherwise, they try to use AQO cardinality
 * prediction engine.
 * If use_aqo flag in true, hooks generate set of all clauses and all
 * absolute relids used in the relation being built and pass this
 * information to predict_for_relation function. Also these hooks compute
 * and pass to predict_for_relation marginal cardinalities for clauses.
 * If predict_for_relation returns non-negative value, then hooks assume it
 * to be true cardinality for given relation. Negative returned value means
 * refusal to predict cardinality. In this case hooks also use default
 * postgreSQL cardinality estimator.
 *
 *****************************************************************************/

double predicted_ppi_rows;
double fss_ppi_hash;

static void call_default_set_baserel_rows_estimate(PlannerInfo *root,
									   RelOptInfo *rel);
static double call_default_get_parameterized_baserel_size(PlannerInfo *root,
											RelOptInfo *rel,
											List *param_clauses);
static void call_default_set_joinrel_size_estimates(PlannerInfo *root,
										RelOptInfo *rel,
										RelOptInfo *outer_rel,
										RelOptInfo *inner_rel,
										SpecialJoinInfo *sjinfo,
										List *restrictlist);
static double call_default_get_parameterized_joinrel_size(PlannerInfo *root,
											RelOptInfo *rel,
											Path *outer_path,
											Path *inner_path,
											SpecialJoinInfo *sjinfo,
											List *restrict_clauses);
double find_cardinality(char *);
char * join_strs(int , char **);
static int str_compare(const void*, const void*);

double find_cardinality(char *key)
{
	struct cardinality *c;
  unsigned int num_keys;
  char debug_text[100];
  debug_print("in find cardinality\n");
  /*print_cardinalities();*/

  num_keys = HASH_COUNT(query_context.cardinalities);
  sprintf(debug_text, "num keys: %d\n", num_keys);
  debug_print(debug_text);

  c = (struct cardinality *) malloc(sizeof *c);
  HASH_FIND_STR(query_context.cardinalities, key, c);

  // TODO: can free the structure?
  debug_print(key);
  if (c) {
    debug_print("\nfound key in the hashmap\n");
  } else {
    error_print(key);
    error_print("\ndid not find key in hashmap\n");
    debug_print("\ndid not find key in hashmap\n");
    // FIXME: this should be just for cross joins
    return 1000000000.00;
  }
  return c->cardinality;
}

// Defining comparator function for strings
static int str_compare(const void* a, const void* b)
{
	return strcmp(*(const char**)a, *(const char**)b);
}

char * join_strs(int num, char **words)
{
	char *message = NULL;
	// add first word, then space
	int message_len;
	message_len = strlen(words[0]) + 1;
  message = malloc(message_len);
  strcpy(message, words[0]);

  for(int i = 1; i < num; ++i)
  {
    message_len += 1 + strlen(words[i]);
    message = (char*) realloc(message, message_len);
    /*strncat(strncat(message, " ", message_len), words[i], message_len);*/
    strcat(message, " ");
    strcat(message, words[i]);
  }

	return message;
}

double get_json_cardinality(PlannerInfo *root, RelOptInfo *rel)
{
  int MAX_TABLE_NAME_SIZE;
  // can we just modify this stuff?
	char *tables[20];
  char debug_text[10000];
	int num_tables;
  char *joined;
  double rows;
	MAX_TABLE_NAME_SIZE = 1000;
	num_tables = 0;
  debug_print("hello from get_json_cardinality!\n");
  // FIXME: for each table, check if it is in cardinalities or not
	for (int rti = 0; rti < root->simple_rel_array_size; rti++)
	{
		if (bms_is_member(rti, rel->relids)) {
      char *buffer = malloc(MAX_TABLE_NAME_SIZE);
			RangeTblEntry *rte=root->simple_rte_array[rti];
			/*sprintf(buffer, "%d: %d, %s\n", i, rti, get_rel_name(rte->relid));*/
      sprintf(buffer, "%s", get_rel_name(rte->relid));
      tables[num_tables] = buffer;
			num_tables += 1;
		}
	}

  /*debug_print(query_text);*/
  sprintf(debug_text, "num tables after: %d\n", num_tables);
  debug_print(debug_text);

  // TODO: use bitset instead of joined strings somehow for cardinalities
  // lookup.
	qsort(tables, num_tables, sizeof(const char*), str_compare);
  joined = join_strs(num_tables, tables);
	debug_print(joined);
  debug_print("\n");

  debug_print("before find cardinality\n");
  rows = find_cardinality(joined);
  debug_print("rows found\n");
  free(joined);

  debug_print("before return rows\n");
  if (rows == 0.00) {
    // zero makes postgres mad
    rows += 1.00;
  }
  return rows;
}

/*
 * Calls standard set_baserel_rows_estimate or its previous hook.
 */
void
call_default_set_baserel_rows_estimate(PlannerInfo *root, RelOptInfo *rel)
{
	if (prev_set_baserel_rows_estimate_hook)
		prev_set_baserel_rows_estimate_hook(root, rel);
	else
		set_baserel_rows_estimate_standard(root, rel);
}

/*
 * Calls standard get_parameterized_baserel_size or its previous hook.
 */
double
call_default_get_parameterized_baserel_size(PlannerInfo *root,
											RelOptInfo *rel,
											List *param_clauses)
{
	if (prev_get_parameterized_baserel_size_hook)
		return prev_get_parameterized_baserel_size_hook(root, rel, param_clauses);
	else
		return get_parameterized_baserel_size_standard(root, rel, param_clauses);
}

/*
 * Calls standard get_parameterized_joinrel_size or its previous hook.
 */
double
call_default_get_parameterized_joinrel_size(PlannerInfo *root,
											RelOptInfo *rel,
											Path *outer_path,
											Path *inner_path,
											SpecialJoinInfo *sjinfo,
											List *restrict_clauses)
{
	if (prev_get_parameterized_joinrel_size_hook)
		return prev_get_parameterized_joinrel_size_hook(root, rel,
														outer_path,
														inner_path,
														sjinfo,
														restrict_clauses);
	else
		return get_parameterized_joinrel_size_standard(root, rel,
													   outer_path,
													   inner_path,
													   sjinfo,
													   restrict_clauses);
}

/*
 * Calls standard set_joinrel_size_estimates or its previous hook.
 */
void
call_default_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
										RelOptInfo *outer_rel,
										RelOptInfo *inner_rel,
										SpecialJoinInfo *sjinfo,
										List *restrictlist)
{
	if (prev_set_joinrel_size_estimates_hook)
		prev_set_joinrel_size_estimates_hook(root, rel,
											 outer_rel,
											 inner_rel,
											 sjinfo,
											 restrictlist);
	else
		set_joinrel_size_estimates_standard(root, rel,
											outer_rel,
											inner_rel,
											sjinfo,
											restrictlist);
}

/*
 * Our hook for setting baserel rows estimate.
 * Tries to find cardinality in the loaded cardinalities map (in preprocessing
 * hook), and if it is not present, then call the default implementation.
 */
void
aqo_set_baserel_rows_estimate(PlannerInfo *root, RelOptInfo *rel)
{
  debug_print("hello from aqo baserel!\n");
  rel->rows = get_json_cardinality(root, rel);
}

void
ppi_hook(ParamPathInfo *ppi)
{
	ppi->predicted_ppi_rows = predicted_ppi_rows;
	ppi->fss_ppi_hash = fss_ppi_hash;
}

/*
 * Our hook for estimating parameterized baserel rows estimate.
 * Extracts clauses (including parametrization ones), their selectivities
 * and list of relation relids and passes them to predict_for_relation.
 */
double
aqo_get_parameterized_baserel_size(PlannerInfo *root,
								   RelOptInfo *rel,
								   List *param_clauses)
{
  return get_json_cardinality(root, rel);
}

/*
 * Our hook for setting joinrel rows estimate.
 * Extracts clauses, their selectivities and list of relation relids and
 * passes them to predict_for_relation.
 */
void
aqo_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
							   RelOptInfo *outer_rel,
							   RelOptInfo *inner_rel,
							   SpecialJoinInfo *sjinfo,
							   List *restrictlist)
{
  /*rel->rows = 50.0;*/
  rel->rows = get_json_cardinality(root, rel);
}

/*
 * Our hook for estimating parameterized joinrel rows estimate.
 * Extracts clauses (including parametrization ones), their selectivities
 * and list of relation relids and passes them to predict_for_relation.
 */
double
aqo_get_parameterized_joinrel_size(PlannerInfo *root,
								   RelOptInfo *rel,
								   Path *outer_path,
								   Path *inner_path,
								   SpecialJoinInfo *sjinfo,
								   List *restrict_clauses)
{
  return get_json_cardinality(root, rel);
}
