#include "aqo.h"
#include "utils/lsyscache.h"

/*#define JSMN_HEADER*/
#include "jsmn.h"

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

int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
      strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
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

double find_cardinality(const char *cardinalities, char *tables)
{
  // find the cardinality, by doing a lookup into the stored json
  // FIXME: use smaller sized buffers?
  jsmn_parser p;
  jsmntok_t t[150000];
  jsmn_init(&p);
  char test_str[100];
  char double_str[128];
  double card;
  int i,r;
  r = jsmn_parse(&p, cardinalities, strlen(cardinalities), t,
                  150000);
  sprintf(test_str, "r: %d\n", r);
  debug_print(test_str);
  card = 42.0;

  if (r < 0) {
    sprintf(test_str, "Failed to parse JSON: %d\n", r);
    debug_print(test_str);
  }

  /* Loop over all keys of the root object */
  // FIXME: i+=2?
  for (i = 0; i < r; i+=1) {
    if (jsoneq(cardinalities, &t[i], tables) == 0) {
      sprintf(test_str, "%s: %.*s\n", tables, t[i + 1].end - t[i + 1].start,
             cardinalities + t[i + 1].start);
      sprintf(double_str, "%.*s", t[i + 1].end - t[i + 1].start,
             cardinalities + t[i + 1].start);
      card = atof(double_str);
      break;
    }
  }

  if (card == 42.0) {
    debug_print("did not find cardinality for: ");
    debug_print(tables);
    exit(-1);
  }
  debug_print("returning from find cardinality\n");
  // FIXME: use clamp row estimate here
  if (card == 0.00) {
    card += 1.00;
  }
  return card;
}

double get_json_cardinality(PlannerInfo *root, RelOptInfo *rel)
{
  int MAX_TABLES, MAX_TABLE_NAME_SIZE;
  // can we just modify this stuff?
	char *tables[20];
  char debug_text[1000];
	int num_tables;
  char *joined;
  double rows;
	MAX_TABLE_NAME_SIZE = 50;
	num_tables = 0;
  debug_print("hello from get_json_cardinality!\n");
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

  sprintf(debug_text, "num tables after: %d\n", num_tables);
  debug_print(debug_text);

  /*joined = join_strs(num_tables, tables);*/
	/*debug_print(joined);*/
  /*debug_print("\n");*/
  /*free(joined);*/

	qsort(tables, num_tables, sizeof(const char*), str_compare);
  joined = join_strs(num_tables, tables);
	debug_print(joined);
  debug_print("\n");

  debug_print("before find cardinality\n");
  rows = find_cardinality(query_context.cardinalities, joined);
  debug_print("rows found\n");

  free(joined);

  debug_print("before return rows\n");
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
 * Should just read the cardinalities from a file, and log error message
 * if any cardinality is missing.
 * TODO: figure out a smooth way to call default function if we don't have
 * correct cardinality.
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
