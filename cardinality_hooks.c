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
  debug_print("hello from our aqo!\n");
  // can we just modify this stuff?
	char **tables = malloc(sizeof(int *) * 20);
	int i = 0;
	for (int rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		if (bms_is_member(rti, rel->relids)) {
      char buffer[32];
			RangeTblEntry *rte=root->simple_rte_array[rti];
			sprintf(buffer, "%d: %d, %s\n", i, rti, get_rel_name(rte->relid));
			debug_print(buffer);
      tables[i] = buffer;
			i += 1;
		}
	}
  debug_print("going to print out the list of strings\n");
  for (int j = 0; j < i; j++) {
    debug_print(tables[j]);
  }

  rel->rows = 111.0;
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
	double		predicted;
	Oid			relid = InvalidOid;
	List	   *relids = NULL;
	List	   *allclauses = NULL;
	List	   *selectivities = NULL;
	ListCell   *l;
	ListCell   *l2;
	int			nargs;
	int		   *args_hash;
	int		   *eclass_hash;
	int			current_hash;
	int fss = 0;

	if (query_context.use_aqo || query_context.learn_aqo)
	{
		allclauses = list_concat(list_copy(param_clauses),
								 list_copy(rel->baserestrictinfo));
		selectivities = get_selectivities(root, allclauses, rel->relid,
										  JOIN_INNER, NULL);
		relid = planner_rt_fetch(rel->relid, root)->relid;
		get_eclasses(allclauses, &nargs, &args_hash, &eclass_hash);
		forboth(l, allclauses, l2, selectivities)
		{
			current_hash = get_clause_hash(
										((RestrictInfo *) lfirst(l))->clause,
										   nargs, args_hash, eclass_hash);
			cache_selectivity(current_hash, rel->relid, relid,
							  *((double *) lfirst(l2)));
		}
		pfree(args_hash);
		pfree(eclass_hash);
	}

	if (!query_context.use_aqo)
	{
		if (query_context.learn_aqo)
		{
			list_free_deep(selectivities);
			list_free(allclauses);
		}
		return call_default_get_parameterized_baserel_size(root, rel,
														   param_clauses);
	}

	relids = list_make1_int(relid);

	predicted = predict_for_relation(allclauses, selectivities, relids, &fss);

	predicted_ppi_rows = predicted;
	fss_ppi_hash = fss;

	if (predicted >= 0)
		return predicted;
	else
		return call_default_get_parameterized_baserel_size(root, rel,
														   param_clauses);
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
  double card;
  int i,r;
  // find the cardinality, by doing a lookup into the stored json
  jsmn_parser p;
  jsmntok_t t[15000];
  jsmn_init(&p);
  char test_str[100];
  char double_str[20];
  r = jsmn_parse(&p, cardinalities, strlen(cardinalities), t,
                 sizeof(t) / sizeof(t[0]));

  if (r < 0) {
    sprintf(test_str, "Failed to parse JSON: %d\n", r);
    debug_print(test_str);
  }

  /* Loop over all keys of the root object */
  // FIXME: i+=2?
  for (i = 1; i < r; i+=1) {
    if (jsoneq(cardinalities, &t[i], tables) == 0) {
      sprintf(test_str, "%s: %.*s\n", tables, t[i + 1].end - t[i + 1].start,
             cardinalities + t[i + 1].start);
      sprintf(double_str, "%.*s", t[i + 1].end - t[i + 1].start,
             cardinalities + t[i + 1].start);
      debug_print(test_str);
      debug_print(double_str);
      sscanf(double_str, "%lf", &card);
      break;
    }
  }
  return card;
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
  int MAX_TABLES, MAX_TABLE_NAME_SIZE;
  // can we just modify this stuff?
	char **tables;
	int num_tables;
  char *joined;
  double rows;
	MAX_TABLES = 20;
	MAX_TABLE_NAME_SIZE = 20;
	tables = malloc(sizeof(int *) * MAX_TABLES);
	num_tables = 0;
  debug_print("hello from our join size!\n");
	for (int rti = 1; rti < root->simple_rel_array_size; rti++)
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

  joined = join_strs(num_tables, tables);
	debug_print(joined);
  debug_print("\n");
  free(joined);

	qsort(tables, num_tables, sizeof(const char*), str_compare);
  joined = join_strs(num_tables, tables);
	debug_print(joined);
  debug_print("\n");

  rows = find_cardinality(query_context.cardinalities, joined);

  free(joined);

  // free things
  rel->rows = rows;
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
	double		predicted;
	List	   *relids;
	List	   *outer_clauses;
	List	   *inner_clauses;
	List	   *allclauses;
	List	   *selectivities;
	List	   *inner_selectivities;
	List	   *outer_selectivities;
	List	   *current_selectivities = NULL;
	int			fss = 0;

	if (query_context.use_aqo || query_context.learn_aqo)
		current_selectivities = get_selectivities(root, restrict_clauses, 0,
												  sjinfo->jointype, sjinfo);

	if (!query_context.use_aqo)
	{
		if (query_context.learn_aqo)
			list_free_deep(current_selectivities);

		return call_default_get_parameterized_joinrel_size(root, rel,
														   outer_path,
														   inner_path,
														   sjinfo,
														   restrict_clauses);
	}

	relids = get_list_of_relids(root, rel->relids);
	outer_clauses = get_path_clauses(outer_path, root, &outer_selectivities);
	inner_clauses = get_path_clauses(inner_path, root, &inner_selectivities);
	allclauses = list_concat(list_copy(restrict_clauses),
							 list_concat(outer_clauses, inner_clauses));
	selectivities = list_concat(current_selectivities,
								list_concat(outer_selectivities,
											inner_selectivities));

	predicted = predict_for_relation(allclauses, selectivities, relids, &fss);

	predicted_ppi_rows = predicted;
	fss_ppi_hash = fss;

	if (predicted >= 0)
		return predicted;
	else
		return call_default_get_parameterized_joinrel_size(root, rel,
														   outer_path,
														   inner_path,
														   sjinfo,
														   restrict_clauses);
}
