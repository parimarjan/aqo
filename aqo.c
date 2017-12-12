#include "aqo.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

/* GUC variables */
static const struct config_enum_entry format_options[] = {
	{"intelligent", AQO_MODE_INTELLIGENT, false},
	{"forced", AQO_MODE_FORCED, false},
	{"controlled", AQO_MODE_CONTROLLED, false},
	{"learn", AQO_MODE_LEARN, false},
	{"disabled", AQO_MODE_DISABLED, false},
	{NULL, 0, false}
};

/* Strategy of determining feature space for new queries. */
int			aqo_mode;

/* Time in milliseconds for make updates with background worker */
int			worker_aqo_naptime;

/* The database on which aqo background worker could be run */
char	   *aqo_database;

/* Master database connection information (in the case of replication) */
char	   *aqo_conninfo;


/* Parameters of autotuning */
int			aqo_stat_size = 20;
int			auto_tuning_window_size = 5;
double		auto_tuning_exploration = 0.1;
int			auto_tuning_max_iterations = 50;
int			auto_tuning_infinite_loop = 8;

/* stat_size > infinite_loop + window_size + 3 is required for auto_tuning*/

/* Machine learning parameters */
double		object_selection_prediction_threshold = 0.3;
double		object_selection_object_threshold = 0.1;
double		learning_rate = 1e-1;
int			aqo_k = 3;
int			aqo_K = 30;
double		log_selectivity_lower_bound = -30;

/* Parameters for current query */
int			query_hash;
bool		learn_aqo;
bool		use_aqo;
int			fspace_hash;
bool		auto_tuning;
bool		collect_stat;
bool		adding_query;
bool		explain_only;
bool		explain_aqo;

/* Query execution time */
instr_time	query_starttime;
double		query_planning_time;

/* Saved hook values in case of unload */
post_parse_analyze_hook_type prev_post_parse_analyze_hook;
planner_hook_type prev_planner_hook;
ExecutorStart_hook_type prev_ExecutorStart_hook;
ExecutorEnd_hook_type prev_ExecutorEnd_hook;
set_baserel_rows_estimate_hook_type prev_set_baserel_rows_estimate_hook;
get_parameterized_baserel_size_hook_type prev_get_parameterized_baserel_size_hook;
set_joinrel_size_estimates_hook_type prev_set_joinrel_size_estimates_hook;
get_parameterized_joinrel_size_hook_type prev_get_parameterized_joinrel_size_hook;
copy_generic_path_info_hook_type prev_copy_generic_path_info_hook;
ExplainOnePlan_hook_type prev_ExplainOnePlan_hook;

/*****************************************************************************
 *
 *	CREATE/DROP EXTENSION FUNCTIONS
 *
 *****************************************************************************/

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "aqo module must be initialized by Postmaster. "
			 "Put the following line to configuration file: "
			 "shared_preload_libraries='aqo'");
	}

	DefineCustomIntVariable("aqo.worker_naptime",
							"Sleep time in milliseconds for AQO worker",
							NULL,
							&worker_aqo_naptime,
							500,
							0,
							30000,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);
	DefineCustomStringVariable(
							   "aqo.database",
							   "On which database aqo could be run",
							   NULL,
							   &aqo_database,
							   "postgres",
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL
		);
	DefineCustomStringVariable(
							   "aqo.conninfo",
	   "Master database connection information (in the case of replication)",
							   NULL,
							   &aqo_conninfo,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL
		);
	DefineCustomEnumVariable("aqo.mode",
							 "Mode of aqo usage.",
							 NULL,
							 &aqo_mode,
							 AQO_MODE_CONTROLLED,
							 format_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	prev_planner_hook = planner_hook;
	planner_hook = &aqo_planner;
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = &get_query_text;
	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = &aqo_ExecutorStart;
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = &learn_query_stat;
	prev_set_baserel_rows_estimate_hook = set_baserel_rows_estimate_hook;
	set_baserel_rows_estimate_hook = &aqo_set_baserel_rows_estimate;
	prev_get_parameterized_baserel_size_hook =
		get_parameterized_baserel_size_hook;
	get_parameterized_baserel_size_hook =
		&aqo_get_parameterized_baserel_size;
	prev_set_joinrel_size_estimates_hook = set_joinrel_size_estimates_hook;
	set_joinrel_size_estimates_hook = &aqo_set_joinrel_size_estimates;
	prev_get_parameterized_joinrel_size_hook =
		get_parameterized_joinrel_size_hook;
	get_parameterized_joinrel_size_hook =
		&aqo_get_parameterized_joinrel_size;
	prev_copy_generic_path_info_hook = copy_generic_path_info_hook;
	copy_generic_path_info_hook = &aqo_copy_generic_path_info;
	prev_ExplainOnePlan_hook = ExplainOnePlan_hook;
	ExplainOnePlan_hook = print_into_explain;
	init_deactivated_queries_storage();

	start_background_worker();
}

void
_PG_fini(void)
{
	planner_hook = prev_planner_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	ExecutorStart_hook = prev_ExecutorStart_hook;
	ExecutorEnd_hook = prev_ExecutorEnd_hook;
	set_baserel_rows_estimate_hook = prev_set_baserel_rows_estimate_hook;
	get_parameterized_baserel_size_hook =
		prev_get_parameterized_baserel_size_hook;
	set_joinrel_size_estimates_hook = prev_set_joinrel_size_estimates_hook;
	get_parameterized_joinrel_size_hook =
		prev_get_parameterized_joinrel_size_hook;
	copy_generic_path_info_hook = prev_copy_generic_path_info_hook;
	ExplainOnePlan_hook = prev_ExplainOnePlan_hook;
	fini_deactivated_queries_storage();
}

PG_FUNCTION_INFO_V1(invalidate_deactivated_queries_cache);

/*
 * Clears the cache of deactivated queries if the user changed aqo_queries
 * manually.
 */
Datum
invalidate_deactivated_queries_cache(PG_FUNCTION_ARGS)
{
	fini_deactivated_queries_storage();
	init_deactivated_queries_storage();
	PG_RETURN_POINTER(NULL);
}
