/*-------------------------------------------------------------------------
 *
 * auto_explain.c
 *
 *
 * Copyright (c) 2008-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/auto_explain/auto_explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/parallel.h"
#include "access/table.h"
#include "catalog/pg_statistic_ext_data_d.h"
#include "commands/explain.h"
#include "commands/defrem.h"
#include "executor/instrument.h"
#include "jit/jit.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "statistics/statistics.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

PG_MODULE_MAGIC;

/* GUC variables */
static int	auto_explain_log_min_duration = -1; /* msec or -1 */
static bool auto_explain_log_analyze = false;
static bool auto_explain_log_verbose = false;
static bool auto_explain_log_buffers = false;
static bool auto_explain_log_triggers = false;
static bool auto_explain_log_timing = true;
static bool auto_explain_log_settings = false;
static int	auto_explain_log_format = EXPLAIN_FORMAT_TEXT;
static int	auto_explain_log_level = LOG;
static bool auto_explain_log_nested_statements = false;
static bool auto_explain_suggest_only = false;
static double auto_explain_sample_rate = 1;
static double auto_explain_add_statistics_threshold = 0.0;

static const struct config_enum_entry format_options[] = {
	{"text", EXPLAIN_FORMAT_TEXT, false},
	{"xml", EXPLAIN_FORMAT_XML, false},
	{"json", EXPLAIN_FORMAT_JSON, false},
	{"yaml", EXPLAIN_FORMAT_YAML, false},
	{NULL, 0, false}
};

static const struct config_enum_entry loglevel_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"log", LOG, false},
	{NULL, 0, false}
};

/* Current nesting depth of ExecutorRun calls */
static int	nesting_level = 0;

/* Is the current top-level query to be sampled? */
static bool current_query_sampled = false;

#define auto_explain_enabled() \
	(auto_explain_log_min_duration >= 0 && \
	 (nesting_level == 0 || auto_explain_log_nested_statements) && \
	 current_query_sampled)

/* Saved hook values in case of unload */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

void		_PG_init(void);
void		_PG_fini(void);

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
								ScanDirection direction,
								uint64 count, bool execute_once);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomIntVariable("auto_explain.log_min_duration",
							"Sets the minimum execution time above which plans will be logged.",
							"Zero prints all plans. -1 turns this feature off.",
							&auto_explain_log_min_duration,
							-1,
							-1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("auto_explain.log_analyze",
							 "Use EXPLAIN ANALYZE for plan logging.",
							 NULL,
							 &auto_explain_log_analyze,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_settings",
							 "Log modified configuration parameters affecting query planning.",
							 NULL,
							 &auto_explain_log_settings,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_verbose",
							 "Use EXPLAIN VERBOSE for plan logging.",
							 NULL,
							 &auto_explain_log_verbose,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_buffers",
							 "Log buffers usage.",
							 NULL,
							 &auto_explain_log_buffers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_triggers",
							 "Include trigger statistics in plans.",
							 "This has no effect unless log_analyze is also set.",
							 &auto_explain_log_triggers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("auto_explain.log_format",
							 "EXPLAIN format to be used for plan logging.",
							 NULL,
							 &auto_explain_log_format,
							 EXPLAIN_FORMAT_TEXT,
							 format_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("auto_explain.log_level",
							 "Log level for the plan.",
							 NULL,
							 &auto_explain_log_level,
							 LOG,
							 loglevel_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_nested_statements",
							 "Log nested statements.",
							 NULL,
							 &auto_explain_log_nested_statements,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_timing",
							 "Collect timing data, not just row counts.",
							 NULL,
							 &auto_explain_log_timing,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("auto_explain.sample_rate",
							 "Fraction of queries to process.",
							 NULL,
							 &auto_explain_sample_rate,
							 1.0,
							 0.0,
							 1.0,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("auto_explain.add_statistics_threshold",
							 "Sets the threshold for actual/estimated #rows ratio triggering creation of multicolumn statistic for the related columns.",
							 "Zero disables implicit creation of multicolumn statistic.",
							 &auto_explain_add_statistics_threshold,
							 0.0,
							 0.0,
							 INT_MAX,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.suggest_only",
							 "Do not create statistic but just record in WAL suggested create statistics statement.",
							 NULL,
							 &auto_explain_suggest_only,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	EmitWarningsOnPlaceholders("auto_explain");

	/* Install hooks. */
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = explain_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = explain_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = explain_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	/*
	 * At the beginning of each top-level statement, decide whether we'll
	 * sample this statement.  If nested-statement explaining is enabled,
	 * either all nested statements will be explained or none will.
	 *
	 * When in a parallel worker, we should do nothing, which we can implement
	 * cheaply by pretending we decided not to sample the current statement.
	 * If EXPLAIN is active in the parent session, data will be collected and
	 * reported back to the parent, and it's no business of ours to interfere.
	 */
	if (nesting_level == 0)
	{
		if (auto_explain_log_min_duration >= 0 && !IsParallelWorker())
			current_query_sampled = (random() < auto_explain_sample_rate *
									 ((double) MAX_RANDOM_VALUE + 1));
		else
			current_query_sampled = false;
	}

	if (auto_explain_enabled())
	{
		/* Enable per-node instrumentation iff log_analyze is required. */
		if (auto_explain_log_analyze && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		{
			if (auto_explain_log_timing)
				queryDesc->instrument_options |= INSTRUMENT_TIMER;
			else
				queryDesc->instrument_options |= INSTRUMENT_ROWS;
			if (auto_explain_log_buffers)
				queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
		}
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (auto_explain_enabled())
	{
		/*
		 * Set up to track total elapsed time in ExecutorRun.  Make sure the
		 * space is allocated in the per-query context so it will go away at
		 * ExecutorEnd.
		 */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					uint64 count, bool execute_once)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
explain_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
AddMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es);

static void
AddMultiColumnStatisticsForSubPlans(List *plans, ExplainState *es)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

		AddMultiColumnStatisticsForNode(sps->planstate, es);
	}
}

static void
AddMultiColumnStatisticsForMemberNodes(PlanState **planstates, int nsubnodes,
									ExplainState *es)
{
	int			j;

	for (j = 0; j < nsubnodes; j++)
		AddMultiColumnStatisticsForNode(planstates[j], es);
}

static int
vars_list_comparator(const ListCell *a, const ListCell *b)
{
	char* va = strVal((Value *) linitial(((ColumnRef *)lfirst(a))->fields));
	char* vb = strVal((Value *) linitial(((ColumnRef *)lfirst(b))->fields));
	return strcmp(va, vb);
}

static void
AddMultiColumnStatisticsForQual(void* qual, ExplainState *es)
{
	List *vars = NULL;
	ListCell* lc;
	foreach (lc, qual)
	{
		Node* node = (Node*)lfirst(lc);
		if (IsA(node, RestrictInfo))
			node = (Node*)((RestrictInfo*)node)->clause;
		vars = list_concat(vars, pull_vars_of_level(node, 0));
	}
	while (vars != NULL)
	{
		ListCell *cell;
		List *cols = NULL;
		Index varno = 0;
		Bitmapset* colmap = NULL;

		foreach (cell, vars)
		{
			Node* node = (Node *) lfirst(cell);
			if (IsA(node, Var))
			{
				Var *var = (Var *) node;
				if (cols == NULL || var->varnoold == varno)
				{
					varno = var->varnoold;
					if (var->varattno > 0 &&
						!bms_is_member(var->varattno, colmap) &&
						varno >= 1 &&
						varno <= list_length(es->rtable) &&
						list_length(cols) < STATS_MAX_DIMENSIONS)
					{
						RangeTblEntry *rte = rt_fetch(varno, es->rtable);
						if (rte->rtekind == RTE_RELATION)
						{
							ColumnRef  *col = makeNode(ColumnRef);
							char *colname = get_rte_attribute_name(rte, var->varattno);
							col->fields = list_make1(makeString(colname));
							cols = lappend(cols, col);
							colmap = bms_add_member(colmap, var->varattno);
						}
					}
				}
				else
				{
					continue;
				}
			}
			vars = foreach_delete_current(vars, cell);
		}
		if (list_length(cols) >= 2)
		{
			CreateStatsStmt* stats = makeNode(CreateStatsStmt);
			RangeTblEntry *rte = rt_fetch(varno, es->rtable);
			char *rel_namespace = get_namespace_name(get_rel_namespace(rte->relid));
			char *rel_name = get_rel_name(rte->relid);
			RangeVar* rel = makeRangeVar(rel_namespace, rel_name, 0);
			char* stat_name = rel_name;
			char* create_stat_stmt = (char*)"";
			char const* sep = "ON";
			Relation* stat;

			list_sort(cols, vars_list_comparator);
			/* Construct name for statistic by concatenating relation name with all columns */
			foreach (cell, cols)
			{
				char* col_name = strVal((Value *) linitial(((ColumnRef *)lfirst(cell))->fields));
				stat_name = psprintf("%s_%s", stat_name, col_name);
				create_stat_stmt = psprintf("%s%s %s", create_stat_stmt, sep, col_name);
				sep = ",";
			}
			/*
			 * Check if multicolumn statistic object with such name already exists.
			 * Most likely if was already created by auto_explain, but either ANALYZE was not performed since
			 * this time, either presence of this multicolumn statistic doesn't help to provide more precise estimation.
			 * Despite to the fact that we create statistics with "if_not_exist" option, presence of such check
			 * allows to eliminate notice message that statistics object already exists.
			 */
			if (!SearchSysCacheExists2(STATEXTNAMENSP,
									   CStringGetDatum(stat_name),
									   ObjectIdGetDatum(get_rel_namespace(rte->relid))))
			{
				if (auto_explain_suggest_only)
				{
					elog(NOTICE, "Auto_explain suggestion: CREATE STATISTICS %s %s FROM %s", stat_name, create_stat_stmt, rel_name);
				}
				else
				{
					Relation stat = table_open(StatisticExtDataRelationId, AccessExclusiveLock);
					if (stat == NULL)
						elog(ERROR, "Failed to lock statistic table");

					/* Recheck under lock */
					if (!SearchSysCacheExists2(STATEXTNAMENSP,
									   CStringGetDatum(stat_name),
									   ObjectIdGetDatum(get_rel_namespace(rte->relid))))
					{
						elog(LOG, "Add statistics %s", stat_name);
						stats->defnames = list_make2(makeString(rel_namespace), makeString(stat_name));
						stats->if_not_exists = true;
						stats->relations = list_make1(rel);
						stats->exprs = cols;
						CreateStatistics(stats);
					}
					table_close(stat, AccessExclusiveLock);
				}
			}
		}
	}
}

static void
AddMultiColumnStatisticsForNode(PlanState *planstate, ExplainState *es)
{
	Plan	   *plan = planstate->plan;

	if (planstate->instrument && plan->plan_rows != 0)
	{
		if (auto_explain_add_statistics_threshold != 0
			&& planstate->instrument->ntuples / plan->plan_rows >= auto_explain_add_statistics_threshold)
		{
			elog(DEBUG1, "Estimated=%f, actual=%f, error=%f: plan=%s", plan->plan_rows, planstate->instrument->ntuples, planstate->instrument->ntuples / plan->plan_rows, nodeToString(plan));
			/* quals, sort keys, etc */
			switch (nodeTag(plan))
			{
			  case T_IndexScan:
				AddMultiColumnStatisticsForQual(((IndexScan *) plan)->indexqualorig, es);
				break;
			  case T_IndexOnlyScan:
				AddMultiColumnStatisticsForQual(((IndexOnlyScan *) plan)->indexqual, es);
				break;
			  case T_BitmapIndexScan:
				AddMultiColumnStatisticsForQual(((BitmapIndexScan *) plan)->indexqualorig, es);
				break;
			  case T_NestLoop:
				AddMultiColumnStatisticsForQual(((NestLoop *) plan)->join.joinqual, es);
				break;
			  case T_MergeJoin:
				AddMultiColumnStatisticsForQual(((MergeJoin *) plan)->mergeclauses, es);
				AddMultiColumnStatisticsForQual(((MergeJoin *) plan)->join.joinqual, es);
				break;
			  case T_HashJoin:
				AddMultiColumnStatisticsForQual(((HashJoin *) plan)->hashclauses, es);
				AddMultiColumnStatisticsForQual(((HashJoin *) plan)->join.joinqual, es);
				break;
			  default:
				break;
			}
			AddMultiColumnStatisticsForQual(plan->qual, es);
		}
	}

	/* initPlan-s */
	if (planstate->initPlan)
		AddMultiColumnStatisticsForSubPlans(planstate->initPlan, es);

	/* lefttree */
	if (outerPlanState(planstate))
		AddMultiColumnStatisticsForNode(outerPlanState(planstate), es);

	/* righttree */
	if (innerPlanState(planstate))
		AddMultiColumnStatisticsForNode(innerPlanState(planstate), es);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			AddMultiColumnStatisticsForMemberNodes(((ModifyTableState *) planstate)->mt_plans,
												   ((ModifyTableState *) planstate)->mt_nplans,
												   es);
			break;
		case T_Append:
			AddMultiColumnStatisticsForMemberNodes(((AppendState *) planstate)->appendplans,
												   ((AppendState *) planstate)->as_nplans,
												   es);
			break;
		case T_MergeAppend:
			AddMultiColumnStatisticsForMemberNodes(((MergeAppendState *) planstate)->mergeplans,
												   ((MergeAppendState *) planstate)->ms_nplans,
												   es);
			break;
		case T_BitmapAnd:
			AddMultiColumnStatisticsForMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
												   ((BitmapAndState *) planstate)->nplans,
												   es);
			break;
		case T_BitmapOr:
			AddMultiColumnStatisticsForMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
												   ((BitmapOrState *) planstate)->nplans,
												   es);
			break;
		case T_SubqueryScan:
			AddMultiColumnStatisticsForNode(((SubqueryScanState *) planstate)->subplan, es);
			break;
		default:
			break;
	}
}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && auto_explain_enabled())
	{
		double		msec;

		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		/* Log plan if duration is exceeded. */
		msec = queryDesc->totaltime->total * 1000.0;
		if (msec >= auto_explain_log_min_duration)
		{
			ExplainState *es = NewExplainState();

			es->analyze = (queryDesc->instrument_options && auto_explain_log_analyze);
			es->verbose = auto_explain_log_verbose;
			es->buffers = (es->analyze && auto_explain_log_buffers);
			es->timing = (es->analyze && auto_explain_log_timing);
			es->summary = es->analyze;
			es->format = auto_explain_log_format;
			es->settings = auto_explain_log_settings;

			ExplainBeginOutput(es);
			ExplainQueryText(es, queryDesc);
			ExplainPrintPlan(es, queryDesc);
			if (es->analyze && auto_explain_log_triggers)
				ExplainPrintTriggers(es, queryDesc);
			if (es->costs)
				ExplainPrintJITSummary(es, queryDesc);
			ExplainEndOutput(es);

			/* Add multicolumn statistic if requested */
			if (auto_explain_add_statistics_threshold && !IsParallelWorker())
				AddMultiColumnStatisticsForNode(queryDesc->planstate, es);

			/* Remove last line break */
			if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
				es->str->data[--es->str->len] = '\0';

			/* Fix JSON to output an object */
			if (auto_explain_log_format == EXPLAIN_FORMAT_JSON)
			{
				es->str->data[0] = '{';
				es->str->data[es->str->len - 1] = '}';
			}

			/*
			 * Note: we rely on the existing logging of context or
			 * debug_query_string to identify just which statement is being
			 * reported.  This isn't ideal but trying to do it here would
			 * often result in duplication.
			 */
			ereport(auto_explain_log_level,
					(errmsg("duration: %.3f ms  plan:\n%s",
							msec, es->str->data),
					 errhidestmt(true)));

			pfree(es->str->data);
		}
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}
