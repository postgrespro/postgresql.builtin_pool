/*-------------------------------------------------------------------------
 *
 * inmem_handler.c
 *	  Inmemory storage table access method code
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/inmem/inmem_handler.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "optimizer/plancat.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/rel.h"

static HTAB* tables;

typedef struct InmemTuple
{
	struct InmemTuple* next;
	struct InmemTuple* prev;
	Datum* values;
} InmemTuple;

typedef struct
{
	Oid relid;
	size_t n_tuples;
	InmemTuple extent;
	MemoryContext ctx;
} InmemTable;

typedef struct
{
	TableScanDescData rs_scan;
	InmemTuple* current;
} InmemScanDesc;

typedef struct
{
	IndexFetchTableData idx_fetch_data;
	InmemTable* table;
} InmemIndexFetch;

#define TUPLE_ISNULL(tuple,slot) ((bool*)(tuple->values + slot->tts_nvalid))

static bool
inmem_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return true;
}

static bool
inmem_fetch_row_version(Relation relation,
							 ItemPointer tid,
							 Snapshot snapshot,
							 TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
	return false;
}

static void
inmem_get_latest_tid(TableScanDesc scan,
					 ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}


static void
inmem_store_slot(InmemTuple* tuple, TupleTableSlot *slot)
{
	int n_atts = slot->tts_nvalid;
	TupleDesc desc = slot->tts_tupleDescriptor;
	char* data;
	int i;
	size_t var_size = 0;

	Assert(n_atts == desc->natts);

	for (i = 0; i < n_atts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[i])
			continue;

		val = slot->tts_values[i];
		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			var_size = att_align_nominal(var_size, att->attalign);
			var_size += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			var_size = att_align_nominal(var_size, att->attalign);
			var_size = att_addlength_datum(var_size, att->attlen, val);
		}
	}
	data = palloc(MAXALIGN((sizeof(Datum) + sizeof(bool))*n_atts) + var_size);
	tuple->values = (Datum*)data;
	data += MAXALIGN((sizeof(Datum) + sizeof(bool))*n_atts);
	memcpy(tuple->values, slot->tts_values, n_atts*sizeof(Datum));
	memcpy(TUPLE_ISNULL(tuple, slot), slot->tts_isnull, n_atts*sizeof(bool));

	for (i = 0; i < n_atts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[i])
			continue;

		val = slot->tts_values[i];
		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			Size		data_length;

			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			tuple->values[i] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			tuple->values[i] = PointerGetDatum(data);
			data += data_length;
		}
	}
}

static void
inmem_insert_slot(InmemTable* table, TupleTableSlot *slot)
{
	InmemTuple* tuple = palloc(sizeof(*tuple));
	tuple->next = &table->extent;
	tuple->prev = table->extent.prev;
	table->extent.prev = table->extent.prev->next = tuple;
	inmem_store_slot(tuple, slot);
	table->n_tuples += 1;
}

static InmemTable* inmem_get_table(Relation relation)
{
	bool found;
	InmemTable* table;

	if (tables == NULL)
	{
		HASHCTL	ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(InmemTable);
		ctl.hcxt = TopMemoryContext;
		tables = hash_create("inmemory_table_hash", 113, &ctl, HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
	}
    table = (InmemTable*)hash_search(tables, &relation->rd_id, HASH_ENTER, &found);
	if (!found) {
		table->n_tuples = 0;
		table->ctx = AllocSetContextCreate(TopMemoryContext,
										   "inmem table context",
										   ALLOCSET_DEFAULT_SIZES);
		table->extent.next = table->extent.prev = &table->extent;
		table->extent.values = NULL;
	}
	return table;
}

static void
inmem_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				  int options, struct BulkInsertStateData *bistate)
{
	InmemTable* table = inmem_get_table(relation);
	MemoryContext oldctx = MemoryContextSwitchTo(table->ctx);
	inmem_insert_slot(table, slot);
	MemoryContextSwitchTo(oldctx);
}

static void
inmem_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
							  int options, BulkInsertState bistate, uint32 specToken)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
inmem_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 spekToken,
								bool succeeded)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
inmem_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
						CommandId cid, int options, BulkInsertState bistate)
{
	int i;
	InmemTable* table = inmem_get_table(relation);
	MemoryContext oldctx = MemoryContextSwitchTo(table->ctx);
	for (i = 0; i < ntuples; i++)
	{
		inmem_insert_slot(table, slots[i]);
	}
	MemoryContextSwitchTo(oldctx);
}


static TM_Result
inmem_delete(Relation relation, ItemPointer tid_p, CommandId cid,
				  Snapshot snapshot, Snapshot crosscheck, bool wait,
				  TM_FailureData *hufd, bool changingPart)
{
	InmemTable* table = inmem_get_table(relation);
	InmemTuple* tuple = *(InmemTuple**)tid_p;
	tuple->next->prev = tuple->prev;
	tuple->prev->next = tuple->next;
	table->n_tuples -= 1;
	pfree(tuple->values);
	pfree(tuple);
	return TM_Ok;
}

static TM_Result
inmem_lock_tuple(Relation relation, ItemPointer tid, Snapshot snapshot,
					  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					  LockWaitPolicy wait_policy, uint8 flags,
					  TM_FailureData *hufd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static TM_Result
inmem_update(Relation relation, ItemPointer otid_p, TupleTableSlot *slot,
				  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
				  bool wait, TM_FailureData *hufd,
				  LockTupleMode *lockmode, bool *update_indexes)
{
	InmemTable* table = inmem_get_table(relation);
	InmemTuple* tuple = *(InmemTuple**)otid_p;
	MemoryContext oldctx = MemoryContextSwitchTo(table->ctx);
	pfree(tuple->values);
	inmem_store_slot(tuple, slot);
	MemoryContextSwitchTo(oldctx);

	return TM_Ok;
}

static const TupleTableSlotOps *
inmem_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

static TableScanDesc
inmem_beginscan(Relation relation,
				Snapshot snapshot,
				int nkeys, struct ScanKeyData *key,
				ParallelTableScanDesc pscan,
				uint32 flags)
{
	InmemScanDesc* scan;
	InmemTable* table = inmem_get_table(relation);
	scan = (InmemScanDesc*) palloc0(sizeof(InmemScanDesc));
	scan->rs_scan.rs_rd = relation;
	scan->rs_scan.rs_snapshot = snapshot;
	scan->rs_scan.rs_nkeys = nkeys;
	scan->rs_scan.rs_parallel = pscan;
	scan->rs_scan.rs_flags = flags;
	Assert(nkeys == 0);

	scan->current = &table->extent;
	return (TableScanDesc) scan;
}

static void
inmem_endscan(TableScanDesc scan)
{
	pfree(scan);
}

static void
inmem_fetch_slot(InmemTuple* tuple, TupleTableSlot *slot)
{
	*(void**)&slot->tts_tid = tuple;
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_values = tuple->values;
	slot->tts_isnull = TUPLE_ISNULL(tuple, slot);
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_flags |= TTS_FLAG_PINNED;
}

static bool
inmem_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	InmemScanDesc* scan = (InmemScanDesc*)sscan;
	InmemTuple* tuple = scan->current;
	switch (direction)
	{
	  case BackwardScanDirection:
		tuple = tuple->prev;
		break;
	  case NoMovementScanDirection:
		break;
	  case ForwardScanDirection:
		tuple = tuple->next;
		break;
	}
	scan->current = tuple;
	if (tuple->values != NULL) {
		inmem_fetch_slot(tuple, slot);
		return true;
	} else {
		slot->tts_nvalid = 0;
		slot->tts_flags |= TTS_FLAG_EMPTY;
		return false;
	}
}

static bool
inmem_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
									Snapshot snapshot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static TransactionId
inmem_compute_xid_horizon_for_tuples(Relation rel,
										  ItemPointerData *items,
										  int nitems)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));

}

static IndexFetchTableData *
inmem_begin_index_fetch(Relation rel)
{
	InmemIndexFetch* scan = palloc0(sizeof(InmemIndexFetch));
	scan->table = inmem_get_table(rel);
	return (IndexFetchTableData *)scan;
}

static void
inmem_reset_index_fetch(IndexFetchTableData *scan)
{
}

static void
inmem_end_index_fetch(IndexFetchTableData *scan)
{
	pfree(scan);
}

static bool
inmem_index_fetch_tuple(struct IndexFetchTableData *iscan,
							 ItemPointer tid_p,
							 Snapshot snapshot,
							 TupleTableSlot *slot,
							 bool *call_again, bool *all_dead)
{
	InmemTuple* tuple = *(InmemTuple**)tid_p;
	inmem_fetch_slot(tuple, slot);
	return true;
}

static void
inmem_index_validate_scan(Relation heapRelation,
							   Relation indexRelation,
							   IndexInfo *indexInfo,
							   Snapshot snapshot,
							   ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static double
inmem_index_build_range_scan(Relation baseRelation,
								  Relation indexRelation,
								  IndexInfo *indexInfo,
								  bool allow_sync,
								  bool anyvisible,
								  bool progress,
								  BlockNumber start_blockno,
								  BlockNumber numblocks,
								  IndexBuildCallback callback,
								  void *callback_state,
								  TableScanDesc scan)
{
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	HeapTuple   heapTuple;
	InmemTuple* tuple;
	InmemTable* table = inmem_get_table(baseRelation);

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(baseRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	reltuples = 0;
	for (tuple = table->extent.next; tuple->values != NULL; tuple = tuple->next)
	{
		CHECK_FOR_INTERRUPTS();

		/* table_scan_getnextslot did the visibility check */
		reltuples += 1;

		/*
		 * TODO: Once we have in-place updates, like HOT, this will need
		 * to work harder, to figure out which tuple version to index.
		 */

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		inmem_fetch_slot(tuple, slot);

		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/* Call the AM's callback routine to process the tuple */
		heapTuple = ExecCopySlotHeapTuple(slot);
		heapTuple->t_self = slot->tts_tid;
		callback(indexRelation, &slot->tts_tid, values, isnull, true, callback_state);
		pfree(heapTuple);
	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}

static void
inmem_finish_bulk_insert(Relation relation, int options)
{
}

static void
inmem_relation_set_new_filenode(Relation rel,
								const RelFileNode *newrnode,
								char persistence,
								TransactionId *freezeXid,
								MultiXactId *minmulti)
{
	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();
}

static void
inmem_relation_nontransactional_truncate(Relation rel)
{
	InmemTable* table = inmem_get_table(rel);
	MemoryContextReset(table->ctx);
	table->extent.next = table->extent.prev = &table->extent;
	table->n_tuples = 0;
}

static void
inmem_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
inmem_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
								Relation OldIndex,
								bool use_sort,
								TransactionId OldestXmin,
								TransactionId *xid_cutoff,
								MultiXactId *multi_cutoff,
								double *num_tuples,
								double *tups_vacuumed,
								double *tups_recently_dead)
{
}

static bool
inmem_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								   BufferAccessStrategy bstrategy)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("function %s not implemented yet", __func__)));
	return false;
}

static bool
inmem_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								   double *liverows, double *deadrows,
								   TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("function %s not implemented yet", __func__)));
	return false;
}

static uint64
inmem_relation_size(Relation rel, ForkNumber forkNumber)
{
	InmemTable* table = inmem_get_table(rel);
	return MemoryContextMemAllocated(table->ctx, false);
}

static bool
inmem_relation_needs_toast_table(Relation rel)
{
	return false;
}

static void
inmem_relation_estimate_size(Relation rel, int32 *attr_widths,
								  BlockNumber *pages, double *tuples,
								  double *allvisfrac)
{
	InmemTable* table = inmem_get_table(rel);
	*pages = 1;
	*tuples = table->n_tuples;
	*allvisfrac = 1;
}

static bool
inmem_scan_bitmap_next_block(TableScanDesc sscan,
								  TBMIterateResult *tbmres)
{
	return true;
}

static bool
inmem_scan_bitmap_next_tuple(TableScanDesc sscan,
								  TBMIterateResult *tbmres,
								  TupleTableSlot *slot)
{
	InmemScanDesc* scan = (InmemScanDesc*)sscan;
	InmemTuple* tuple = scan->current->next;
	scan->current = tuple;
	if (tuple->values != NULL) {
		inmem_fetch_slot(tuple, slot);
		return true;
	} else {
		slot->tts_nvalid = 0;
		slot->tts_flags |= TTS_FLAG_EMPTY;
		return false;
	}
}

static bool
inmem_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("function %s not implemented yet", __func__)));
	return false;
}

static bool
inmem_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								  TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("function %s not implemented yet", __func__)));
	return false;
}

static void
inmem_vacuum_rel(Relation onerel, VacuumParams *params,
				 BufferAccessStrategy bstrategy)
{
}

static Size
inmem_estimate(Relation rel)
{
	return 0;
}
static Size
inmem_initialize(Relation rel, ParallelTableScanDesc pscan)
{

	return 0;
}

static void
inmem_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
}

/* ------------------------------------------------------------------------
 * Slot related callbacks for heap AM
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine inmem_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = inmem_slot_callbacks,

	.scan_begin = inmem_beginscan,
	.scan_end = inmem_endscan,
	.scan_rescan = heap_rescan,
	.scan_getnextslot = inmem_getnextslot,

	.parallelscan_estimate = inmem_estimate,
	.parallelscan_initialize = inmem_initialize,
	.parallelscan_reinitialize = inmem_reinitialize,

	.index_fetch_begin = inmem_begin_index_fetch,
	.index_fetch_reset = inmem_reset_index_fetch,
	.index_fetch_end = inmem_end_index_fetch,
	.index_fetch_tuple = inmem_index_fetch_tuple,

	.tuple_insert = inmem_insert,
	.tuple_insert_speculative = inmem_insert_speculative,
	.tuple_complete_speculative = inmem_complete_speculative,
	.multi_insert = inmem_multi_insert,
	.tuple_delete = inmem_delete,
	.tuple_update = inmem_update,
	.tuple_lock = inmem_lock_tuple,
	.finish_bulk_insert = inmem_finish_bulk_insert,

	.tuple_fetch_row_version = inmem_fetch_row_version,
	.tuple_get_latest_tid = inmem_get_latest_tid,
	.tuple_tid_valid = inmem_tuple_tid_valid,
	.tuple_satisfies_snapshot = inmem_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = inmem_compute_xid_horizon_for_tuples,

	.relation_set_new_filenode = inmem_relation_set_new_filenode,
	.relation_nontransactional_truncate = inmem_relation_nontransactional_truncate,
	.relation_copy_data = inmem_relation_copy_data,
	.relation_copy_for_cluster = inmem_relation_copy_for_cluster,
	.relation_vacuum = inmem_vacuum_rel,
	.scan_analyze_next_block = inmem_scan_analyze_next_block,
	.scan_analyze_next_tuple = inmem_scan_analyze_next_tuple,

	.index_build_range_scan = inmem_index_build_range_scan,
	.index_validate_scan = inmem_index_validate_scan,

	.relation_size = inmem_relation_size,
	.relation_needs_toast_table = inmem_relation_needs_toast_table,
	.relation_estimate_size = inmem_relation_estimate_size,

	.scan_bitmap_next_block = inmem_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = inmem_scan_bitmap_next_tuple,
	.scan_sample_next_block = inmem_scan_sample_next_block,
	.scan_sample_next_tuple = inmem_scan_sample_next_tuple
};

Datum
inmem_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&inmem_methods);
}

