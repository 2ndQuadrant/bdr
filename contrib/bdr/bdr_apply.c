/* -------------------------------------------------------------------------
 *
 * bdr_apply.c
 *		Replication!!!
 *
 * Replication???
 *
 * Copyright (C) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr_apply.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/committs.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/xact.h"

#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "executor/executor.h"

#include "libpq/pqformat.h"

#include "parser/parse_relation.h"
#include "parser/parse_type.h"

#include "replication/logical.h"
#include "replication/replication_identifier.h"

#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"

#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/* Useful for development:
#define VERBOSE_INSERT
#define VERBOSE_DELETE
#define VERBOSE_UPDATE
*/

typedef struct BDRTupleData
{
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	bool		changed[MaxTupleAttributeNumber];
} BDRTupleData;

bool		started_transaction = false;
Oid			QueuedDDLCommandsRelid = InvalidOid;
Oid			QueuedDropsRelid = InvalidOid;

/*
 * This code only runs within an apply bgworker, so we can stash a pointer to our
 * state in shm in a global for convenient access.
 *
 * TODO: make static once bdr_apply_main moved into bdr.c
 */
BdrApplyWorker *bdr_apply_worker = NULL;

static void build_index_scan_keys(EState *estate, ScanKey *scan_keys, TupleTableSlot *slot);
static void build_index_scan_key(ScanKey skey, Relation rel, Relation idx_rel, TupleTableSlot *slot);
static bool find_pkey_tuple(ScanKey skey, Relation rel, Relation idx_rel, TupleTableSlot *slot,
							bool lock, LockTupleMode mode);

static void UserTableUpdateIndexes(EState *estate, TupleTableSlot *slot);
static void UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot);
static EState *bdr_create_rel_estate(Relation rel);

/* read data from the wire */
static Relation read_rel(StringInfo s, LOCKMODE mode);
extern void read_tuple_parts(StringInfo s, Relation rel, BDRTupleData *tup);
static void read_tuple(StringInfo s, Relation rel, TupleTableSlot *slot);

static void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple);

static void check_apply_update(RepNodeId local_node_id, TimestampTz ts, bool *perform_update, bool *log_update);
static void do_log_update(RepNodeId local_node_id, bool apply_update, TimestampTz ts, Relation idxrel, HeapTuple old_key);
static void do_apply_update(Relation rel, EState *estate,
							TupleTableSlot *oldslot, TupleTableSlot *newslot,
							BDRTupleData new_tuple);
static void fetch_sysid_via_node_id(RepNodeId node_id, uint64 *sysid, TimeLineID *tli);

static void check_sequencer_wakeup(Relation rel);
static HeapTuple process_queued_drop(HeapTuple cmdtup);
static void process_queued_ddl_command(HeapTuple cmdtup, bool tx_just_started);
static bool bdr_performing_work(void);

void
process_remote_begin(StringInfo s)
{
	XLogRecPtr		origlsn;
	TimestampTz		committime;
	TimestampTz		current;
	char			statbuf[100];
	int				apply_delay = -1;
	const char     *apply_delay_str;

	Assert(bdr_apply_worker != NULL);

	started_transaction = false;

	origlsn = pq_getmsgint64(s);
	committime = pq_getmsgint64(s);

	/* setup state for commit and conflict detection */
	replication_origin_lsn = origlsn;
	replication_origin_timestamp = committime;

	snprintf(statbuf, sizeof(statbuf),
			"bdr_apply: BEGIN origin(source, orig_lsn, timestamp): %s, %X/%X, %s",
			 NameStr(bdr_apply_worker->name),
			(uint32) (origlsn >> 32), (uint32) origlsn,
			timestamptz_to_str(committime));

	elog(LOG, "%s", statbuf);

	pgstat_report_activity(STATE_RUNNING, statbuf);

	apply_delay_str = bdr_get_worker_option(NameStr(bdr_apply_worker->name), "apply_delay", true);
	if (apply_delay_str)
		/* This is an integer GUC, so parsing as an int can't fail */
		(void) parse_int(apply_delay_str, &apply_delay, 0, NULL);

	if (apply_delay == -1)
		apply_delay = bdr_default_apply_delay;

	/* don't want the overhead otherwise */
	if (apply_delay > 0)
	{
		current = GetCurrentIntegerTimestamp();

		/* ensure no weirdness due to clock drift */
		if (current > replication_origin_timestamp)
		{
			long		sec;
			int			usec;

			current = TimestampTzPlusMilliseconds(current,
												  -apply_delay);

			TimestampDifference(current, replication_origin_timestamp,
								&sec, &usec);
			/* FIXME: deal with overflow? */
			pg_usleep(usec + (sec * USECS_PER_SEC));
		}
	}
}

/*
 * Process a commit message from the output plugin, advance replication
 * identifiers, commit the local transaction, and determine whether replay
 * should continue.
 *
 * Returns true if apply should continue with the next record, false if replay
 * should stop after this record.
 */
bool
process_remote_commit(StringInfo s)
{
	XLogRecPtr		commit_lsn;
	TimestampTz		committime;
	TimestampTz		end_lsn;
	int				flags;
	RepNodeId		origin_id = InvalidRepNodeId;
	XLogRecPtr		origin_lsn = InvalidXLogRecPtr;

	Assert(bdr_apply_worker != NULL);

	flags = pq_getmsgint(s, 4);

	/* order of access to fields after flags is important */
	commit_lsn = pq_getmsgint64(s);
	end_lsn = pq_getmsgint64(s);
	committime = pq_getmsgint64(s);

	if (flags & BDR_OUTPUT_COMMIT_HAS_ORIGIN)
	{
		origin_id = pq_getmsgint(s, 2);
		origin_lsn = pq_getmsgint64(s);
	}

	elog(LOG, "COMMIT origin(lsn, end, timestamp): %X/%X, %X/%X, %s",
		 (uint32) (commit_lsn >> 32), (uint32) commit_lsn,
		 (uint32) (end_lsn >> 32), (uint32) end_lsn,
		 timestamptz_to_str(committime));

	Assert(commit_lsn == replication_origin_lsn);
	Assert(committime == replication_origin_timestamp);

	if (started_transaction)
	{
		CommitTransactionCommand();
	}

	pgstat_report_activity(STATE_IDLE, NULL);

	/*
	 * Advance the local replication identifier's lsn, so we don't replay this
	 * commit again.
	 *
	 * We always advance the local replication identifier for the origin node,
	 * even if we're really replaying a commit that's been forwarded from
	 * another node (per origin_id below). This is necessary to make sure we
	 * don't replay the same forwarded commit multiple times.
	 */
	AdvanceCachedReplicationIdentifier(end_lsn, XactLastCommitEnd);

	if (origin_id != InvalidRepNodeId)
	{
		/*
		 * We're replaying a record that's been forwarded from another node, so
		 * we need to advance the replication identifier for that node so that
		 * replay directly from that node will start from the correct LSN when
		 * we replicate directly.
		 *
		 * If it was from the immediate origin node, origin_id would be set to
		 * InvalidRepNodeId by the remote end's output plugin.
		 */
		AdvanceReplicationIdentifier(origin_id, origin_lsn, XactLastCommitEnd);
	}

	CurrentResourceOwner = bdr_saved_resowner;

	bdr_count_commit();

	/*
	 * Stop replay if we're doing limited replay and we've replayed up to the
	 * last record we're supposed to process.
	 */
	if (bdr_apply_worker->replay_stop_lsn != InvalidXLogRecPtr
			&& bdr_apply_worker->replay_stop_lsn <= end_lsn)
	{
		ereport(LOG,
				(errmsg("bdr apply %s finished processing; replayed to %X/%X of required %X/%X",
				 NameStr(bdr_apply_worker->name),
				 (uint32)(end_lsn>>32), (uint32)end_lsn,
				 (uint32)(bdr_apply_worker->replay_stop_lsn>>32), (uint32)bdr_apply_worker->replay_stop_lsn)));
		/*
		 * We clear the replay_stop_lsn field to indicate successful catchup,
		 * so we don't need a separate flag field in shmem for all apply
		 * workers.
		 */
		bdr_apply_worker->replay_stop_lsn = InvalidXLogRecPtr;
		return false;
	}
	else
		return true;
}

void
process_remote_insert(StringInfo s)
{
	char		action;
	EState	   *estate;
	TupleTableSlot *slot;
	TupleTableSlot *oldslot;
	Relation	rel;
	bool		started_tx;
#ifdef VERBOSE_INSERT
	StringInfoData o;
#endif
	ResultRelInfo *relinfo;
	ItemPointer conflicts;
	bool		conflict = false;
	ScanKey	   *index_keys;
	int			i;
	ItemPointerData conflicting_tid;

	ItemPointerSetInvalid(&conflicting_tid);

	started_tx = bdr_performing_work();

	Assert(bdr_apply_worker != NULL);

	/*
	 * Read tuple into a context that's long lived enough for CONCURRENTLY
	 * processing.
	 */
	MemoryContextSwitchTo(MessageContext);
	rel = read_rel(s, RowExclusiveLock);

	action = pq_getmsgbyte(s);
	if (action != 'N')
		elog(ERROR, "expected new tuple but got %d",
			 action);

	estate = bdr_create_rel_estate(rel);
	slot = ExecInitExtraTupleSlot(estate);
	oldslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(slot, RelationGetDescr(rel));
	ExecSetSlotDescriptor(oldslot, RelationGetDescr(rel));

	read_tuple(s, rel, slot);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	/* debug output */
#ifdef VERBOSE_INSERT
	initStringInfo(&o);
	tuple_to_stringinfo(&o, RelationGetDescr(rel), slot->tts_tuple);
	elog(LOG, "INSERT:%s", o.data);
	resetStringInfo(&o);
#endif

	/*
	 * Search for conflicting tuples.
	 */
	ExecOpenIndices(estate->es_result_relation_info);
	relinfo = estate->es_result_relation_info;
	index_keys = palloc0(relinfo->ri_NumIndices * sizeof(ScanKeyData*));
	conflicts = palloc0(relinfo->ri_NumIndices * sizeof(ItemPointerData));

	build_index_scan_keys(estate, index_keys, slot);

	/* do a SnapshotDirty search for conflicting tuples */
	for (i = 0; i < relinfo->ri_NumIndices; i++)
	{
		IndexInfo  *ii = relinfo->ri_IndexRelationInfo[i];
		bool found = false;

		Assert(ii->ii_Expressions == NIL);

		if (!ii->ii_Unique)
			continue;

		/* if conflict: wait */
		found = find_pkey_tuple(index_keys[i],
								rel, relinfo->ri_IndexRelationDescs[i],
								oldslot, true, LockTupleExclusive);

		/* alert if there's more than one conflicting unique key */
		if (found &&
			ItemPointerIsValid(&conflicting_tid) &&
			!ItemPointerEquals(&oldslot->tts_tuple->t_self,
							   &conflicting_tid))
		{
			/* FIXME: improve logging here */
			elog(ERROR, "diverging uniqueness conflict");
		}
		else if (found)
		{
			ItemPointerCopy(&oldslot->tts_tuple->t_self, &conflicting_tid);
			conflict = true;
			break;
		}
		else
			ItemPointerSetInvalid(&conflicts[i]);

		CHECK_FOR_INTERRUPTS();
	}

	/*
	 * If there's a conflict use the version created later, otherwise do a
	 * plain insert.
	 */
	if (conflict)
	{
		TransactionId xmin;
		TimestampTz local_ts;
		RepNodeId	local_node_id;
		bool		apply_update;
		bool		log_update;
		CommitExtraData local_node_id_raw;

		/* refetch tuple, check for old commit ts & origin */
		xmin = HeapTupleHeaderGetXmin(oldslot->tts_tuple->t_data);

		/*
		 * We now need to determine whether to keep the original version of
		 * the row, or apply the insert (as an update) we received.  We use
		 * the last-update-wins strategy for this, except when the new update
		 * comes from the same node that originated the previous version of
		 * the tuple.
		 */
		TransactionIdGetCommitTsData(xmin, &local_ts, &local_node_id_raw);
		local_node_id = local_node_id_raw;

		check_apply_update(local_node_id, local_ts,
						   &apply_update, &log_update);

		elog(LOG, "insert vs insert conflict: %s",
			 apply_update ? "update" : "ignore");

		if (apply_update)
		{
			simple_heap_update(rel,
							   &oldslot->tts_tuple->t_self,
							   slot->tts_tuple);
			/* races will be resolved by abort/retry */
			UserTableUpdateOpenIndexes(estate, slot);
		}
	}
	else
	{
		simple_heap_insert(rel, slot->tts_tuple);
		/* races will be resolved by abort/retry */
		UserTableUpdateOpenIndexes(estate, slot);
	}

	ExecCloseIndices(estate->es_result_relation_info);

	check_sequencer_wakeup(rel);

	/* execute DDL if insertion was into the ddl command queue */
	if (RelationGetRelid(rel) == QueuedDDLCommandsRelid ||
		RelationGetRelid(rel) == QueuedDropsRelid)
	{
		HeapTuple ht;
		LockRelId	lockid = rel->rd_lockInfo.lockRelId;
		TransactionId oldxid = GetTopTransactionId();

		/* there never should be conflicts on these */
		Assert(!conflict);

		/*
		 * Release transaction bound resources for CONCURRENTLY support.
		 */
		MemoryContextSwitchTo(MessageContext);
		ht = heap_copytuple(slot->tts_tuple);

		LockRelationIdForSession(&lockid, RowExclusiveLock);
		heap_close(rel, NoLock);

		ExecResetTupleTable(estate->es_tupleTable, true);
		FreeExecutorState(estate);

		if (RelationGetRelid(rel) == QueuedDDLCommandsRelid)
			process_queued_ddl_command(ht, started_tx);
		if (RelationGetRelid(rel) == QueuedDropsRelid)
			process_queued_drop(ht);

		rel = heap_open(QueuedDDLCommandsRelid, RowExclusiveLock);

		UnlockRelationIdForSession(&lockid, RowExclusiveLock);

		heap_close(rel, NoLock);

		if (oldxid != GetTopTransactionId())
		{
			CommitTransactionCommand();
			started_transaction = false;
		}
	}
	else
	{
		heap_close(rel, NoLock);
		ExecResetTupleTable(estate->es_tupleTable, true);
		FreeExecutorState(estate);
	}

	CommandCounterIncrement();
}

void
process_remote_update(StringInfo s)
{
	char		action;
	EState	   *estate;
	TupleTableSlot *newslot;
	TupleTableSlot *oldslot;
	bool		pkey_sent;
	bool		found_tuple;
	BDRTupleData new_tuple;
	Oid			idxoid;
	Relation	rel;
	Relation	idxrel;
	StringInfoData o;
	ScanKeyData skey[INDEX_MAX_KEYS];

	bdr_performing_work();

	rel = read_rel(s, RowExclusiveLock);

	action = pq_getmsgbyte(s);

	/* old key present, identifying key changed */
	if (action != 'K' && action != 'N')
		elog(ERROR, "expected action 'N' or 'K', got %c",
			 action);

	estate = bdr_create_rel_estate(rel);
	oldslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(oldslot, RelationGetDescr(rel));
	newslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(newslot, RelationGetDescr(rel));

	if (action == 'K')
	{
		pkey_sent = true;
		read_tuple(s, rel, oldslot);
		action = pq_getmsgbyte(s);
	}
	else
		pkey_sent = false;

	/* check for new  tuple */
	if (action != 'N')
		elog(ERROR, "expected action 'N', got %c",
			 action);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	/* read new tuple */
	read_tuple_parts(s, rel, &new_tuple);

	/*
	 * Generate key for lookup if the primary key didn't change.
	 */
	if (!pkey_sent)
	{
		HeapTuple old_key;
		old_key = heap_form_tuple(RelationGetDescr(rel),
								  new_tuple.values, new_tuple.isnull);
		ExecStoreTuple(old_key, oldslot, InvalidBuffer, true);
	}

#ifdef VERBOSE_UPDATE
	initStringInfo(&o);
	tuple_to_stringinfo(&o, RelationGetDescr(rel), oldslot->tts_tuple);
	appendStringInfo(&o, " to");
	tuple_to_stringinfo(&o, RelationGetDescr(rel), oldslot->tts_tuple);
	elog(LOG, "UPDATE:%s", o.data);
	resetStringInfo(&o);
#endif

	/* lookup index to build scankey */
	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);
	idxoid = rel->rd_replidindex;
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(rel));
		return;
	}

	/* open index, so we can build scan key for row */
	idxrel = index_open(idxoid, RowExclusiveLock);

	Assert(idxrel->rd_index->indisunique);

	build_index_scan_key(skey, rel, idxrel, oldslot);

	PushActiveSnapshot(GetTransactionSnapshot());

	/* look for tuple identified by the (old) primary key */
	found_tuple = find_pkey_tuple(skey, rel, idxrel, oldslot, true,
						pkey_sent ? LockTupleExclusive : LockTupleNoKeyExclusive);

	if (found_tuple)
	{
		TransactionId xmin;
		TimestampTz local_ts;
		RepNodeId	local_node_id;
		bool		apply_update;
		bool		log_update;

		CommitExtraData local_node_id_raw;

		/* refetch tuple, check for old commit ts & origin */
		xmin = HeapTupleHeaderGetXmin(oldslot->tts_tuple->t_data);

		/*
		 * We now need to determine whether to keep the original version of
		 * the row, or apply the update we received.  We use the
		 * last-update-wins strategy for this, except when the new update
		 * comes from the same node that originated the previous version of
		 * the tuple.
		 */
		TransactionIdGetCommitTsData(xmin, &local_ts, &local_node_id_raw);
		local_node_id = local_node_id_raw;

		check_apply_update(local_node_id, local_ts,
						   &apply_update, &log_update);

		if (log_update)
			do_log_update(local_node_id, apply_update, local_ts,
						  idxrel, oldslot->tts_tuple);

		if (apply_update)
			do_apply_update(rel, estate, oldslot, newslot, new_tuple);
		else
			bdr_count_update_conflict();
	}
	else
	{
		initStringInfo(&o);
		tuple_to_stringinfo(&o, RelationGetDescr(rel),
							oldslot->tts_tuple);
		bdr_count_update_conflict();

		ereport(ERROR,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: could not find existing tuple for pkey %s",
						o.data)));
	}

	PopActiveSnapshot();

	check_sequencer_wakeup(rel);

	/* release locks upon commit */
	index_close(idxrel, NoLock);
	heap_close(rel, NoLock);

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

void
process_remote_delete(StringInfo s)
{
#ifdef VERBOSE_DELETE
	StringInfoData o;
#endif
	char		action;
	EState	   *estate;
	TupleTableSlot *slot;
	Oid			idxoid;
	Relation	rel;
	Relation	idxrel;
	ScanKeyData skey[INDEX_MAX_KEYS];
	bool		found_old;

	Assert(bdr_apply_worker != NULL);

	bdr_performing_work();

	rel = read_rel(s, RowExclusiveLock);

	action = pq_getmsgbyte(s);

	if (action != 'K' && action != 'E')
		elog(ERROR, "expected action K or E got %c", action);

	if (action == 'E')
	{
		elog(WARNING, "got delete without pkey");
		return;
	}

	estate = bdr_create_rel_estate(rel);
	slot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(slot, RelationGetDescr(rel));

	read_tuple(s, rel, slot);

	/* lookup index to build scankey */
	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);
	idxoid = rel->rd_replidindex;
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(rel));
		return;
	}

	/* Now open the primary key index */
	idxrel = index_open(idxoid, RowExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

#ifdef VERBOSE_DELETE
	initStringInfo(&o);
	tuple_to_stringinfo(&o, RelationGetDescr(idxrel), slot->tts_tuple);
	elog(LOG, "DELETE old-key:%s", o.data);
	resetStringInfo(&o);
#endif

	PushActiveSnapshot(GetTransactionSnapshot());

	build_index_scan_key(skey, rel, idxrel, slot);

	/* try to find tuple via a (candidate|primary) key */
	found_old = find_pkey_tuple(skey, rel, idxrel, slot, true, LockTupleExclusive);

	if (found_old)
	{
		simple_heap_delete(rel, &slot->tts_tuple->t_self);
		bdr_count_delete();
	}
	else
	{
		StringInfoData s_key;

		bdr_count_delete_conflict();

		initStringInfo(&s_key);
		tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), slot->tts_tuple);

		ereport(ERROR,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: DELETE could not find existing tuple for pkey %s", s_key.data)));
		resetStringInfo(&s_key);
	}

	PopActiveSnapshot();

	check_sequencer_wakeup(rel);

	index_close(idxrel, NoLock);
	heap_close(rel, NoLock);

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

/*
 * Check whether a remote update conflicts with the local row version.
 *
 * perform_update, log_update is set to true if the update should be performed
 * and logged respectively
 */
static void
check_apply_update(RepNodeId local_node_id, TimestampTz local_ts,
				   bool *perform_update, bool *log_update)
{
	uint64		local_sysid,
				remote_sysid;
	TimeLineID	local_tli,
				remote_tli;
	int			cmp;

	if (local_node_id == bdr_apply_worker->origin_id)
	{
		/*
		 * If the row got updated twice within a single node, just apply the
		 * update with no conflict.  Don't warn/log either, regardless of the
		 * timing; that's just too common and valid since normal row level
		 * locking guarantees are met.
		 */
		*perform_update = true;
		*log_update = false;
		return;
	}
	else
	{
		/*
		 * Decide what update wins based on transaction timestamp difference.
		 * The later transaction wins.	If the timestamps compare equal, use
		 * sysid + TLI to discern.
		 */

		cmp = timestamptz_cmp_internal(replication_origin_timestamp,
									   local_ts);

		if (cmp > 0)
		{
			*perform_update = true;
			*log_update = false;
			return;
		}
		else if (cmp == 0)
		{
			fetch_sysid_via_node_id(local_node_id,
									&local_sysid, &local_tli);
			fetch_sysid_via_node_id(bdr_apply_worker->origin_id,
									&remote_sysid, &remote_tli);

			if (local_sysid < remote_sysid)
				*perform_update = true;
			else if (local_sysid > remote_sysid)
				*perform_update = false;
			else if (local_tli < remote_tli)
				*perform_update = true;
			else if (local_tli > remote_tli)
				*perform_update = false;
			else
				/* shouldn't happen */
				elog(ERROR, "unsuccessful node comparison");

			*log_update = true;
			return;
		}
		else
		{
			*perform_update = false;
			*log_update = true;
			return;
		}
	}

	elog(ERROR, "unreachable code");
}

static void
do_log_update(RepNodeId local_node_id, bool apply_update, TimestampTz ts,
			  Relation idxrel, HeapTuple old_key)
{
	StringInfoData s_key;
	char		remote_ts[MAXDATELEN + 1];
	char		local_ts[MAXDATELEN + 1];

	uint64		local_sysid,
				remote_sysid;
	TimeLineID	local_tli,
				remote_tli;


	fetch_sysid_via_node_id(local_node_id,
							&local_sysid, &local_tli);
	fetch_sysid_via_node_id(bdr_apply_worker->origin_id,
							&remote_sysid, &remote_tli);

	Assert(remote_sysid == bdr_apply_worker->sysid);
	Assert(remote_tli == bdr_apply_worker->timeline);

	memcpy(remote_ts, timestamptz_to_str(replication_origin_timestamp),
		   MAXDATELEN);
	memcpy(local_ts, timestamptz_to_str(ts),
		   MAXDATELEN);

	initStringInfo(&s_key);
	tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), old_key);

	ereport(LOG,
			(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
			 errmsg("CONFLICT: %s remote update originating at node " UINT64_FORMAT ":%u at ts %s; row was previously updated at %s node " UINT64_FORMAT ":%u at ts %s. PKEY:%s",
					apply_update ? "applying" : "skipping",
					remote_sysid, remote_tli, remote_ts,
					local_node_id == InvalidRepNodeId ? "local" : "remote",
					local_sysid, local_tli, local_ts, s_key.data)));
	resetStringInfo(&s_key);
}

static void
do_apply_update(Relation rel, EState *estate,
				TupleTableSlot *oldslot,
				TupleTableSlot *newslot,
				BDRTupleData new_tuple)
{
	HeapTuple	nt;

	nt = heap_modify_tuple(oldslot->tts_tuple, RelationGetDescr(rel),
						   new_tuple.values, new_tuple.isnull,
						   new_tuple.changed);
	ExecStoreTuple(nt, newslot, InvalidBuffer, true);
	simple_heap_update(rel, &oldslot->tts_tuple->t_self, newslot->tts_tuple);
	UserTableUpdateIndexes(estate, newslot);
	bdr_count_update();
}


static void
process_queued_ddl_command(HeapTuple cmdtup, bool tx_just_started)
{
	Relation	cmdsrel;
#ifdef NOT_YET
	HeapTuple	newtup;
#endif
	Datum		datum;
	char	   *type;
	char	   *identstr;
	char	   *cmdstr;
	bool		isnull;

	List	   *commands;
	ListCell   *command_i;
	bool		isTopLevel;
	MemoryContext oldcontext;

	/* ----
	 * We can't use spi here, because it implicitly assumes a transaction
	 * context. As we want to be able to replicate CONCURRENTLY commands,
	 * that's not going to work...
	 * So instead do all the work manually, being careful about managing the
	 * lifecycle of objects.
	 * ----
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

	cmdsrel = heap_open(QueuedDDLCommandsRelid, NoLock);

	/* fetch the object type */
	datum = heap_getattr(cmdtup, 1,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
		elog(ERROR, "null object type in command tuple in \"%s\"",
			 RelationGetRelationName(cmdsrel));
	type = TextDatumGetCString(datum);

	/* fetch the object identity */
	datum = heap_getattr(cmdtup, 2,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
		elog(ERROR, "null identity in command tuple for object of type %s",
			 RelationGetRelationName(cmdsrel));

	identstr = TextDatumGetCString(datum);

	/* finally fetch and execute the command */
	datum = heap_getattr(cmdtup, 3,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
		elog(ERROR, "null command in tuple for %s \"%s\"", type, identstr);

	cmdstr = TextDatumGetCString(datum);

	/* close relation, command execution might end/start xact */
	heap_close(cmdsrel, NoLock);

	commands = pg_parse_query(cmdstr);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Do a limited amount of safety checking against CONCURRENTLY commands
	 * executed in situations where they aren't allowed. The sender side shoul
	 * provide protection, but better be safe than sorry.
	 */
	isTopLevel = (list_length(commands) == 1) && tx_just_started;

	foreach(command_i, commands)
	{
		List	   *plantree_list;
		List	   *querytree_list;
		Node	   *command = (Node *) lfirst(command_i);
		const char *commandTag;
		Portal		portal;
		DestReceiver *receiver;

		/* temporarily push snapshot for parse analysis/planning */
		PushActiveSnapshot(GetTransactionSnapshot());

		oldcontext = MemoryContextSwitchTo(MessageContext);

		commandTag = CreateCommandTag(command);

		querytree_list = pg_analyze_and_rewrite(
			command, cmdstr, NULL, 0);

		plantree_list = pg_plan_queries(
			querytree_list, 0, NULL);

		PopActiveSnapshot();

		portal = CreatePortal("", true, true);
		PortalDefineQuery(portal, NULL,
						  cmdstr, commandTag,
						  plantree_list, NULL);
		PortalStart(portal, NULL, 0, InvalidSnapshot);

		receiver = CreateDestReceiver(DestNone);

		(void) PortalRun(portal, FETCH_ALL,
						 isTopLevel,
						 receiver, receiver,
						 NULL);
		(*receiver->rDestroy) (receiver);

		PortalDrop(portal, false);

		CommandCounterIncrement();

		MemoryContextSwitchTo(oldcontext);
	}

#ifdef NOT_YET
	/* FIXME: update tuple to set set "executed" to true */
	// newtup = heap_modify_tuple( .. );
	newtup = cmdtup;
#endif
}

static HeapTuple
process_queued_drop(HeapTuple cmdtup)
{
	Relation	cmdsrel;
	HeapTuple	newtup;
	Datum		arrayDatum;
	ArrayType  *array;
	bool		null;
	Oid			elmtype;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Oid			elmoutoid;
	bool		elmisvarlena;
	TupleDesc	elemdesc;
	Datum	   *values;
	int			nelems;
	int			i;
	ObjectAddresses *addresses;

	cmdsrel = heap_open(QueuedDropsRelid, AccessShareLock);
	arrayDatum = heap_getattr(cmdtup, 1,
							  RelationGetDescr(cmdsrel),
							  &null);
	if (null)
	{
		elog(WARNING, "null dropped object array in command tuple in \"%s\"",
			 RelationGetRelationName(cmdsrel));
		return cmdtup;
	}
	array = DatumGetArrayTypeP(arrayDatum);
	elmtype = ARR_ELEMTYPE(array);

	get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
	deconstruct_array(array, elmtype,
					  elmlen, elmbyval, elmalign,
					  &values, NULL, &nelems);

	getTypeOutputInfo(elmtype, &elmoutoid, &elmisvarlena);
	elemdesc = TypeGetTupleDesc(elmtype, NIL);

	addresses = new_object_addresses();

	for (i = 0; i < nelems; i++)
	{
		HeapTupleHeader	elemhdr;
		HeapTupleData tmptup;
		ObjectType objtype;
		Datum	datum;
		bool	isnull;
		char   *type;
		List   *objnames;
		List   *objargs = NIL;
		Relation objrel;
		ObjectAddress addr;

		elemhdr = (HeapTupleHeader) DatumGetPointer(values[i]);
		tmptup.t_len = HeapTupleHeaderGetDatumLength(elemhdr);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = elemhdr;

		/* obtain the object type as a C-string ... */
		datum = heap_getattr(&tmptup, 1, elemdesc, &isnull);
		if (isnull)
		{
			elog(WARNING, "null type !?");
			continue;
		}
		type = TextDatumGetCString(datum);
		objtype = unstringify_objtype(type);

		if (objtype == OBJECT_TYPE ||
			objtype == OBJECT_DOMAIN)
		{
			Datum  *values;
			bool   *nulls;
			int		nelems;
			char   *typestring;
			TypeName *typeName;

			datum = heap_getattr(&tmptup, 2, elemdesc, &isnull);
			if (isnull)
			{
				elog(WARNING, "null typename !?");
				continue;
			}

			deconstruct_array(DatumGetArrayTypeP(datum),
							  TEXTOID, -1, false, 'i',
							  &values, &nulls, &nelems);

			typestring = TextDatumGetCString(values[0]);
			typeName = typeStringToTypeName(typestring);
			objnames = typeName->names;
		}
		else if (objtype == OBJECT_FUNCTION ||
				 objtype == OBJECT_AGGREGATE ||
				 objtype == OBJECT_OPERATOR)
		{
			Datum  *values;
			bool   *nulls;
			int		nelems;
			int		i;
			char   *typestring;

			/* objname */
			objnames = NIL;
			datum = heap_getattr(&tmptup, 2, elemdesc, &isnull);
			if (isnull)
			{
				elog(WARNING, "null objname !?");
				continue;
			}

			deconstruct_array(DatumGetArrayTypeP(datum),
							  TEXTOID, -1, false, 'i',
							  &values, &nulls, &nelems);
			for (i = 0; i < nelems; i++)
				objnames = lappend(objnames,
								   makeString(TextDatumGetCString(values[i])));

			/* objargs are type names */
			datum = heap_getattr(&tmptup, 3, elemdesc, &isnull);
			if (isnull)
			{
				elog(WARNING, "null typename !?");
				continue;
			}

			deconstruct_array(DatumGetArrayTypeP(datum),
							  TEXTOID, -1, false, 'i',
							  &values, &nulls, &nelems);

			for (i = 0; i < nelems; i++)
			{
				typestring = TextDatumGetCString(values[i]);
				objargs = lappend(objargs, typeStringToTypeName(typestring));
			}
		}
		else
		{
			Datum  *values;
			bool   *nulls;
			int		nelems;
			int		i;

			/* objname */
			objnames = NIL;
			datum = heap_getattr(&tmptup, 2, elemdesc, &isnull);
			if (isnull)
			{
				elog(WARNING, "null objname !?");
				continue;
			}

			deconstruct_array(DatumGetArrayTypeP(datum),
							  TEXTOID, -1, false, 'i',
							  &values, &nulls, &nelems);
			for (i = 0; i < nelems; i++)
				objnames = lappend(objnames,
								   makeString(TextDatumGetCString(values[i])));

			datum = heap_getattr(&tmptup, 3, elemdesc, &isnull);
			if (!isnull)
			{
				Datum  *values;
				bool   *nulls;
				int		nelems;
				int		i;

				deconstruct_array(DatumGetArrayTypeP(datum),
								  TEXTOID, -1, false, 'i',
								  &values, &nulls, &nelems);
				for (i = 0; i < nelems; i++)
					objargs = lappend(objargs,
									  makeString(TextDatumGetCString(values[i])));
			}
		}

		addr = get_object_address(objtype, objnames, objargs, &objrel,
								  AccessExclusiveLock, false);
		/* unsupported object? */
		if (addr.classId == InvalidOid)
			continue;

		/*
		 * For certain objects, get_object_address returned us an open and
		 * locked relation.  Close it because we have no use for it; but
		 * keeping the lock seems easier than figure out lock level to release.
		 */
		if (objrel != NULL)
			relation_close(objrel, NoLock);

		add_exact_object_address(&addr, addresses);
	}

	performMultipleDeletions(addresses, DROP_RESTRICT, 0);

	newtup = cmdtup;

	heap_close(cmdsrel, AccessShareLock);

	return newtup;
}

static void
fetch_sysid_via_node_id(RepNodeId node_id, uint64 *sysid, TimeLineID *tli)
{
	if (node_id == InvalidRepNodeId)
	{
		*sysid = GetSystemIdentifier();
		*tli = ThisTimeLineID;
	}
	else
	{
		HeapTuple	node;
		Form_pg_replication_identifier node_class;
		char *ident;

		uint64 remote_sysid;
		Oid remote_dboid;
		TimeLineID remote_tli;
		Oid local_dboid;
		NameData replication_name;

		node = GetReplicationInfoByIdentifier(node_id, false);

		node_class = (Form_pg_replication_identifier) GETSTRUCT(node);

		ident = text_to_cstring(&node_class->riname);

		if (sscanf(ident, BDR_NODE_ID_FORMAT,
				   &remote_sysid, &remote_tli, &remote_dboid, &local_dboid,
				   NameStr(replication_name)) != 4)
			elog(ERROR, "could not parse sysid: %s", ident);
		ReleaseSysCache(node);
		pfree(ident);

		*sysid = remote_sysid;
		*tli = remote_tli;
	}
}

static bool
bdr_performing_work(void)
{
	if (started_transaction)
		return false;

	started_transaction = true;
	StartTransactionCommand();

	return true;
}

static void
check_sequencer_wakeup(Relation rel)
{
	Oid			reloid = RelationGetRelid(rel);

	if (reloid == BdrSequenceValuesRelid ||
		reloid == BdrSequenceElectionsRelid ||
		reloid == BdrVotesRelid)
		bdr_schedule_eoxact_sequencer_wakeup();
}

void
read_tuple_parts(StringInfo s, Relation rel, BDRTupleData *tup)
{
	TupleDesc	desc = RelationGetDescr(rel);
	int			i;
	int			rnatts;
	char		action;

	action = pq_getmsgbyte(s);

	if (action != 'T')
		elog(ERROR, "expected TUPLE, got %c", action);

	memset(tup->isnull, 1, sizeof(tup->isnull));
	memset(tup->changed, 1, sizeof(tup->changed));

	rnatts = pq_getmsgint(s, 4);

	if (desc->natts != rnatts)
		elog(ERROR, "tuple natts mismatch, %u vs %u", desc->natts, rnatts);

	/* FIXME: unaligned data accesses */

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = desc->attrs[i];
		char		kind = pq_getmsgbyte(s);
		const char *data;
		int			len;

		switch (kind)
		{
			case 'n': /* null */
				/* already marked as null */
				break;
			case 'u': /* unchanged column */
				tup->isnull[i] = false;
				tup->changed[i] = false;
				break;

			case 'b': /* binary format */
				tup->isnull[i] = false;
				len = pq_getmsgint(s, 4); /* read length */

				data = pq_getmsgbytes(s, len);

				/* and data */
				if (att->attbyval)
					tup->values[i] = fetch_att(data, true, len);
				else
					tup->values[i] = PointerGetDatum(data);
				break;
			case 's': /* send/recv format */
				{
					Oid typreceive;
					Oid typioparam;
					StringInfoData buf;

					tup->isnull[i] = false;
					len = pq_getmsgint(s, 4); /* read length */

					getTypeBinaryInputInfo(att->atttypid,
										   &typreceive, &typioparam);

					/* create StringInfo pointing into the bigger buffer */
					initStringInfo(&buf);
					/* and data */
					buf.data = (char *) pq_getmsgbytes(s, len);
					buf.len = len;
					tup->values[i] = OidReceiveFunctionCall(
						typreceive, &buf, typioparam, att->atttypmod);

					if (buf.len != buf.cursor)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
								 errmsg("incorrect binary data format")));
					break;
				}
			case 't': /* text format */
				{
					Oid typinput;
					Oid typioparam;

					tup->isnull[i] = false;
					len = pq_getmsgint(s, 4); /* read length */

					getTypeInputInfo(att->atttypid, &typinput, &typioparam);
					/* and data */
					data = (char *) pq_getmsgbytes(s, len);
					tup->values[i] = OidInputFunctionCall(
						typinput, (char *) data, typioparam, att->atttypmod);
				}
				break;
			default:
				elog(ERROR, "unknown column type '%c'", kind);
		}

		if (att->attisdropped && !tup->isnull[i])
			elog(ERROR, "data for dropped column");
	}
}

static Relation
read_rel(StringInfo s, LOCKMODE mode)
{
	int			relnamelen;
	int			nspnamelen;
	RangeVar*	rv;
	Oid			relid;

	rv = makeNode(RangeVar);

	nspnamelen = pq_getmsgint(s, 2);
	rv->schemaname = (char *) pq_getmsgbytes(s, nspnamelen);

	relnamelen = pq_getmsgint(s, 2);
	rv->relname = (char *) pq_getmsgbytes(s, relnamelen);

	relid = RangeVarGetRelidExtended(rv, mode, false, false, NULL, NULL);

	return heap_open(relid, NoLock);
}

/*
 * Read a tuple from s, return it as a HeapTuple allocated in the current
 * memory context. Also, reloid is set to the OID of the relation that this
 * tuple is related to.(The passed data contains schema and relation names;
 * they are resolved to the corresponding local OID.)
 */
static void
read_tuple(StringInfo s, Relation rel, TupleTableSlot *slot)
{
	BDRTupleData tupdata;
	HeapTuple	tup;

	read_tuple_parts(s, rel, &tupdata);
	tup = heap_form_tuple(RelationGetDescr(rel),
						  tupdata.values, tupdata.isnull);
	ExecStoreTuple(tup, slot, InvalidBuffer, true);
}

/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
	int			natt;
	Oid			oid;

	/* print oid of tuple, it's not included in the TupleDesc */
	if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid)
	{
		appendStringInfo(s, " oid[oid]:%u", oid);
	}

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		HeapTuple	type_tuple; /* information about a type */
		Form_pg_type type_form;
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		Datum		val;		/* definitely detoasted Datum */
		char	   *outputstr = NULL;
		bool		isnull;		/* column is null? */

		attr = tupdesc->attrs[natt];

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* gather type name */
		type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);
		type_form = (Form_pg_type) GETSTRUCT(type_tuple);

		/* print attribute name */
		appendStringInfoChar(s, ' ');
		appendStringInfoString(s, NameStr(attr->attname));

		/* print attribute type */
		appendStringInfoChar(s, '[');
		appendStringInfoString(s, NameStr(type_form->typname));
		appendStringInfoChar(s, ']');

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		ReleaseSysCache(type_tuple);

		/* get Datum from tuple */
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull)
			outputstr = "(null)";
		else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
			outputstr = "(unchanged-toast-datum)";
		else if (typisvarlena)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;

		/* print data */
		if (outputstr == NULL)
			outputstr = OidOutputFunctionCall(typoutput, val);

		appendStringInfoChar(s, ':');
		appendStringInfoString(s, outputstr);
	}
}

static EState *
bdr_create_rel_estate(Relation rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;

	estate = CreateExecutorState();

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = rel;
	resultRelInfo->ri_TrigInstrument = NULL;

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	return estate;
}

static void
UserTableUpdateIndexes(EState *estate, TupleTableSlot *slot)
{
	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(slot->tts_tuple))
		return;

	ExecOpenIndices(estate->es_result_relation_info);
	UserTableUpdateOpenIndexes(estate, slot);
	ExecCloseIndices(estate->es_result_relation_info);
}

static void
UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot)
{
	List	   *recheckIndexes = NIL;

	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(slot->tts_tuple))
		return;

	if (estate->es_result_relation_info->ri_NumIndices > 0)
	{
		recheckIndexes = ExecInsertIndexTuples(slot,
											   &slot->tts_tuple->t_self,
											   estate);

		if (recheckIndexes != NIL)
			elog(ERROR, "bdr doesn't don't support index rechecks");
	}

	/* FIXME: recheck the indexes */
	list_free(recheckIndexes);
}

static void
build_index_scan_keys(EState *estate, ScanKey *scan_keys, TupleTableSlot *slot)
{
	ResultRelInfo *relinfo;
	int i;

	relinfo = estate->es_result_relation_info;

	/* build scankeys for each index */
	for (i = 0; i < relinfo->ri_NumIndices; i++)
	{
		IndexInfo  *ii = relinfo->ri_IndexRelationInfo[i];

		if (!ii->ii_Unique)
			continue;

		scan_keys[i] = palloc(ii->ii_NumIndexAttrs * sizeof(ScanKeyData));
		build_index_scan_key(scan_keys[i],
					   relinfo->ri_RelationDesc,
					   relinfo->ri_IndexRelationDescs[i],
					   slot);
	}
}

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 */
static void
build_index_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleTableSlot *slot)
{
	int			attoff;
	Datum		indclassDatum;
	Datum		indkeyDatum;
	bool		isnull;
	oidvector  *opclass;
	int2vector  *indkey;
	HeapTuple	key = slot->tts_tuple;

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	indkeyDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indkey, &isnull);
	Assert(!isnull);
	indkey = (int2vector *) DatumGetPointer(indkeyDatum);


	for (attoff = 0; attoff < RelationGetNumberOfAttributes(idxrel); attoff++)
	{
		Oid			operator;
		Oid			opfamily;
		RegProcedure regop;
		int			pkattno = attoff + 1;
		int			mainattno = indkey->values[attoff];
		Oid			atttype = attnumTypeId(rel, mainattno);
		Oid			optype = get_opclass_input_type(opclass->values[attoff]);

		opfamily = get_opclass_family(opclass->values[attoff]);

		operator = get_opfamily_member(opfamily, optype,
									   optype,
									   BTEqualStrategyNumber);

		if (!OidIsValid(operator))
			elog(ERROR,
				 "could not lookup equality operator for type %u, optype %u in opfamily %u",
				 atttype, optype, opfamily);

		regop = get_opcode(operator);

		/* FIXME: convert type? */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					fastgetattr(key, mainattno,
								RelationGetDescr(rel), &isnull));
		if (isnull)
			elog(ERROR, "index tuple with a null column");
	}
}

/*
 * Search the index 'idxrel' for a tuple identified by 'skey' in 'rel'.
 *
 * If a matching tuple is found setup 'tid' to point to it and return true,
 * false is returned otherwise.
 */
static bool
find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel,
				TupleTableSlot *slot, bool lock, LockTupleMode mode)
{
	HeapTuple	scantuple;
	bool		found;
	IndexScanDesc scan;
	SnapshotData snap;
	TransactionId xwait;

	InitDirtySnapshot(snap);
	scan = index_beginscan(rel, idxrel,
						   &snap,
						   RelationGetNumberOfAttributes(idxrel),
						   0);

retry:
	found = false;

	index_rescan(scan, skey, RelationGetNumberOfAttributes(idxrel), NULL, 0);

	if ((scantuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		HeapTuple ht;

		found = true;
		/*
		 * Ugly trick to track the HeapTupleData pointing into a buffer in the
		 * slot.
		 */
		ht = MemoryContextAllocZero(slot->tts_mcxt, sizeof(HeapTupleData));
		memcpy(ht, scantuple, sizeof(HeapTupleData));
		ExecStoreTuple(ht, slot, scan->xs_cbuf, false);
		slot->tts_shouldFree = true;

		xwait = TransactionIdIsValid(snap.xmin) ?
			snap.xmin : snap.xmax;

		if (TransactionIdIsValid(xwait))
		{
			XactLockTableWait(xwait, NULL, NULL, XLTW_None);
			goto retry;
		}
	}

	if (lock && found)
	{
		Buffer buf;
		HeapUpdateFailureData hufd;
		HTSU_Result res;
		HeapTupleData locktup;

		ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

		PushActiveSnapshot(GetLatestSnapshot());

		res = heap_lock_tuple(rel, &locktup, GetCurrentCommandId(false), mode,
							  false /* wait */,
							  false /* don't follow updates */,
							  &buf, &hufd);
		/* the tuple slot already has the buffer pinned */
		ReleaseBuffer(buf);

		PopActiveSnapshot();

		switch (res)
		{
			case HeapTupleMayBeUpdated:
				break;
			case HeapTupleUpdated:
				/* XXX: Improve handling here */
				ereport(LOG,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("concurrent update, retrying")));
				goto retry;
			default:
				elog(ERROR, "unexpected HTSU_Result after locking: %u", res);
				break;
		}
	}

	index_endscan(scan);

	return found;
}
