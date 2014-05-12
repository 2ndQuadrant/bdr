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
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/xact.h"

#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "libpq/pqformat.h"

#include "parser/parse_type.h"

#include "replication/logical.h"
#include "replication/replication_identifier.h"

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
 * this should really be a static in bdr_apply.c, but bdr.c needs it for
 * bdr_apply_main currently.
 */
bool		exit_worker = false;

/* During apply, holds xid of remote transaction */
TransactionId replication_origin_xid = InvalidTransactionId;

/*
 * This code only runs within an apply bgworker, so we can stash a pointer to our
 * state in shm in a global for convenient access.
 *
 * TODO: make static once bdr_apply_main moved into bdr.c
 */
BdrApplyWorker *bdr_apply_worker = NULL;

/*
 * GUCs for this apply worker - again, this is fixed for the lifetime of the
 * worker so we can stash it in a global.
 */
BdrConnectionConfig *bdr_apply_config = NULL;

static BDRRelation *read_rel(StringInfo s, LOCKMODE mode);
extern void read_tuple_parts(StringInfo s, BDRRelation *rel, BDRTupleData *tup);
static void read_tuple(StringInfo s, BDRRelation *rel, TupleTableSlot *slot);
static void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple);

static void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple);

static void check_apply_update(RepNodeId local_node_id, TimestampTz local_ts,
				   BDRRelation *rel, HeapTuple local_tuple,
				   HeapTuple remote_tuple, HeapTuple *new_tuple,
				   bool *perform_update, bool *log_update,
				   BdrConflictResolution *resolution);
static void do_log_update(RepNodeId local_node_id, bool apply_update,
			  TimestampTz ts, Relation idxrel, BDRRelation *rel,
			  HeapTuple old_key, HeapTuple user_tuple);
static void do_apply_update(BDRRelation *rel, EState *estate, TupleTableSlot *oldslot,
				TupleTableSlot *newslot);

static void check_sequencer_wakeup(BDRRelation *rel);
static HeapTuple process_queued_drop(HeapTuple cmdtup);
static void process_queued_ddl_command(HeapTuple cmdtup, bool tx_just_started);
static bool bdr_performing_work(void);

static void process_remote_begin(StringInfo s);
static bool process_remote_commit(StringInfo s);
static void process_remote_insert(StringInfo s);
static void process_remote_update(StringInfo s);
static void process_remote_delete(StringInfo s);

static void
process_remote_begin(StringInfo s)
{
	XLogRecPtr		origlsn;
	TimestampTz		committime;
	TimestampTz		current;
	TransactionId	remote_xid;
	char			statbuf[100];
	int				apply_delay = bdr_apply_config->apply_delay;

	Assert(bdr_apply_worker != NULL);

	started_transaction = false;

	origlsn = pq_getmsgint64(s);
	committime = pq_getmsgint64(s);
	remote_xid = pq_getmsgint(s, 4);

	/* setup state for commit and conflict detection */
	replication_origin_lsn = origlsn;
	replication_origin_timestamp = committime;

	/* store remote xid for logging and debugging */
	replication_origin_xid = remote_xid;

	snprintf(statbuf, sizeof(statbuf),
			"bdr_apply: BEGIN origin(source, orig_lsn, timestamp): %s, %X/%X, %s",
			 NameStr(bdr_apply_config->name),
			(uint32) (origlsn >> 32), (uint32) origlsn,
			timestamptz_to_str(committime));

	elog(DEBUG1, "%s", statbuf);

	pgstat_report_activity(STATE_RUNNING, statbuf);

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
static bool
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

	elog(DEBUG1, "COMMIT origin(lsn, end, timestamp): %X/%X, %X/%X, %s",
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

	replication_origin_xid = InvalidTransactionId;

	/*
	 * Stop replay if we're doing limited replay and we've replayed up to the
	 * last record we're supposed to process.
	 */
	if (bdr_apply_worker->replay_stop_lsn != InvalidXLogRecPtr
			&& bdr_apply_worker->replay_stop_lsn <= end_lsn)
	{
		ereport(LOG,
				(errmsg("bdr apply %s finished processing; replayed to %X/%X of required %X/%X",
				 NameStr(bdr_apply_config->name),
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

static void
process_remote_insert(StringInfo s)
{
	char		action;
	EState	   *estate;
	TupleTableSlot *slot;
	TupleTableSlot *oldslot;
	BDRRelation	*rel;
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

	estate = bdr_create_rel_estate(rel->rel);
	slot = ExecInitExtraTupleSlot(estate);
	oldslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(slot, RelationGetDescr(rel->rel));
	ExecSetSlotDescriptor(oldslot, RelationGetDescr(rel->rel));

	read_tuple(s, rel, slot);

	if (rel->rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rel->rd_rel->relkind, RelationGetRelationName(rel->rel));

	/* debug output */
#ifdef VERBOSE_INSERT
	initStringInfo(&o);
	tuple_to_stringinfo(&o, RelationGetDescr(rel), slot->tts_tuple);
	elog(DEBUG1, "INSERT:%s", o.data);
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

		if (!ii->ii_Unique)
			continue;

		Assert(ii->ii_Expressions == NIL);

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
		BdrConflictResolution resolution;

		/* refetch tuple, check for old commit ts & origin */
		xmin = HeapTupleHeaderGetXmin(oldslot->tts_tuple->t_data);

		/*
		 * Use conflict triggers and/or last-update-wins to decide which tuple
		 * to retain.
		 */
		TransactionIdGetCommitTsData(xmin, &local_ts, &local_node_id_raw);
		local_node_id = local_node_id_raw;

		check_apply_update(local_node_id, local_ts, rel,
						   NULL, NULL, NULL, &apply_update, &log_update,
						   &resolution);

		if (log_update)
			/* TODO: Roll into conflict logging code */
			elog(DEBUG2, "bdr: insert vs insert conflict: %s",
				 apply_update ? "update" : "ignore");

		if (apply_update)
		{
			simple_heap_update(rel->rel,
							   &oldslot->tts_tuple->t_self,
							   slot->tts_tuple);
			/* races will be resolved by abort/retry */
			UserTableUpdateOpenIndexes(estate, slot);
		}

		if (log_update)
		{
			bdr_conflict_log(BdrConflictType_InsertInsert, resolution,
							 replication_origin_xid, rel, oldslot,
							 local_node_id, slot, NULL /*no error*/);
		}
	}
	else
	{
		simple_heap_insert(rel->rel, slot->tts_tuple);
		/* races will be resolved by abort/retry */
		UserTableUpdateOpenIndexes(estate, slot);
	}

	ExecCloseIndices(estate->es_result_relation_info);

	check_sequencer_wakeup(rel);

	/* execute DDL if insertion was into the ddl command queue */
	if (RelationGetRelid(rel->rel) == QueuedDDLCommandsRelid ||
		RelationGetRelid(rel->rel) == QueuedDropsRelid)
	{
		HeapTuple ht;
		LockRelId	lockid = rel->rel->rd_lockInfo.lockRelId;
		TransactionId oldxid = GetTopTransactionId();
		Oid relid = RelationGetRelid(rel->rel);
		Relation qrel;

		/* there never should be conflicts on these */
		Assert(!conflict);

		/*
		 * Release transaction bound resources for CONCURRENTLY support.
		 */
		MemoryContextSwitchTo(MessageContext);
		ht = heap_copytuple(slot->tts_tuple);

		LockRelationIdForSession(&lockid, RowExclusiveLock);
		bdr_heap_close(rel, NoLock);

		ExecResetTupleTable(estate->es_tupleTable, true);
		FreeExecutorState(estate);

		if (relid == QueuedDDLCommandsRelid)
			process_queued_ddl_command(ht, started_tx);
		if (relid == QueuedDropsRelid)
			process_queued_drop(ht);

		qrel = heap_open(QueuedDDLCommandsRelid, RowExclusiveLock);

		UnlockRelationIdForSession(&lockid, RowExclusiveLock);

		heap_close(qrel, NoLock);

		if (oldxid != GetTopTransactionId())
		{
			CommitTransactionCommand();
			started_transaction = false;
		}
	}
	else
	{
		bdr_heap_close(rel, NoLock);
		ExecResetTupleTable(estate->es_tupleTable, true);
		FreeExecutorState(estate);
	}

	CommandCounterIncrement();
}

static void
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
	BDRRelation	*rel;
	Relation	idxrel;
	StringInfoData o;
	ScanKeyData skey[INDEX_MAX_KEYS];
	HeapTuple	user_tuple = NULL,
				remote_tuple = NULL;

	bdr_performing_work();

	rel = read_rel(s, RowExclusiveLock);

	action = pq_getmsgbyte(s);

	/* old key present, identifying key changed */
	if (action != 'K' && action != 'N')
		elog(ERROR, "expected action 'N' or 'K', got %c",
			 action);

	estate = bdr_create_rel_estate(rel->rel);
	oldslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(oldslot, RelationGetDescr(rel->rel));
	newslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(newslot, RelationGetDescr(rel->rel));

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

	if (rel->rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rel->rd_rel->relkind, RelationGetRelationName(rel->rel));

	/* read new tuple */
	read_tuple_parts(s, rel, &new_tuple);

	/*
	 * Generate key for lookup if the primary key didn't change.
	 */
	if (!pkey_sent)
	{
		HeapTuple old_key;
		old_key = heap_form_tuple(RelationGetDescr(rel->rel),
								  new_tuple.values, new_tuple.isnull);
		ExecStoreTuple(old_key, oldslot, InvalidBuffer, true);
	}

	/* lookup index to build scankey */
	if (rel->rel->rd_indexvalid == 0)
		RelationGetIndexList(rel->rel);
	idxoid = rel->rel->rd_replidindex;
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(rel->rel));
		return;
	}

	/* open index, so we can build scan key for row */
	idxrel = index_open(idxoid, RowExclusiveLock);

	Assert(idxrel->rd_index->indisunique);

	build_index_scan_key(skey, rel->rel, idxrel, oldslot);

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
		BdrConflictResolution resolution;

		CommitExtraData local_node_id_raw;

		remote_tuple = heap_modify_tuple(oldslot->tts_tuple,
										 RelationGetDescr(rel->rel),
										 new_tuple.values,
										 new_tuple.isnull,
										 new_tuple.changed);

		ExecStoreTuple(remote_tuple, newslot, InvalidBuffer, true);

#ifdef VERBOSE_UPDATE
		initStringInfo(&o);
		tuple_to_stringinfo(&o, RelationGetDescr(rel), oldslot->tts_tuple);
		appendStringInfo(&o, " to");
		tuple_to_stringinfo(&o, RelationGetDescr(rel), remote_tuple);
		elog(DEBUG1, "UPDATE:%s", o.data);
		resetStringInfo(&o);
#endif

		/* refetch tuple, check for old commit ts & origin */
		xmin = HeapTupleHeaderGetXmin(oldslot->tts_tuple->t_data);

		/*
		 * Use conflict triggers and/or last-update-wins to decide which tuple
		 * to retain.
		 */
		TransactionIdGetCommitTsData(xmin, &local_ts, &local_node_id_raw);
		local_node_id = local_node_id_raw;

		check_apply_update(local_node_id, local_ts, rel, oldslot->tts_tuple,
						   remote_tuple, &user_tuple, &apply_update,
						   &log_update, &resolution);

		if (log_update)
			do_log_update(local_node_id, apply_update, local_ts,
						  idxrel, rel, oldslot->tts_tuple, user_tuple);

		if (apply_update)
		{
			/* user provided a new tuple; form it to a bdr tuple */
			if (user_tuple != NULL)
			{
#ifdef VERBOSE_UPDATE
				initStringInfo(&o);
				tuple_to_stringinfo(&o, RelationGetDescr(rel), user_tuple);
				elog(DEBUG1, "USER tuple:%s", o.data);
				resetStringInfo(&o);
#endif

				ExecStoreTuple(user_tuple, newslot, InvalidBuffer, true);
			}

			do_apply_update(rel, estate, oldslot, newslot);
		}
		else
			bdr_count_update_conflict();
	}
	else
	{
		/*
		 * Update target is missing. We don't know if this is an update-vs-delete
		 * conflict or if the target tuple came from some 3rd node and hasn't yet
		 * been applied to the local node.
		 */
		initStringInfo(&o);
		tuple_to_stringinfo(&o, RelationGetDescr(rel->rel),
							oldslot->tts_tuple);
		bdr_count_update_conflict();

		ereport(LOG,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: could not find existing tuple for pkey %s",
						o.data)));
	}

	PopActiveSnapshot();

	check_sequencer_wakeup(rel);

	/* release locks upon commit */
	index_close(idxrel, NoLock);
	bdr_heap_close(rel, NoLock);

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
process_remote_delete(StringInfo s)
{
#ifdef VERBOSE_DELETE
	StringInfoData o;
#endif
	char		action;
	EState	   *estate;
	TupleTableSlot *slot;
	Oid			idxoid;
	BDRRelation	*rel;
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

	estate = bdr_create_rel_estate(rel->rel);
	slot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(slot, RelationGetDescr(rel->rel));

	read_tuple(s, rel, slot);

	/* lookup index to build scankey */
	if (rel->rel->rd_indexvalid == 0)
		RelationGetIndexList(rel->rel);
	idxoid = rel->rel->rd_replidindex;
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(rel->rel));
		return;
	}

	/* Now open the primary key index */
	idxrel = index_open(idxoid, RowExclusiveLock);

	if (rel->rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rel->rd_rel->relkind, RelationGetRelationName(rel->rel));

#ifdef VERBOSE_DELETE
	initStringInfo(&o);
	tuple_to_stringinfo(&o, RelationGetDescr(idxrel), slot->tts_tuple);
	elog(DEBUG1, "DELETE old-key:%s", o.data);
	resetStringInfo(&o);
#endif

	PushActiveSnapshot(GetTransactionSnapshot());

	build_index_scan_key(skey, rel->rel, idxrel, slot);

	/* try to find tuple via a (candidate|primary) key */
	found_old = find_pkey_tuple(skey, rel, idxrel, slot, true, LockTupleExclusive);

	if (found_old)
	{
		simple_heap_delete(rel->rel, &slot->tts_tuple->t_self);
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
	bdr_heap_close(rel, NoLock);

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

/*
 * Check whether a remote insert or update conflicts with the local row
 * version.
 *
 * User-defined conflict triggers get invoked here.
 *
 * perform_update, log_update is set to true if the update should be performed
 * and logged respectively
 *
 * resolution is set to indicate how the conflict was resolved if log_update
 * is true. Its value is undefined if log_update is false.
 */
static void
check_apply_update(RepNodeId local_node_id, TimestampTz local_ts,
				 BDRRelation *rel, HeapTuple local_tuple, HeapTuple remote_tuple,
				HeapTuple *new_tuple, bool *perform_update, bool *log_update,
				BdrConflictResolution *resolution)
{
	uint64		local_sysid,
				remote_sysid;
	TimeLineID	local_tli,
				remote_tli;
	int			cmp, microsecs;
	long		secs;

	bool		skip = false;

	/*
	 * ensure that new_tuple is initialized with NULL; there are cases where
	 * we wouldn't touch it otherwise.
	 */
	if (new_tuple)
		*new_tuple = NULL;

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
		 * Decide whether to keep the remote or local tuple based on a conflict
		 * trigger (if defined) or last-update-wins.
		 *
		 * If the caller doesn't provide storage for the conflict handler to
		 * store a new tuple in, don't fire any conflict triggers.
		 */
		*log_update = true;

		if (new_tuple)
		{
			/*
			 * --------------
			 * Conflict trigger conflict handling - let the user decide whether to:
			 * - Ignore the remote update;
			 * - Supply a new tuple to replace the current tuple; or
			 * - Take no action and fall through to the next handling option
			 * --------------
			 */
			TimestampDifference(replication_origin_timestamp, local_ts,
								&secs, &microsecs);

			*new_tuple = bdr_conflict_handlers_resolve(rel, local_tuple,
													   remote_tuple, "UPDATE",
													   BDRUpdateUpdateConflictHandler,
													   abs(secs) * 1000000 + abs(microsecs),
													   &skip);

			if (skip)
			{
				*perform_update = false;
				*resolution = BdrConflictResolution_ConflictTriggerSkipChange;
				return;
			}
			else if (*new_tuple)
			{
				*perform_update = true;
				*resolution = BdrConflictResolution_ConflictTriggerReturnedTuple;
				return;
			}

			/*
			 * if user decided not to skip the conflict but didn't provide a
			 * resolving tuple we fall back to default handling
			 */
		}

		/* Last update wins conflict handling */
		cmp = timestamptz_cmp_internal(replication_origin_timestamp, local_ts);
		if (cmp > 0)
		{
			/* The most recent update is the remote one; apply it */
			*perform_update = true;
			*resolution = BdrConflictResolution_LastUpdateWins_KeepRemote;
			return;
		}
		else if (cmp < 0)
		{
			/* The most recent update is the local one; retain it */
			*perform_update = false;
			*resolution = BdrConflictResolution_LastUpdateWins_KeepLocal;
			return;
		}
		else if (cmp == 0)
		{
			/*
			 * Timestamps are equal. Use sysid + timeline id to decide which
			 * tuple to retain.
			 */
			fetch_sysid_via_node_id(local_node_id,
									&local_sysid, &local_tli);
			fetch_sysid_via_node_id(bdr_apply_worker->origin_id,
									&remote_sysid, &remote_tli);

			/*
			 * Apply the user tuple or, if none supplied, fall back to "last
			 * update wins"
			 */
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

			/*
			 * We don't log whether we used timestamp, sysid or timeline id to
			 * decide which tuple to retain. That'll be in the log record
			 * anyway, so we can reconstruct the decision from the log record
			 * later.
			 */
			if (*perform_update)
				*resolution = BdrConflictResolution_LastUpdateWins_KeepRemote;
			else
				*resolution = BdrConflictResolution_LastUpdateWins_KeepLocal;

			return;
		}
	}

	elog(ERROR, "unreachable code");
}

static void
do_log_update(RepNodeId local_node_id, bool apply_update, TimestampTz ts,
			  Relation idxrel, BDRRelation *rel, HeapTuple old_key,
			  HeapTuple user_tuple)
{
	StringInfoData s_key,
				s_user_tuple;
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

	if (user_tuple != NULL)
	{
		initStringInfo(&s_user_tuple);
		tuple_to_stringinfo(&s_user_tuple, RelationGetDescr(rel->rel),
							user_tuple);

		ereport(LOG,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: %s remote update originating at node " UINT64_FORMAT ":%u at ts %s; row was previously updated at %s node " UINT64_FORMAT ":%u at ts %s. PKEY:%s, resolved by user tuple:%s",
						apply_update ? "applying" : "skipping",
						remote_sysid, remote_tli, remote_ts,
						local_node_id == InvalidRepNodeId ? "local" : "remote",
						local_sysid, local_tli, local_ts, s_key.data,
						s_user_tuple.data)));
	}
	else
	{
		ereport(LOG,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: %s remote update originating at node " UINT64_FORMAT ":%u at ts %s; row was previously updated at %s node " UINT64_FORMAT ":%u at ts %s. PKEY:%s",
						apply_update ? "applying" : "skipping",
						remote_sysid, remote_tli, remote_ts,
						local_node_id == InvalidRepNodeId ? "local" : "remote",
						local_sysid, local_tli, local_ts, s_key.data)));
	}

	resetStringInfo(&s_key);
}

static void
do_apply_update(BDRRelation *rel, EState *estate, TupleTableSlot *oldslot,
				TupleTableSlot *newslot)
{
	simple_heap_update(rel->rel, &oldslot->tts_tuple->t_self, newslot->tts_tuple);
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
	char	   *command_tag;
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

	/* fetch the command tag */
	datum = heap_getattr(cmdtup, 3,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
		elog(ERROR, "null command tag in command tuple in \"%s\"",
			 RelationGetRelationName(cmdsrel));
	command_tag = TextDatumGetCString(datum);

	/* finally fetch and execute the command */
	datum = heap_getattr(cmdtup, 4,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
		elog(ERROR, "null command for \"%s\" command tuple", command_tag);

	cmdstr = TextDatumGetCString(datum);

	/* close relation, command execution might end/start xact */
	heap_close(cmdsrel, NoLock);

	commands = pg_parse_query(cmdstr);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Do a limited amount of safety checking against CONCURRENTLY commands
	 * executed in situations where they aren't allowed. The sender side should
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
	arrayDatum = heap_getattr(cmdtup, 3,
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

void
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
check_sequencer_wakeup(BDRRelation *rel)
{
	Oid			reloid = RelationGetRelid(rel->rel);

	if (reloid == BdrSequenceValuesRelid ||
		reloid == BdrSequenceElectionsRelid ||
		reloid == BdrVotesRelid)
		bdr_schedule_eoxact_sequencer_wakeup();
}

void
read_tuple_parts(StringInfo s, BDRRelation *rel, BDRTupleData *tup)
{
	TupleDesc	desc = RelationGetDescr(rel->rel);
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

static BDRRelation *
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

	return bdr_heap_open(relid, NoLock);
}

/*
 * Read a tuple from s, return it as a HeapTuple allocated in the current
 * memory context. Also, reloid is set to the OID of the relation that this
 * tuple is related to.(The passed data contains schema and relation names;
 * they are resolved to the corresponding local OID.)
 */
static void
read_tuple(StringInfo s, BDRRelation *rel, TupleTableSlot *slot)
{
	BDRTupleData tupdata;
	HeapTuple	tup;

	read_tuple_parts(s, rel, &tupdata);
	tup = heap_form_tuple(RelationGetDescr(rel->rel),
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

/*
 * Read a remote action type and process the action record.
 *
 * May set exit_worker to stop processing before next record.
 */
void
bdr_process_remote_action(StringInfo s)
{
	char action = pq_getmsgbyte(s);
	switch (action)
	{
			/* BEGIN */
		case 'B':
			process_remote_begin(s);
			break;
			/* COMMIT */
		case 'C':
			if (!process_remote_commit(s))
				exit_worker = true;
			break;
			/* INSERT */
		case 'I':
			process_remote_insert(s);
			break;
			/* UPDATE */
		case 'U':
			process_remote_update(s);
			break;
			/* DELETE */
		case 'D':
			process_remote_delete(s);
			break;
		default:
			elog(ERROR, "unknown action of type %c", action);
	}
}
