/* -------------------------------------------------------------------------
 *
 * bdr_replication_identifier.c
 *		Replication identifiers emulation
 *
 * The replication identifier is used to track the position to which local
 * node has replayed the replication stream received from the remote node.
 * The information is used after reconnect to specify from which position
 * we want to receive changes.
 *
 * The replication identifier information has to be crash safe in order
 * to guarantee that we always start replaying the stream from the position
 * that was sucessfully saved to disk on local node.
 *
 * In BDR patched PostgreSQL we attach the replication identifier info into
 * xlog records. But because extensions don't have access to xlog, UDR has
 * to store data in a table which is less efficient.
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_replication_identifier.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr_replication_identifier.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "access/xlogdefs.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"

#include "miscadmin.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "bdr.h"

RepNodeId	replication_origin_id = InvalidRepNodeId; /* assumed identity */
XLogRecPtr	replication_origin_lsn;
TimestampTz	replication_origin_timestamp;

#define Natts_pg_replication_identifier		2
#define Anum_pg_replication_riident			1
#define Anum_pg_replication_riname			2

#define Natts_pg_replication_identifier_pos		3
#define Anum_pg_replication_pos_riident			1
#define Anum_pg_replication_pos_riremote_lsn	2
#define Anum_pg_replication_pos_rilocal_lsn 	3

static Oid ReplicationIdentifierRelationId = InvalidOid;
static Oid ReplicationIdentifierPosRelationId = InvalidOid;
static Oid ReplicationLocalIdentIndex = InvalidOid;
static Oid ReplicationPosLocalIdentIndex = InvalidOid;

/*
 * Replay progress of a single remote node.
 */
typedef struct ReplicationState
{
	/*
	 * Local identifier for the remote node.
	 */
	RepNodeId	local_identifier;

	/*
	 * Location of the latest commit from the remote side.
	 */
	XLogRecPtr	remote_lsn;

	/*
	 * Remember the local lsn of the commit record so we can XLogFlush() to it
	 * during a checkpoint so we know the commit record actually is safe on
	 * disk.
	 */
	XLogRecPtr	local_lsn;
} ReplicationState;

/*
 * Backend-local, cached element from ReplicationStates for use in a backend
 * replaying remote commits, so we don't have to search ReplicationStates for
 * the backends current RepNodeId.
 */
static ReplicationState *local_replication_state = NULL;

PGDLLEXPORT Datum bdr_replication_identifier_is_replaying(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum bdr_replication_identifier_advance(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum bdr_replication_identifier_drop(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bdr_replication_identifier_is_replaying);
PG_FUNCTION_INFO_V1(bdr_replication_identifier_advance);
PG_FUNCTION_INFO_V1(bdr_replication_identifier_drop);

static void
EnsureReplicationIdentifierRelationId(void)
{
	if (ReplicationIdentifierRelationId == InvalidOid ||
		ReplicationLocalIdentIndex == InvalidOid ||
		ReplicationIdentifierPosRelationId == InvalidOid ||
		ReplicationPosLocalIdentIndex == InvalidOid)
	{
		Oid schema_oid = get_namespace_oid("bdr", false);

		ReplicationIdentifierRelationId =
			bdr_lookup_relid("bdr_replication_identifier", schema_oid);
		ReplicationLocalIdentIndex =
			bdr_lookup_relid("bdr_replication_identifier_riiident_index", schema_oid);

		ReplicationIdentifierPosRelationId =
			bdr_lookup_relid("bdr_replication_identifier_pos", schema_oid);
		ReplicationPosLocalIdentIndex =
			bdr_lookup_relid("bdr_replication_identifier_pos_riiident_index", schema_oid);
	}
}

RepNodeId
GetReplicationIdentifier(char *riname, bool missing_ok)
{
	Relation		rel;
	Snapshot		snap;
	SysScanDesc		scan;
	ScanKeyData		key;
	HeapTuple		tuple;
	Oid				riident = InvalidOid;

	EnsureReplicationIdentifierRelationId();

	snap = RegisterSnapshot(GetLatestSnapshot());
	rel = heap_open(ReplicationIdentifierRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_replication_riname,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(riname));
	scan = systable_beginscan(rel, 0, true, snap, 1, &key);
	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		Datum		values[Natts_pg_replication_identifier];
		bool		nulls[Natts_pg_replication_identifier];

		heap_deform_tuple(tuple, RelationGetDescr(rel),
						  values, nulls);
		riident = DatumGetObjectId(values[0]);

	}
	else if (!missing_ok)
		elog(ERROR, "cache lookup failed for replication identifier named %s",
			riname);

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, RowExclusiveLock);

	return riident;
}

RepNodeId
CreateReplicationIdentifier(char *riname)
{
	Oid			riident;
	HeapTuple	tuple = NULL;
	Relation	rel,
				relpos;
	Datum		riname_d;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;

	riname_d = CStringGetTextDatum(riname);

	Assert(IsTransactionState());

	EnsureReplicationIdentifierRelationId();

	/*
	 * We need the numeric replication identifiers to be 16bit wide, so we
	 * cannot rely on the normal oid allocation. So we simply scan
	 * pg_replication_identifier for the first unused id. That's not
	 * particularly efficient, but this should be an fairly infrequent
	 * operation - we can easily spend a bit more code when it turns out it
	 * should be faster.
	 *
	 * We handle concurrency by taking an exclusive lock (allowing reads!)
	 * over the table for the duration of the search. Because we use a "dirty
	 * snapshot" we can read rows that other in-progress sessions have
	 * written, even though they would be invisible with normal snapshots. Due
	 * to the exclusive lock there's no danger that new rows can appear while
	 * we're checking.
	 */
	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationIdentifierRelationId, ExclusiveLock);
	relpos = heap_open(ReplicationIdentifierPosRelationId, ExclusiveLock);

	for (riident = InvalidOid + 1; riident <= UINT16_MAX; riident++)
	{
		bool		nulls[Natts_pg_replication_identifier_pos];
		Datum		values[Natts_pg_replication_identifier_pos];
		bool		collides;
		CHECK_FOR_INTERRUPTS();

		ScanKeyInit(&key,
					Anum_pg_replication_riident,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(riident));

		scan = systable_beginscan(rel, ReplicationLocalIdentIndex,
								  true /* indexOK */,
								  &SnapshotDirty,
								  1, &key);

		collides = HeapTupleIsValid(systable_getnext(scan));

		systable_endscan(scan);

		if (!collides)
		{
			/*
			 * Ok, found an unused riident, insert the new row and do a CCI,
			 * so our callers can look it up if they want to.
			 */
			memset(&nulls, false, sizeof(nulls));
			memset(values, 0, sizeof(values));

			values[0] = ObjectIdGetDatum(riident);
			values[1] = riname_d;

			tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
			simple_heap_insert(rel, tuple);
			CatalogUpdateIndexes(rel, tuple);

			/* Insert new tuple to the position tracking table too */
			memset(&nulls, false, sizeof(nulls));
			memset(values, 0, sizeof(values));

			values[0] = ObjectIdGetDatum(riident);

			tuple = heap_form_tuple(RelationGetDescr(relpos), values, nulls);
			simple_heap_insert(relpos, tuple);
			CatalogUpdateIndexes(relpos, tuple);

			CommandCounterIncrement();
			break;
		}
	}

	/* now release lock again,  */
	heap_close(rel, ExclusiveLock);
	heap_close(relpos, ExclusiveLock);

	if (tuple == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("no free replication id could be found")));

	heap_freetuple(tuple);
	return riident;
}

void
DropReplicationIdentifier(RepNodeId riident)
{
	HeapTuple	tuple = NULL;
	Relation	rel,
				relpos;
	SnapshotData SnapshotDirty;
	SysScanDesc	scan;
	ScanKeyData	key;

	Assert(IsTransactionState());

	EnsureReplicationIdentifierRelationId();

	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationIdentifierRelationId, ExclusiveLock);
	relpos = heap_open(ReplicationIdentifierPosRelationId, ExclusiveLock);

	/* Find and delete tuple from name table */
	ScanKeyInit(&key,
				Anum_pg_replication_riident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(riident));

	scan = systable_beginscan(rel, ReplicationLocalIdentIndex,
							  true /* indexOK */,
							  &SnapshotDirty,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
		simple_heap_delete(rel, &tuple->t_self);

	systable_endscan(scan);

	/* Find and delete tuple from position tracking table */
	ScanKeyInit(&key,
				Anum_pg_replication_pos_riident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(riident));

	scan = systable_beginscan(relpos, ReplicationPosLocalIdentIndex,
							  true /* indexOK */,
							  &SnapshotDirty,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
		simple_heap_delete(relpos, &tuple->t_self);

	systable_endscan(scan);

	CommandCounterIncrement();

	/* now release lock again,  */
	heap_close(rel, ExclusiveLock);
	heap_close(relpos, ExclusiveLock);
}

void
AdvanceReplicationIdentifier(RepNodeId node,
							 XLogRecPtr remote_commit,
							 XLogRecPtr local_commit)
{
	HeapTuple tuple = NULL;
	Relation rel;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;

	Assert(IsTransactionState());

	EnsureReplicationIdentifierRelationId();

	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationIdentifierPosRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_replication_pos_riident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(node));

	scan = systable_beginscan(rel, ReplicationPosLocalIdentIndex,
							  true /* indexOK */,
							  &SnapshotDirty,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		HeapTuple	newtuple;
		Datum		values[Natts_pg_replication_identifier_pos];
		bool		nulls[Natts_pg_replication_identifier_pos];

		heap_deform_tuple(tuple, RelationGetDescr(rel),
							  values, nulls);

		values[1] = LSNGetDatum(remote_commit);
		values[2] = LSNGetDatum(local_commit);

		newtuple = heap_form_tuple(RelationGetDescr(rel),
									values, nulls);
		simple_heap_update(rel, &tuple->t_self, newtuple);
		CatalogUpdateIndexes(rel, newtuple);
	}

	systable_endscan(scan);

	CommandCounterIncrement();

	/* now release lock again,  */
	heap_close(rel, RowExclusiveLock);
}

void
SetupCachedReplicationIdentifier(RepNodeId node)
{
	if (local_replication_state != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot setup replication origin when one is already setup")));

	local_replication_state = (ReplicationState *) palloc(sizeof(ReplicationState));
	local_replication_state->local_identifier = node;
	local_replication_state->remote_lsn = InvalidXLogRecPtr;
	local_replication_state->local_lsn = InvalidXLogRecPtr;
}

void
AdvanceCachedReplicationIdentifier(XLogRecPtr remote_commit,
								   XLogRecPtr local_commit)
{
	bool start_transaction = !IsTransactionState();

	Assert(local_replication_state != NULL);
	local_replication_state->local_lsn = local_commit;
	local_replication_state->remote_lsn = remote_commit;

	if (start_transaction)
		StartTransactionCommand();

	AdvanceReplicationIdentifier(local_replication_state->local_identifier, remote_commit, local_commit);

	if (start_transaction)
		CommitTransactionCommand();
}

XLogRecPtr
RemoteCommitFromCachedReplicationIdentifier(void)
{
	Assert(local_replication_state != NULL);
	return local_replication_state->remote_lsn;
}


/*
 * Lookup pg_replication_identifier tuple via its riident.
 *
 * The result needs to be ReleaseSysCache'ed and is an invalid HeapTuple if
 * the lookup failed.
 */
void
GetReplicationInfoByIdentifier(RepNodeId riident, bool missing_ok, char **riname)
{
	Relation		rel;
	Snapshot		snap;
	SysScanDesc		scan;
	ScanKeyData		key;
	HeapTuple		tuple = NULL;
	Form_pg_replication_identifier ric;

	EnsureReplicationIdentifierRelationId();

	snap = RegisterSnapshot(GetLatestSnapshot());
	rel = heap_open(ReplicationIdentifierRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_replication_riident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(riident));
	scan = systable_beginscan(rel, ReplicationLocalIdentIndex,
							  true /* indexOK */,
							  snap,
							  1, &key);
	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		ric = (Form_pg_replication_identifier) GETSTRUCT(tuple);
		*riname = pstrdup(text_to_cstring(&ric->riname));
	}

	if (!HeapTupleIsValid(tuple) && !missing_ok)
		elog(ERROR, "cache lookup failed for replication identifier id: %u",
			riident);

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, RowExclusiveLock);
}

static void
CheckReplicationIdentifierPrerequisites(bool check_slots)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superusers can query or manipulate replication identifiers")));

	if (check_slots && max_replication_slots == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot query or manipulate replication identifiers when max_replication_slots = 0")));

}

Datum
bdr_replication_identifier_is_replaying(PG_FUNCTION_ARGS)
{
	CheckReplicationIdentifierPrerequisites(true);

	PG_RETURN_BOOL(replication_origin_id != InvalidRepNodeId);
}

Datum
bdr_replication_identifier_advance(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	XLogRecPtr	remote_lsn = PG_GETARG_LSN(1);
	XLogRecPtr	local_lsn = PG_GETARG_LSN(2);
	RepNodeId	node;

	CheckReplicationIdentifierPrerequisites(true);

	node = GetReplicationIdentifier(text_to_cstring(name), false);

	AdvanceReplicationIdentifier(node, remote_lsn, local_lsn);

	PG_RETURN_VOID();
}

Datum
bdr_replication_identifier_drop(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RepNodeId	node;

	CheckReplicationIdentifierPrerequisites(true);

	node = GetReplicationIdentifier(text_to_cstring(name), false);

	DropReplicationIdentifier(node);

	PG_RETURN_VOID();
}
