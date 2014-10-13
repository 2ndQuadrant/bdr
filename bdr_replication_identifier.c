/* -------------------------------------------------------------------------
 *
 * bdr_replication_identifier.c
 *		Replication identifiers emulation
 *
 *
 * Copyright (C) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr_replication_identifier.c
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

#define Natts_pg_replication_identifier		4
#define Anum_pg_replication_riident			1
#define Anum_pg_replication_riname			2
#define Anum_pg_replication_riremote_lsn	3
#define Anum_pg_replication_rilocal_lsn 	4


static Oid ReplicationIdentifierRelationId = InvalidOid;
static Oid ReplicationLocalIdentIndex = InvalidOid;

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

static void
EnsureReplicationIdentifierRelationId(void)
{
	if (ReplicationIdentifierRelationId == InvalidOid ||
		ReplicationLocalIdentIndex == InvalidOid)
	{
		Oid schema_oid = get_namespace_oid("bdr", false);

		ReplicationIdentifierRelationId =
			bdr_lookup_relid("bdr_replication_identifier", schema_oid);
		ReplicationLocalIdentIndex =
			bdr_lookup_relid("bdr_replication_identifier_riiident_index", schema_oid);
	}
}

RepNodeId
GetReplicationIdentifier(char *riname, bool missing_ok)
{
    Relation        rel;
	Snapshot		snap;
	SysScanDesc		scan;
	ScanKeyData     key;
	HeapTuple		tuple;
	Oid		        riident = InvalidOid;

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
	Oid		riident;
	HeapTuple tuple = NULL;
	Relation rel;
	Datum	riname_d;
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

	for (riident = InvalidOid + 1; riident <= UINT16_MAX; riident++)
	{
		bool		nulls[Natts_pg_replication_identifier];
		Datum		values[Natts_pg_replication_identifier];
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
			memset(&nulls, 0, sizeof(nulls));

			values[0] = ObjectIdGetDatum(riident);
			values[1] = riname_d;

			tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
			simple_heap_insert(rel, tuple);
			CatalogUpdateIndexes(rel, tuple);
			CommandCounterIncrement();
			break;
		}
	}

	/* now release lock again,  */
	heap_close(rel, ExclusiveLock);

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
	HeapTuple tuple = NULL;
	Relation rel;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;

	Assert(IsTransactionState());

	EnsureReplicationIdentifierRelationId();

	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationIdentifierRelationId, ExclusiveLock);

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

	CommandCounterIncrement();

	/* now release lock again,  */
	heap_close(rel, ExclusiveLock);
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

	rel = heap_open(ReplicationIdentifierRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_replication_riident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(node));

	scan = systable_beginscan(rel, ReplicationLocalIdentIndex,
							  true /* indexOK */,
							  &SnapshotDirty,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
    {
		HeapTuple	newtuple;
		Datum		values[Natts_pg_replication_identifier];
		bool		nulls[Natts_pg_replication_identifier];

        heap_deform_tuple(tuple, RelationGetDescr(rel),
							  values, nulls);

        values[2] = LSNGetDatum(remote_commit);
        values[3] = LSNGetDatum(local_commit);

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
HeapTuple
GetReplicationInfoByIdentifier(RepNodeId riident, bool missing_ok)
{
    Relation        rel;
	Snapshot		snap;
	SysScanDesc		scan;
	ScanKeyData     key;
	HeapTuple		tuple = NULL;

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

    if (!HeapTupleIsValid(tuple) && !missing_ok)
		elog(ERROR, "cache lookup failed for replication identifier id: %u",
			riident);

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, RowExclusiveLock);

	return tuple;
}

