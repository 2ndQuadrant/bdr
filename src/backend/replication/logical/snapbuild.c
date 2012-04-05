/*-------------------------------------------------------------------------
 *
 * snapbuild.c
 *
 *	  Support for building timetravel snapshots based on the contents of the
 *	  wal
 *
 * NOTES:
 *
 * We build snapshots which can *only* be used to read catalog contents by
 * reading the wal stream. The aim is to provide mvcc and SnapshotNow snapshots
 * that behave the same as their respective counterparts would have at the time
 * the XLogRecord was generated. This is done to provide a reliable environment
 * for decoding those records into every format that pleases the author of an
 * output plugin.
 *
 * To build the snapshots we reuse the infrastructure built for hot
 * standby. The snapshots we build look different than HS' because we have
 * different needs. To successfully decode data from the WAL we only need to
 * access catalogs/(sys|rel|cat)cache, not the actual user tables. And we need
 * to build multiple, vastly different, ones, without being able to fully rely
 * on the clog for information about committed transactions because they might
 * commit in the future from the POV of the wal entry were currently decoding.

 * As the percentage of transactions modifying the catalog normally is fairly
 * small, instead of keeping track of all running transactions and treating
 * everything inside (xmin, xmax) thats not known to be running as commited we
 * do the contrary. That is we keep a list of transactions between
 * snapshot->(xmin, xmax) that we consider committed, everything else is
 * considered aborted/in progress.
 * That also allows us not to care about subtransactions before they have
 * committed.
 *
 * Classic SnapshotNow behaviour - which is mainly used for efficiency, not for
 * correctness - is not actually required by any of the routines that we need
 * during decoding and is hard to emulate fully. Instead we build snapshots
 * with MVCC behaviour that are updated whenever another transaction
 * commits. That gives behaviour consistent with a SnapshotNow behaviour
 * happening in exactly that instant without other transactions interfering.
 *
 * One additional complexity of doing this is that to handle mixed DDL/DML
 * transactions we need Snapshots that see intermediate states in a
 * transaction. In normal operation this is achieved by using
 * CommandIds/cmin/cmax. The problem with this however is that for space
 * efficiency reasons only one value of that is stored (cf. combocid.c). To get
 * arround that we log additional information which allows us to get the
 * original (cmin, cmax) pair during visibility checks.
 *
 * To facilitate all this we need our own visibility routine, as the normal
 * ones are optimized for different usecases. We also need the code to use out
 * special snapshots automatically whenever SnapshotNow behaviour is expected
 * (specifying our snapshot everywhere would be far to invasive).
 *
 * To replace the normal SnapshotNows snapshots use the SetupDecodingSnapshots
 * RevertFromDecodingSnapshots functions.
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/snapbuild.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "access/heapam_xlog.h"
#include "access/rmgr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlogreader.h"

#include "catalog/catalog.h"
#include "catalog/pg_control.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace.h"

#include "miscadmin.h"

#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "replication/logical.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "storage/block.h" /* debugging output */
#include "storage/copydir.h" /* fsync_fname */
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/standby.h"
#include "storage/sinval.h"

/* transaction state manipulation functions */
static void SnapBuildEndTxn(Snapstate *snapstate, TransactionId xid);

static void SnapBuildAbortTxn(Snapstate *state, TransactionId xid, int nsubxacts,
							  TransactionId *subxacts);

static void SnapBuildCommitTxn(Snapstate *snapstate, ReorderBuffer *reorder,
							   XLogRecPtr lsn, TransactionId xid,
							   int nsubxacts, TransactionId *subxacts);

/* ->running manipulation */
static bool SnapBuildTxnIsRunning(Snapstate *snapstate, TransactionId xid);

/* ->committed manipulation */
static void SnapBuildPurgeCommittedTxn(Snapstate *snapstate);

/* snapshot building/manipulation/distribution functions */
static Snapshot SnapBuildBuildSnapshot(Snapstate *snapstate, TransactionId xid);

static void	SnapBuildFreeSnapshot(Snapshot snap);

static void SnapBuildSnapIncRefcount(Snapshot snap);

static void SnapBuildDistributeSnapshotNow(Snapstate *snapstate, ReorderBuffer *reorder, XLogRecPtr lsn);

/* on disk serialization & restore */
static bool SnapBuildRestore(Snapstate *state, XLogRecPtr lsn);
static void SnapBuildSerialize(Snapstate *state, XLogRecPtr lsn);


/*
 * Lookup a table via its current relfilenode.
 *
 * This requires that some snapshot in which that relfilenode is actually
 * visible to be set up.
 *
 * The result of this function needs to be released from the syscache.
 */
Relation
LookupRelationByRelFileNode(RelFileNode *relfilenode)
{
	HeapTuple tuple;
	Oid heaprel = InvalidOid;

	/* shared relation */
	if (relfilenode->spcNode == GLOBALTABLESPACE_OID)
	{
		heaprel = RelationMapFilenodeToOid(relfilenode->relNode, true);
	}
	else
	{
		Oid spc;

		/*
		 * relations in the default tablespace are stored with a reltablespace
		 * = InvalidOid in pg_class.
		 */
		spc = relfilenode->spcNode == DEFAULTTABLESPACE_OID ?
			InvalidOid : relfilenode->spcNode;

		tuple = SearchSysCache2(RELFILENODE,
								spc,
								relfilenode->relNode);

		/* has to be nonexistant or a nailed table */
		if (HeapTupleIsValid(tuple))
		{
			heaprel = HeapTupleHeaderGetOid(tuple->t_data);
			ReleaseSysCache(tuple);
		}
		else
		{
			heaprel = RelationMapFilenodeToOid(relfilenode->relNode, false);
		}
	}

	/* shared or nailed table */
	if (heaprel != InvalidOid)
		return RelationIdGetRelation(heaprel);
	return NULL;
}


/*
 * Allocate a new snapshot builder.
 */
Snapstate *
AllocateSnapshotBuilder(ReorderBuffer *reorder)
{
	MemoryContext context;
	Snapstate *snapstate;

	context = AllocSetContextCreate(TopMemoryContext,
									"snapshot builder context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);

	snapstate = MemoryContextAllocZero(context, sizeof(Snapstate));

	snapstate->context = context;

	snapstate->state = SNAPBUILD_START;

	snapstate->running.xcnt = 0;
	snapstate->running.xip = NULL;

	snapstate->committed.xcnt = 0;
	snapstate->committed.xcnt_space = 128; /* arbitrary number */
	snapstate->committed.xip = MemoryContextAlloc(context,
												  snapstate->committed.xcnt_space
												  * sizeof(TransactionId));
	snapstate->committed.includes_all_transactions = true;

	snapstate->transactions_after = InvalidXLogRecPtr;

	snapstate->snapshot = NULL;

	return snapstate;
}

/*
 * Freesnapshot builder.
 */
void
FreeSnapshotBuilder(Snapstate *snapstate)
{
	MemoryContext context = snapstate->context;

	if (snapstate->snapshot)
		SnapBuildFreeSnapshot(snapstate->snapshot);

	if (snapstate->committed.xip)
		pfree(snapstate->committed.xip);

	if (snapstate->running.xip)
		pfree(snapstate->running.xip);

	pfree(snapstate);

	MemoryContextDelete(context);
}

/*
 * Free an unreferenced snapshot that has previously been built by us.
 */
static void
SnapBuildFreeSnapshot(Snapshot snap)
{
	/* make sure we don't get passed an external snapshot */
	Assert(snap->satisfies == HeapTupleSatisfiesMVCCDuringDecoding);

	/* make sure nobody modified our snapshot */
	Assert(snap->curcid == FirstCommandId);
	Assert(!snap->suboverflowed);
	Assert(!snap->takenDuringRecovery);
	Assert(!snap->regd_count);

	/* slightly more likely, so its checked even without casserts */
	if (snap->copied)
		elog(ERROR, "we don't deal with copied snapshots here.");

	if (snap->active_count)
		elog(ERROR, "freeing active snapshot");

	pfree(snap);
}

/*
 * Increase refcount of a snapshot.
 *
 * This is used when handing out a snapshot to some external resource or when
 * adding a Snapshot as snapstate->snapshot.
 */
static void
SnapBuildSnapIncRefcount(Snapshot snap)
{
	snap->active_count++;
}

/*
 * Decrease refcount of a snapshot and free if the refcount reaches zero.
 *
 * Externally visible so external resources that have been handed an IncRef'ed
 * Snapshot can free it easily.
 */
void
SnapBuildSnapDecRefcount(Snapshot snap)
{
	/* make sure we don't get passed an external snapshot */
	Assert(snap->satisfies == HeapTupleSatisfiesMVCCDuringDecoding);

	/* make sure nobody modified our snapshot */
	Assert(snap->curcid == FirstCommandId);
	Assert(!snap->suboverflowed);
	Assert(!snap->takenDuringRecovery);
	Assert(!snap->regd_count);

	Assert(snap->active_count);

	/* slightly more likely, so its checked even without casserts */
	if (snap->copied)
		elog(ERROR, "we don't deal with copied snapshots here.");

	snap->active_count--;
	if (!snap->active_count)
		SnapBuildFreeSnapshot(snap);
}

/*
 * Build a new snapshot, based on currently committed, catalog modifying
 * transactions.
 *
 * In-Progress transaction with catalog access are *not* allowed to modify
 * these snapshots, they have to copy them and fill in appropriate ->curcid and
 * ->subxip/subxcnt values.
 */
static Snapshot
SnapBuildBuildSnapshot(Snapstate *snapstate, TransactionId xid)
{
	Snapshot snapshot;

	Assert(snapstate->state >= SNAPBUILD_FULL_SNAPSHOT);

	snapshot = MemoryContextAllocZero(snapstate->context,
									  sizeof(SnapshotData)
									  + sizeof(TransactionId) * snapstate->committed.xcnt
									  + sizeof(TransactionId) * 1 /* toplevel xid */);

	snapshot->satisfies = HeapTupleSatisfiesMVCCDuringDecoding;
	/*
	 * we copy all currently in progress transaction to ->xip, all
	 * transactions added to the transaction that committed during running -
	 * which thus need to be considered visible in SnapshotNow semantics - get
	 * copied to ->subxip.
	 *
	 * XXX: Do we want extra fields for those two instead?
	 */
	Assert(TransactionIdIsNormal(snapstate->xmin));
	Assert(TransactionIdIsNormal(snapstate->xmax));

	snapshot->xmin = snapstate->xmin;
	snapshot->xmax = snapstate->xmax;

	/* store all transaction to be treated as committed */
	snapshot->xip = (TransactionId *) ((char *) snapshot + sizeof(SnapshotData));
	snapshot->xcnt = snapstate->committed.xcnt;
	memcpy(snapshot->xip, snapstate->committed.xip,
		   snapstate->committed.xcnt * sizeof(TransactionId));
	/* sort so we can bsearch() */
	qsort(snapshot->xip, snapshot->xcnt, sizeof(TransactionId), xidComparator);


	snapshot->subxcnt = 0;
	snapshot->subxip = NULL;

	snapshot->suboverflowed = false;
	snapshot->takenDuringRecovery = false;
	snapshot->copied = false;
	snapshot->curcid = FirstCommandId;
	snapshot->active_count = 0;
	snapshot->regd_count = 0;

	return snapshot;
}


ResourceOwner SavedResourceOwnerDuringExport = NULL;

/*
 * Export a snapshot so it can be set in another session with SET TRANSACTION
 * SNAPSHOT.
 *
 * For that we need to start a transaction in the current backend as the
 * importing side checks whether the source transaction is still open to make
 * sure the xmin horizon hasn't advanced since then.
 *
 * After that we convert a locally built snapshot into the normal variant
 * understood by HeapTupleSatisfiesMVCC et al.
 */
const char*
SnapBuildExportSnapshot(Snapstate *snapstate)
{
	Snapshot snap;
	char *snapname;
	TransactionId xid;
	TransactionId* newxip;
	int newxcnt = 0;

	elog(LOG, "building snapshot");

	if (snapstate->state != SNAPBUILD_CONSISTENT)
		elog(ERROR, "cannot export a snapshot before reaching a consistent state");

	if (!snapstate->committed.includes_all_transactions)
		elog(ERROR, "cannot export a snapshot after, not all transactions are monitored anymore");

	/* so we don't overwrite the existing value */
	if (TransactionIdIsValid(MyPgXact->xmin))
		elog(ERROR, "cannot export a snapshot when MyPgXact->xmin already is valid");

	if (SavedResourceOwnerDuringExport)
		elog(ERROR, "can only export one snapshot at a time");

	SavedResourceOwnerDuringExport = CurrentResourceOwner;

	StartTransactionCommand();

	Assert(!FirstSnapshotSet);

	/* There doesn't seem to a nice API to set these */
	XactIsoLevel = XACT_REPEATABLE_READ;
	XactReadOnly = true;

	snap = SnapBuildBuildSnapshot(snapstate,
								  GetTopTransactionId());

	/*
	 * We know that snap->xmin is alive, enforced by the logical xmin
	 * mechanism. Due to that we can do this without locks, were only changing
	 * our own value.
	 */
	MyPgXact->xmin = snap->xmin;

	/* allocate in transaction context */
	newxip = (TransactionId*)
		palloc(sizeof(TransactionId) * GetMaxSnapshotXidCount());

	/*
	 * snapbuild.c builds transactions in an "inverted" manner, which means it
	 * stores committed transactions in ->xip, not ones in progress. Build a
	 * classical snapshot by marking all non-committed transactions as
	 * in-progress.
	 */
	for (xid = snap->xmin; NormalTransactionIdPrecedes(xid, snap->xmax);)
	{
		void *test;

		/*
		 * check whether transaction committed using the timetravel meaning of
		 * ->xip
		 */
		test = bsearch(&xid, snap->xip, snap->xcnt,
					   sizeof(TransactionId), xidComparator);

		elog(DEBUG2, "checking xid %u.. %d (xmin %u, xmax %u)", xid, test == NULL,
			 snap->xmin, snap->xmax);

		if (test == NULL)
		{
			if (newxcnt == GetMaxSnapshotXidCount())
				elog(ERROR, "too large snapshot");

			newxip[newxcnt++] = xid;

			elog(DEBUG2, "treat %u as in-progress", xid);
		}
		TransactionIdAdvance(xid);
	}
	snap->xcnt = newxcnt;
	snap->xip = newxip;

	snapname = ExportSnapshot(snap);

	elog(LOG, "exported snapbuild snapshot: %s xcnt %u", snapname, snap->xcnt);

	return snapname;
}

/*
 * Reset a previously SnapBuildExportSnapshot'ed snapshot if there is
 * any. Aborts the previously started transaction and resets the resource owner
 * back to the previous value.
 */
void
SnapBuildClearExportedSnapshot()
{
	/* nothing exported, thats the usual case*/
	if (SavedResourceOwnerDuringExport == NULL)
		return;

	AbortCurrentTransaction();

	CurrentResourceOwner = SavedResourceOwnerDuringExport;
	SavedResourceOwnerDuringExport = NULL;
}

/*
 * Handle the effects of a single heap change, appropriate to the current state
 * of the snapshot builder.
 */
static SnapBuildAction
SnapBuildProcessChange(ReorderBuffer *reorder, Snapstate *snapstate,
					   TransactionId xid, XLogRecordBuffer *buf,
					   RelFileNode *relfilenode)
{
	SnapBuildAction ret = SNAPBUILD_SKIP;

	/*
	 * We can't handle data in transactions if we haven't built a snapshot yet,
	 * so don't store them.
	 */
	if (snapstate->state < SNAPBUILD_FULL_SNAPSHOT)
		;
	/*
	 * No point in keeping track of changes in transactions that we don't have
	 * enough information about to decode.
	 */
	else if (snapstate->state < SNAPBUILD_CONSISTENT &&
			 SnapBuildTxnIsRunning(snapstate, xid))
		;
	else
	{
		bool old_tx = ReorderBufferIsXidKnown(reorder, xid);

		ret = SNAPBUILD_DECODE;

		if (!old_tx || !ReorderBufferXidHasBaseSnapshot(reorder, xid))
		{
			if (!snapstate->snapshot) {
				snapstate->snapshot = SnapBuildBuildSnapshot(snapstate, xid);
				/* refcount of the snapshot builder */
				SnapBuildSnapIncRefcount(snapstate->snapshot);
			}

			/* refcount of the transaction */
			SnapBuildSnapIncRefcount(snapstate->snapshot);
			ReorderBufferSetBaseSnapshot(reorder, xid, buf->origptr, snapstate->snapshot);
		}
	}

	return ret;
}

/*
 * Process a single xlog record.
 */
SnapBuildAction
SnapBuildDecodeCallback(ReorderBuffer *reorder, Snapstate *snapstate,
						XLogRecordBuffer *buf)
{
	XLogRecord *r = &buf->record;
	uint8 info = r->xl_info & ~XLR_INFO_MASK;
	TransactionId xid = buf->record.xl_xid;

	SnapBuildAction ret = SNAPBUILD_SKIP;

#if DEBUG_ME_LOUDLY
	{
		StringInfoData s;

		initStringInfo(&s);
		RmgrTable[r->xl_rmid].rm_desc(&s,
									  r->xl_info,
									  buf->record_data);

		/* don't bother emitting empty description */
		if (s.len > 0)
			elog(LOG, "xlog redo %u: %s", xid, s.data);
	}
#endif

	/*
	 * Only search for an initial starting point if we haven't build a full
	 * snapshot yet
	 */
	if (snapstate->state < SNAPBUILD_CONSISTENT)
	{
		/*
		 * Build snapshot incrementally using information about the currently
		 * running transactions. As soon as all of those have finished
		 */
		if (r->xl_rmid == RM_STANDBY_ID && info == XLOG_RUNNING_XACTS)
		{
			xl_running_xacts *running = (xl_running_xacts *) buf->record_data;


			if (TransactionIdIsNormal(snapstate->initial_xmin_horizon) &&
				NormalTransactionIdPrecedes(running->oldestRunningXid, snapstate->initial_xmin_horizon))
			{
				elog(LOG, "skipping snapshot at %X/%X due to initial xmin horizon %u vs snap %u",
					 (uint32)(buf->origptr >> 32), (uint32)buf->origptr,
					 snapstate->initial_xmin_horizon, running->oldestRunningXid);
			}
			/* no transaction running, jump to consistent */
			else if (running->xcnt == 0)
			{
				/*
				 * might have already started to incrementally assemble
				 * transactions.
				 */
				if (snapstate->transactions_after == InvalidXLogRecPtr ||
					snapstate->transactions_after < buf->origptr)
				snapstate->transactions_after = buf->origptr;

				snapstate->xmin = running->oldestRunningXid;
				snapstate->xmax = running->latestCompletedXid;
				TransactionIdAdvance(snapstate->xmax);

				Assert(TransactionIdIsNormal(snapstate->xmin));
				Assert(TransactionIdIsNormal(snapstate->xmax));

				snapstate->running.xcnt = 0;
				snapstate->running.xmin = InvalidTransactionId;
				snapstate->running.xmax = InvalidTransactionId;

				/*
				 * FIXME: abort everything we have stored about running
				 * transactions, relevant e.g. after a crash.
				 */
				snapstate->state = SNAPBUILD_CONSISTENT;

				elog(LOG, "found initial snapshot (xmin %u) due to running xacts with xcnt == 0",
					 snapstate->xmin);
				return SNAPBUILD_DECODE;
			}
			/* valid on disk state */
			else if (SnapBuildRestore(snapstate, buf->origptr))
			{
				Assert(snapstate->state == SNAPBUILD_CONSISTENT);
				return SNAPBUILD_DECODE;
			}
			/* first encounter of a xl_running_xacts record */
			else if (!snapstate->running.xcnt)
			{
				/*
				 * We only care about toplevel xids as those are the ones we
				 * definitely see in the wal stream. As snapbuild.c tracks
				 * committed instead of running transactions we don't need to
				 * know anything about uncommitted subtransactions.
				 */
				snapstate->xmin = running->oldestRunningXid;
				snapstate->xmax = running->latestCompletedXid;
				TransactionIdAdvance(snapstate->xmax);

				Assert(TransactionIdIsNormal(snapstate->xmin));
				Assert(TransactionIdIsNormal(snapstate->xmax));

				snapstate->running.xcnt = running->xcnt;
				snapstate->running.xcnt_space = running->xcnt;

				snapstate->running.xip =
					MemoryContextAlloc(snapstate->context,
									   snapstate->running.xcnt * sizeof(TransactionId));

				memcpy(snapstate->running.xip, running->xids,
					   snapstate->running.xcnt * sizeof(TransactionId));

				/* sort so we can do a binary search */
				qsort(snapstate->running.xip, snapstate->running.xcnt,
					  sizeof(TransactionId), xidComparator);

				snapstate->running.xmin = snapstate->running.xip[0];
				snapstate->running.xmax = snapstate->running.xip[running->xcnt - 1];

				/* makes comparisons cheaper later */
				TransactionIdRetreat(snapstate->running.xmin);
				TransactionIdAdvance(snapstate->running.xmax);

				snapstate->state = SNAPBUILD_FULL_SNAPSHOT;

				elog(LOG, "found initial snapshot (xmin %u) due to running xacts, %u xacts need to finish",
					 snapstate->xmin, (uint32)snapstate->running.xcnt);
			}
		}
	}

	if (snapstate->state == SNAPBUILD_START)
		return SNAPBUILD_SKIP;

	/*
	 * This switch is - partially due to PGs indentation rules - rather deep
	 * and large. Maybe break it into separate functions?
	 */
	switch (r->xl_rmid)
	{
		case RM_XLOG_ID:
			{
				switch (info)
				{
					case XLOG_CHECKPOINT_SHUTDOWN:
#ifdef NOT_YET
						{
							/*
							 * FIXME: abort everything but prepared xacts, we
							 * don't track prepared xacts though so far.  It
							 * might be neccesary to do this to handle subtxn
							 * ids that haven't been assigned to a toplevel xid
							 * after a crash.
							 */
							for ( /* FIXME */ )
							{
							}
						}
#endif
					case XLOG_CHECKPOINT_ONLINE:
						{
							/*
							 * FIXME: dump state to disk so we can restart
							 * from here later
							 */
							break;
						}
				}
				break;
			}
		case RM_STANDBY_ID:
			{
				switch (info)
				{
					case XLOG_RUNNING_XACTS:
						{
							xl_running_xacts *running =
								(xl_running_xacts *) buf->record_data;

							if (snapstate->state == SNAPBUILD_CONSISTENT)
								SnapBuildSerialize(snapstate, buf->origptr);

							/*
							 * update range of interesting xids. We don't
							 * increase ->xmax because once we are in a
							 * consistent state we can do that ourselves and
							 * much more efficiently so because we only need to
							 * do it for catalog transactions.
							 */
							snapstate->xmin = running->oldestRunningXid;


							/*
							 * xmax can be lower than xmin here because we only
							 * increase xmax when we hit a transaction with
							 * catalog changes. While odd looking, its correct
							 * and actually more efficient this way.
							 */


							/*
							 * Remove transactions we don't need to keep track
							 * off anymore.
							 */
							 SnapBuildPurgeCommittedTxn(snapstate);


							 elog(DEBUG1, "xmin: %u, xmax: %u, oldestrunning: %u",
								  snapstate->xmin, snapstate->xmax,
								  running->oldestRunningXid);

							 /*
							  * inrease shared memory state, so vacuum can work
							  * on tuples we prevent from being purged.
							  */
							 IncreaseLogicalXminForSlot(buf->origptr,
														running->oldestRunningXid);

							 break;
						}
					case XLOG_STANDBY_LOCK:
						break;
				}
				break;
			}
		case RM_XACT_ID:
			{
				switch (info)
				{
					case XLOG_XACT_COMMIT:
						{
							xl_xact_commit *xlrec =
								(xl_xact_commit *) buf->record_data;

							ret = SNAPBUILD_DECODE;

							/*
							 * Queue cache invalidation messages.
							 */
							if (xlrec->nmsgs)
							{
								TransactionId *subxacts;
								SharedInvalidationMessage *inval_msgs;

								/* subxid array follows relfilenodes */
								subxacts = (TransactionId *)
									&(xlrec->xnodes[xlrec->nrels]);
								/* invalidation messages follow subxids */
								inval_msgs = (SharedInvalidationMessage *)
									&(subxacts[xlrec->nsubxacts]);

								/*
								 * no need to check
								 * XactCompletionRelcacheInitFileInval, we will
								 * process the sinval messages that the
								 * relmapper change has generated.
								 */
								ReorderBufferAddInvalidations(reorder, xid,
														   InvalidXLogRecPtr,
								                           xlrec->nmsgs,
														   inval_msgs);

								/*
								 * Let everyone know that this transaction
								 * modified the catalog. We need this at commit
								 * time.
								 */
								ReorderBufferXidSetTimetravel(reorder, xid, buf->origptr);

							}

							SnapBuildCommitTxn(snapstate, reorder,
											   buf->origptr, xid,
											   xlrec->nsubxacts,
											   (TransactionId *) &xlrec->xnodes);
							break;
						}
					case XLOG_XACT_COMMIT_COMPACT:
						{
							xl_xact_commit_compact *xlrec =
								(xl_xact_commit_compact *) buf->record_data;

							ret = SNAPBUILD_DECODE;

							SnapBuildCommitTxn(snapstate, reorder,
											   buf->origptr, xid,
											   xlrec->nsubxacts,
											   xlrec->subxacts);
							break;
						}
					case XLOG_XACT_COMMIT_PREPARED:
						{
							xl_xact_commit_prepared *xlrec =
								(xl_xact_commit_prepared *) buf->record_data;

							/* FIXME: check for invalidation messages! */

							SnapBuildCommitTxn(snapstate, reorder,
											   buf->origptr, xlrec->xid,
											   xlrec->crec.nsubxacts,
											   (TransactionId *) &xlrec->crec.xnodes);

							ret = SNAPBUILD_DECODE;
							break;
						}
					case XLOG_XACT_ABORT:
						{
							xl_xact_abort *xlrec =
								(xl_xact_abort *) buf->record_data;

							SnapBuildAbortTxn(snapstate, xid, xlrec->nsubxacts,
											  (TransactionId *) &(xlrec->xnodes[xlrec->nrels]));
							ret = SNAPBUILD_DECODE;
							break;
						}
					case XLOG_XACT_ABORT_PREPARED:
						{
							xl_xact_abort_prepared *xlrec =
								(xl_xact_abort_prepared *) buf->record_data;
							xl_xact_abort *arec = &xlrec->arec;

							SnapBuildAbortTxn(snapstate, xlrec->xid,
											  arec->nsubxacts,
											  (TransactionId *) &(arec->xnodes[arec->nrels]));
							ret = SNAPBUILD_DECODE;
							break;
						}
					case XLOG_XACT_ASSIGNMENT:
						break;
					case XLOG_XACT_PREPARE:
						/*
						 * XXX: We could take note of all in-progress prepared
						 * xacts so we can use shutdown checkpoints to abort
						 * in-progress transactions...
						 */
					default:
						break;
				}
				break;
			}
		case RM_HEAP_ID:
			{
				switch (info & XLOG_HEAP_OPMASK)
				{
					case XLOG_HEAP_INPLACE:
						{
							xl_heap_inplace *xlrec =
								(xl_heap_inplace *) buf->record_data;

							ret = SnapBuildProcessChange(reorder, snapstate,
														 xid, buf,
														 &xlrec->target.node);

							/*
							 * inplace records happen in catalog modifying
							 * txn's
							 */
							ReorderBufferXidSetTimetravel(reorder, xid, buf->origptr);

							break;
						}
					/*
					 * we only ever read changes, so row level locks
					 * aren't interesting
					 */
					case XLOG_HEAP_LOCK:
						break;

					case XLOG_HEAP_INSERT:
						{
							xl_heap_insert *xlrec =
								(xl_heap_insert *) buf->record_data;

							ret = SnapBuildProcessChange(reorder, snapstate,
														 xid, buf,
														 &xlrec->target.node);
							break;
						}
					/* HEAP(_HOT)?_UPDATE use the same data layout */
					case XLOG_HEAP_UPDATE:
					case XLOG_HEAP_HOT_UPDATE:
						{
							xl_heap_update *xlrec =
								(xl_heap_update *) buf->record_data;

							ret = SnapBuildProcessChange(reorder, snapstate,
														 xid, buf,
														 &xlrec->target.node);
							break;
						}
					case XLOG_HEAP_DELETE:
						{
							xl_heap_delete *xlrec =
								(xl_heap_delete *) buf->record_data;

							ret = SnapBuildProcessChange(reorder, snapstate,
														 xid, buf,
														 &xlrec->target.node);
							break;
						}
					default:
						break;
				}
				break;
			}
		case RM_HEAP2_ID:
			{
				switch (info)
				{
					case XLOG_HEAP2_MULTI_INSERT:
						{
							xl_heap_multi_insert *xlrec =
								(xl_heap_multi_insert *) buf->record_data;

							ret = SnapBuildProcessChange(reorder, snapstate, xid,
														 buf, &xlrec->node);
							break;
						}
					case XLOG_HEAP2_NEW_CID:
						{
							CommandId cid;

							xl_heap_new_cid *xlrec =
								(xl_heap_new_cid *) buf->record_data;
#if 0
							elog(WARNING, "found new cid in xid %u: relfilenode %u/%u/%u: tid: (%u, %u) cmin: %u, cmax: %u, combo: %u",
								 xlrec->top_xid,
								 xlrec->target.node.dbNode, xlrec->target.node.spcNode,	xlrec->target.node.relNode,
								 BlockIdGetBlockNumber(&xlrec->target.tid.ip_blkid), xlrec->target.tid.ip_posid,
								 xlrec->cmin, xlrec->cmax, xlrec->combocid);
#endif
							/* we only log new_cid's if a catalog tuple was modified */
							ReorderBufferXidSetTimetravel(reorder, xid, buf->origptr);

							ReorderBufferAddNewTupleCids(reorder, xlrec->top_xid, buf->origptr,
													  xlrec->target.node, xlrec->target.tid,
													  xlrec->cmin, xlrec->cmax, xlrec->combocid);

							/* figure out new command id */
							if (xlrec->cmin != InvalidCommandId && xlrec->cmax != InvalidCommandId)
								cid = Max(xlrec->cmin, xlrec->cmax);
							else if (xlrec->cmax != InvalidCommandId)
								cid = xlrec->cmax;
							else if (xlrec->cmin != InvalidCommandId)
								cid = xlrec->cmin;
							else
							{
								cid = InvalidCommandId; /* silence compiler */
								elog(ERROR, "broken arrow, no cid?");
							}
							/*
							 * FIXME: potential race condition here: if
							 * multiple snapshots were running & generating
							 * changes in the same transaction on the source
							 * side this could be problematic.  But this cannot
							 * happen for system catalogs, right?
							 */
							ReorderBufferAddNewCommandId(reorder, xid, buf->origptr,
													  cid + 1);
							break;
						}
					default:
						break;
				}
			}
			break;
	}

	return ret;
}


/*
 * check whether `xid` is currently running
 */
static bool
SnapBuildTxnIsRunning(Snapstate *snapstate, TransactionId xid)
{
	Assert(snapstate->state < SNAPBUILD_CONSISTENT);
	Assert(TransactionIdIsValid(snapstate->running.xmin));
	Assert(TransactionIdIsValid(snapstate->running.xmax));

	if (snapstate->running.xcnt &&
		NormalTransactionIdFollows(xid, snapstate->running.xmin) &&
		NormalTransactionIdPrecedes(xid, snapstate->running.xmax))
	{
		TransactionId *search =
			bsearch(&xid, snapstate->running.xip, snapstate->running.xcnt_space,
					sizeof(TransactionId), xidComparator);

		if (search != NULL)
		{
			Assert(*search == xid);
			return true;
		}
	}

	return false;
}

/*
 * Add a new SnapshotNow to all transactions we're decoding that currently are
 * in-progress so they can see new catalog contents made by the transaction
 * that just committed.
 */
static void
SnapBuildDistributeSnapshotNow(Snapstate *snapstate, ReorderBuffer *reorder, XLogRecPtr lsn)
{
	dlist_iter txn_i;
	ReorderBufferTXN *txn;

	dlist_foreach(txn_i, &reorder->toplevel_by_lsn)
	{
		txn = dlist_container(ReorderBufferTXN, node, txn_i.cur);

		/*
		 * If we don't have a base snapshot yet, there are no changes yet which
		 * in turn implies we don't yet need a new snapshot.
		 */
		if (ReorderBufferXidHasBaseSnapshot(reorder, txn->xid))
		{
			elog(DEBUG2, "adding a new snapshot to %u at %X/%X", txn->xid,
			     (uint32)(lsn >> 32), (uint32)lsn);
			SnapBuildSnapIncRefcount(snapstate->snapshot);
			ReorderBufferAddSnapshot(reorder, txn->xid, lsn, snapstate->snapshot);
		}
	}
}

/*
 * Keep track of a new catalog changing transaction that has committed
 */
static void
SnapBuildAddCommittedTxn(Snapstate *snapstate, TransactionId xid)
{
	Assert(TransactionIdIsValid(xid));

	if (snapstate->committed.xcnt == snapstate->committed.xcnt_space)
	{
		snapstate->committed.xcnt_space = snapstate->committed.xcnt_space * 2 + 1;

		elog(WARNING, "increasing space for committed transactions to %zu",
			 snapstate->committed.xcnt_space);

		snapstate->committed.xip = repalloc(snapstate->committed.xip,
											snapstate->committed.xcnt_space * sizeof(TransactionId));
	}
	snapstate->committed.xip[snapstate->committed.xcnt++] = xid;
}

/*
 * Remove all transactions we treat as committed that are smaller than
 * ->xmin. Those won't ever get checked via the ->commited array anyway.
 */
static void
SnapBuildPurgeCommittedTxn(Snapstate *snapstate)
{
	int off;
	TransactionId *workspace;
	int surviving_xids = 0;

	/* not ready yet */
	if (!TransactionIdIsNormal(snapstate->xmin))
		return;

	/* XXX: Neater algorithm? */
	workspace =
		MemoryContextAlloc(snapstate->context,
						   snapstate->committed.xcnt * sizeof(TransactionId));


	for (off = 0; off < snapstate->committed.xcnt; off++)
	{
		if (NormalTransactionIdPrecedes(snapstate->committed.xip[off], snapstate->xmin))
			;/* remove */
		else
			workspace[surviving_xids++] = snapstate->committed.xip[off];
	}

	memcpy(snapstate->committed.xip, workspace,
		   surviving_xids * sizeof(TransactionId));

	elog(DEBUG1, "purged committed transactions from %u to %u, xmin: %u, xmax: %u",
		 (uint32)snapstate->committed.xcnt, (uint32)surviving_xids,
		 snapstate->xmin, snapstate->xmax);
	snapstate->committed.xcnt = surviving_xids;

	pfree(workspace);
}

/*
 * Common logic for SnapBuildAbortTxn and SnapBuildCommitTxn dealing with
 * keeping track of the amount of running transactions.
 */
static void
SnapBuildEndTxn(Snapstate *snapstate, TransactionId xid)
{
	if (snapstate->state == SNAPBUILD_CONSISTENT)
		return;

	if (SnapBuildTxnIsRunning(snapstate, xid))
	{
		if (!--snapstate->running.xcnt)
		{
			/*
			 * none of the originally running transaction is running
			 * anymore. Due to that our incrementaly built snapshot now is
			 * complete.
			 */
			elog(LOG, "found consistent point due to SnapBuildEndTxn + running: %u", xid);
			snapstate->state = SNAPBUILD_CONSISTENT;
		}
	}
}

/* Abort a transaction, throw away all state we kept */
static void
SnapBuildAbortTxn(Snapstate *snapstate, TransactionId xid, int nsubxacts, TransactionId *subxacts)
{
	int i;

	for (i = 0; i < nsubxacts; i++)
	{
		TransactionId subxid = subxacts[i];
		SnapBuildEndTxn(snapstate, subxid);
	}

	SnapBuildEndTxn(snapstate, xid);
}

/* Handle everything that needs to be done when a transaction commits */
static void
SnapBuildCommitTxn(Snapstate *snapstate, ReorderBuffer *reorder,
				   XLogRecPtr lsn, TransactionId xid,
				   int nsubxacts, TransactionId *subxacts)
{
	int nxact;

	bool forced_timetravel = false;
	bool sub_does_timetravel = false;
	bool top_does_timetravel = false;

	TransactionId xmax = xid;

	/*
	 * If we couldn't observe every change of a transaction because it was
	 * already running at the point we started to observe we have to assume it
	 * made catalog changes.
	 *
	 * This has the positive benefit that we afterwards have enough information
	 * to build an exportable snapshot thats usable by pg_dump et al.
	 */
	if (snapstate->state < SNAPBUILD_CONSISTENT)
	{
		/* ensure that only commits after this are getting replayed */
		if (snapstate->transactions_after < lsn)
			snapstate->transactions_after = lsn;

		/*
		 * we could avoid treating !SnapBuildTxnIsRunning transactions as
		 * timetravel ones, but we want to be able to export a snapshot when we
		 * reached consistency.
		 */
		forced_timetravel = true;
		elog(DEBUG1, "forced to assume catalog changes for xid %u because it was running to early", xid);
	}

	for (nxact = 0; nxact < nsubxacts; nxact++)
	{
		TransactionId subxid = subxacts[nxact];

		/*
		 * make sure txn is not tracked in running txn's anymore, switch
		 * state
		 */
		SnapBuildEndTxn(snapstate, subxid);

		/*
		 * If we're forcing timetravel we also need accurate subtransaction
		 * status.
		 */
		if (forced_timetravel)
		{
			SnapBuildAddCommittedTxn(snapstate, subxid);
			if (NormalTransactionIdFollows(subxid, xmax))
				xmax = subxid;
		}
		/*
		 * add subtransaction to base snapshot, we don't distinguish to
		 * toplevel transactions there.
		 */
		else if (ReorderBufferXidDoesTimetravel(reorder, subxid))
		{
			sub_does_timetravel = true;

			elog(DEBUG1, "found subtransaction %u:%u with catalog changes.",
				 xid, subxid);

			SnapBuildAddCommittedTxn(snapstate, subxid);

			if (NormalTransactionIdFollows(subxid, xmax))
				xmax = subxid;
		}
	}

	/*
	 * make sure txn is not tracked in running txn's anymore, switch state
	 */
	SnapBuildEndTxn(snapstate, xid);

	if (forced_timetravel)
	{
		elog(DEBUG1, "forced transaction %u to do timetravel.", xid);

		SnapBuildAddCommittedTxn(snapstate, xid);
	}
	/* add toplevel transaction to base snapshot */
	else if (ReorderBufferXidDoesTimetravel(reorder, xid))
	{
		elog(DEBUG1, "found top level transaction %u, with catalog changes!", xid);

		top_does_timetravel = true;
		SnapBuildAddCommittedTxn(snapstate, xid);
	}
	else if (sub_does_timetravel)
	{
		/* mark toplevel txn as timetravel as well */
		SnapBuildAddCommittedTxn(snapstate, xid);
	}

	if (forced_timetravel || top_does_timetravel || sub_does_timetravel)
	{
		if (!TransactionIdIsValid(snapstate->xmax) ||
			TransactionIdFollowsOrEquals(xmax, snapstate->xmax))
		{
			snapstate->xmax = xmax;
			TransactionIdAdvance(snapstate->xmax);
		}

		if (snapstate->state < SNAPBUILD_FULL_SNAPSHOT)
			return;

		/* refcount of the transaction */
		if (snapstate->snapshot)
			SnapBuildSnapDecRefcount(snapstate->snapshot);

		snapstate->snapshot = SnapBuildBuildSnapshot(snapstate, xid);

		/* refcount of the snapshot builder */
		SnapBuildSnapIncRefcount(snapstate->snapshot);

		/* add a new SnapshotNow to all currently running transactions */
		SnapBuildDistributeSnapshotNow(snapstate, reorder, lsn);
	}
	else
	{
		/* record that we cannot export a general snapshot anymore */
		snapstate->committed.includes_all_transactions = false;
	}
}


#define SNAPSTATE_MAGIC 0x51A1E001

/*
 * We store snapstates on disk in the following manner:
 *
 * struct Snapstate;
 * TransactionId * running.xcnt_space;
 * TransactionId * committed.xcnt; (*not xcnt_space*)
 *
 */
typedef struct SnapstateOnDisk
{
	uint32 magic;
	Size size;
	Snapstate state;
	/* variable amount of TransactionId's */
} SnapstateOnDisk;

/*
 * Serialize the snapshot represented by 'state' at the location 'lsn' if it
 * hasn't already been done by another decoding process.
 */
static void
SnapBuildSerialize(Snapstate *state, XLogRecPtr lsn)
{
	Size needed_size =
		sizeof(SnapstateOnDisk) +
		sizeof(TransactionId) * state->running.xcnt_space +
		sizeof(TransactionId) * state->committed.xcnt;

	SnapstateOnDisk *ondisk;
	char *ondisk_c;
	int fd;
	char tmppath[MAXPGPATH];
	char path[MAXPGPATH];
	int ret;
	struct stat stat_buf;

	/*
	 * FIXME: Timeline handling
	 */

	/*
	 * first check whether some other backend already has written the snapshot
	 * for this LSN
	 */
	sprintf(path, "pg_llog/snapshots/%X-%X.snap",
	        (uint32)(lsn >> 32), (uint32)lsn);

	ret = stat(path, &stat_buf);

	if (ret != 0 && errno != ENOENT)
		ereport(ERROR, (errmsg("could not stat snapbuild state file %s", path)));
	else if (ret == 0)
		return;

	elog(LOG, "serializing snapshot to %s", path);

	sprintf(tmppath, "pg_llog/snapshots/%X-%X.snap.%u.tmp",
	        (uint32)(lsn >> 32), (uint32)lsn, getpid());

	if (unlink(tmppath) != 0 && errno != ENOENT)
		ereport(ERROR, (errmsg("could not unlink old file %s", path)));

	ondisk = MemoryContextAllocZero(state->context, needed_size);
	ondisk_c = ((char *) ondisk) + sizeof(SnapstateOnDisk);
	ondisk->magic = SNAPSTATE_MAGIC;
	ondisk->size = needed_size;

	/* copy state */
	ondisk->state = *state;

	/* NULL-ify memory-only data */
	ondisk->state.context = NULL;
	ondisk->state.snapshot = NULL;

	/* copy running xacts */
	memcpy(ondisk_c, state->running.xip,
	       sizeof(TransactionId) * state->running.xcnt_space);
	ondisk_c += sizeof(TransactionId) * state->running.xcnt_space;

	/* copy  committed xacts */
	memcpy(ondisk_c, state->committed.xip,
	       sizeof(TransactionId) * state->committed.xcnt);
	ondisk_c += sizeof(TransactionId) * state->committed.xcnt;

	/* we have valid data now, open tempfile and write it there */
	/* FIXME: locking! */

	fd = OpenTransientFile(tmppath,
						   O_CREAT | O_EXCL | O_WRONLY | PG_BINARY,
						   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR, (errmsg("could not open snapbuild state file %s for writing: %m",  path)));

	if ((write(fd, ondisk, needed_size)) != needed_size)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to snapbuild state file \"%s\": %m",
						tmppath)));
	}

	/*
	 * XXX: Do the fsync() via checkpoints/restartpoints
	 */
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync snapbuild state file \"%s\": %m",
						tmppath)));
	}

	CloseTransientFile(fd);

	/*
	 * We may overwrite the work from some other backend, but that's ok, our
	 * snapshot is valid as well.
	 */
	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename snapbuild state file from \"%s\" to \"%s\": %m",
						tmppath, path)));
	}
	/* make sure we persist */
	fsync_fname(path, false);
	fsync_fname("pg_llog/snapshots", true);
}

/*
 * Restore a snapshot into 'state' if previously one has been stored at the
 * location indicated by 'lsn'. Returns true if successfull, false otherwise.
 */
static bool
SnapBuildRestore(Snapstate *state, XLogRecPtr lsn)
{
	SnapstateOnDisk ondisk;
	int fd;
	char path[MAXPGPATH];
	Size sz;

	sprintf(path, "pg_llog/snapshots/%X-%X.snap",
	        (uint32)(lsn >> 32), (uint32)lsn);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);

	elog(LOG, "restoring snapbuild state from %s", path);

	if (fd < 0 && errno == ENOENT)
		return false;
	else if (fd < 0)
		ereport(ERROR, (errmsg("could not open snapbuild state file %s", path)));

	elog(LOG, "really restoring from %s", path);

	/* read statically sized portion of snapshot */
	if (read(fd, &ondisk, sizeof(ondisk)) != sizeof(ondisk))
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read snapbuild file \"%s\": %m",
						path)));
	}

	if (ondisk.magic != SNAPSTATE_MAGIC)
		ereport(ERROR, (errmsg("snapbuild state file has wrong magic %u instead of %u",
							   ondisk.magic, SNAPSTATE_MAGIC)));

	/* restore running xact information */
	sz = sizeof(TransactionId) * ondisk.state.running.xcnt_space;
	ondisk.state.running.xip = MemoryContextAlloc(state->context, sz);
	if (read(fd, ondisk.state.running.xip, sz) != sz)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read running xacts from snapbuild file \"%s\": %m",
						path)));
	}

	/* restore running xact information */
	sz = sizeof(TransactionId) * ondisk.state.committed.xcnt;
	ondisk.state.committed.xip = MemoryContextAlloc(state->context, sz);
	if (read(fd, ondisk.state.committed.xip, sz) != sz)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read committed xacts from snapbuild file \"%s\": %m",
						path)));
	}

	CloseTransientFile(fd);

	/*
	 * ok, we now have a sensible snapshot here, figure out if it has more
	 * information than we have.
	 *
	 * We are only interested in consistent snapshots for now, comparing
	 * whether one imcomplete snapshot is more "advanced" seems to be
	 * unnecessarily complex.
	 */
	if (ondisk.state.state < SNAPBUILD_CONSISTENT)
		goto snapshot_not_interesting;

	/*
	 * Don't use a snapshot that requires an xmin that we cannot guarantee to
	 * be available.
	 */
	if (TransactionIdPrecedes(ondisk.state.xmin, state->initial_xmin_horizon))
		goto snapshot_not_interesting;

	/* XXX: transactions_after needs to be updated differently, to be checked here */

	/* ok, we think the snapshot is sensible, copy over everything important */
	state->xmin = ondisk.state.xmin;
	state->xmax = ondisk.state.xmax;
	state->state = ondisk.state.state;

	state->committed.xcnt = ondisk.state.committed.xcnt;
	/* We only allocated/stored xcnt, not xcnt_space xids !*/
	/* don't overwrite preallocated xip, if we don't have anything here */
	if (state->committed.xcnt > 0)
	{
		pfree(state->committed.xip);
		state->committed.xcnt_space = ondisk.state.committed.xcnt;
		state->committed.xip = ondisk.state.committed.xip;
	}
	ondisk.state.committed.xip = NULL;

	state->running.xcnt = ondisk.state.committed.xcnt;
	if (state->running.xip)
		pfree(state->running.xip);
	state->running.xcnt_space = ondisk.state.committed.xcnt_space;
	state->running.xip = ondisk.state.running.xip;

	/*  our snapshot is not interesting anymore, build a new one */
	if (state->snapshot != NULL)
	{
		SnapBuildSnapDecRefcount(state->snapshot);
	}
	state->snapshot = SnapBuildBuildSnapshot(state, InvalidTransactionId);
	SnapBuildSnapIncRefcount(state->snapshot);

	return true;

snapshot_not_interesting:
	if (ondisk.state.running.xip != NULL)
		pfree(ondisk.state.running.xip);
	if (ondisk.state.committed.xip != NULL)
		pfree(ondisk.state.committed.xip);
	return false;
}
