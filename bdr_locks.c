/* -------------------------------------------------------------------------
 *
 * bdr_locks.c
 *		global ddl/dml interlocking locks
 *
 *
 * Copyright (C) 2014-2015, PostgreSQL Global Development Group
 *
 * NOTES
 *
 *    A relatively simple distributed DDL locking implementation:
 *
 *    Locks are acquired on a database granularity and can only be held by a
 *    single node. That choice was made to reduce both, the complexity of the
 *    implementation, and to reduce the likelihood of inter node deadlocks.
 *
 *    Because DDL locks have to acquired inside transactions the inter node
 *    communication can't be done via a queue table streamed out via logical
 *    decoding - other nodes would only see the result once the the
 *    transaction commits... Instead the 'messaging' feature is used which
 *    allows to inject transactional and nontransactional messages in the
 *    changestream.
 *
 *    There are really two levels of DDL lock - the global lock that only
 *    one node can hold, and individual local DDL locks on each node. If
 *    a node holds the global DDL lock then it owns the local DDL locks on each
 *    node.
 *
 *    DDL lock acquiration basically works like this:
 *
 *    1) A utility command notices that it needs the global ddl lock and the local
 *       node doesn't already hold it. If there already is a local ddl lock
 *       it'll ERROR out, as this indicates another node already holds or is
 *       trying to acquire the global DDL lock.
 *
 *	  2) It sends out a 'acquire_lock' message to all other nodes.
 *
 *    3) When another node receives a 'acquire_lock' message it checks whether
 *       the local ddl lock is already held. If so it'll send a 'decline_lock'
 *       message back causing the lock acquiration to fail.
 *
 *    4) If a 'acquire_lock' message is received and the local DDL lock is not
 *       held it'll be acquired and an entry into the 'bdr_global_locks' table
 *       will be made marking the lock to be in the 'catchup' phase.
 *
 *    5) All concurrent user transactions will be cancelled.
 *
 *	  6) A 'request_replay_confirm' message will be sent to all other nodes
 *	     containing a lsn that has to be replayed.
 *
 *    7) When a 'request_replay_confirm' message is received, a
 *       'replay_confirm' message will be sent back.
 *
 *    8) Once all other nodes have replied with 'replay_confirm' the DDL lock
 *       has been successfully acquired on the node reading the 'acquire_lock'
 *       message (from 3)). The corresponding bdr_global_locks entry will be
 *       updated to the 'acquired' state and a 'confirm_lock' message will be sent out.
 *
 *    9) Once all nodes have replied with 'confirm_lock' messages the ddl lock
 *       has been acquired.
 *
 *    There's some additional complications to handle crash safety:
 *
 *    Everytime a node crashes it sends out a 'startup' message causing all
 *    other nodes to release locks held by it before the crash.
 *    Then the bdr_global_locks table is read. All existing locks are
 *    acquired. If a lock still is in 'catchup' phase the lock acquiration
 *    process is re-started at step 6)
 *
 * IDENTIFICATION
 *		bdr_locks.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"

#include "bdr_locks.h"

#ifdef BUILDING_BDR

#include "miscadmin.h"

#include "access/xact.h"
#include "access/xlog.h"

#include "commands/dbcommands.h"
#include "catalog/indexing.h"

#include "executor/executor.h"

#include "libpq/pqformat.h"

#include "replication/slot.h"

#include "storage/barrier.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/standby.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

typedef struct BdrLocksDBState {
	/* db slot used */
	bool		in_use;

	/* db this slot is reserved for */
	Oid			dboid;

	/* number of nodes we're connected to */
	Size		nnodes;

	/* has startup progressed far enough to allow writes? */
	bool		locked_and_loaded;

	int			lockcount;
	RepNodeId	lock_holder;

	/* progress of lock acquiration */
	int			acquire_confirmed;
	int			acquire_declined;

	/* progress of replay confirmation */
	int			replay_confirmed;
	XLogRecPtr	replay_confirmed_lsn;

	Latch	   *waiting_latch;
} BdrLocksDBState;

typedef struct BdrLocksCtl {
	LWLock	   *lock;
	BdrLocksDBState dbstate[FLEXIBLE_ARRAY_MEMBER];
} BdrLocksCtl;

static BdrLocksDBState * bdr_locks_find_database(Oid dbid, bool create);
static void bdr_locks_find_my_database(bool create);
static void bdr_prepare_message(StringInfo s, BdrMessageType message_type);

static BdrLocksCtl *bdr_locks_ctl;

/* shmem init hook to chain to on startup, if any */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* this database's state */
static BdrLocksDBState *bdr_my_locks_database = NULL;

static bool this_xact_acquired_lock = false;

/* GUCs */
bool bdr_permit_ddl_locking = false;

static size_t
bdr_locks_shmem_size(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(BdrLocksCtl));
	size = add_size(size, mul_size(sizeof(BdrLocksDBState), bdr_max_databases));

	return size;
}

static void
bdr_locks_shmem_startup(void)
{
	bool        found;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	bdr_locks_ctl = ShmemInitStruct("bdr_locks",
									bdr_locks_shmem_size(),
									&found);
	if (!found)
	{
		memset(bdr_locks_ctl, 0, bdr_locks_shmem_size());
		bdr_locks_ctl->lock = LWLockAssign();
	}
	LWLockRelease(AddinShmemInitLock);
}

/* Needs to be called from a shared_preload_library _PG_init() */
void
bdr_locks_shmem_init()
{
	/* Must be called from postmaster its self */
	Assert(IsPostmasterEnvironment && !IsUnderPostmaster);

	bdr_locks_ctl = NULL;

	RequestAddinShmemSpace(bdr_locks_shmem_size());
	RequestAddinLWLocks(1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = bdr_locks_shmem_startup;
}

/*
 * Find, and create if necessary, the lock state entry for dboid.
 */
static BdrLocksDBState*
bdr_locks_find_database(Oid dboid, bool create)
{
	int off;
	int free_off = -1;

	for(off = 0; off < bdr_max_databases; off++)
	{
		BdrLocksDBState *db = &bdr_locks_ctl->dbstate[off];

		if (db->in_use && db->dboid == MyDatabaseId)
		{
			bdr_my_locks_database = db;
			return db;

		}
		if (!db->in_use && free_off == -1)
			free_off = off;
	}

	if (!create)
		/*
		 * We can't call get_databse_name here as the catalogs may not be
		 * accessible, so we can only report the oid of the database.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("database with oid=%u is not configured for bdr or bdr is still starting up",
						dboid)));

	if (free_off != -1)
	{
		BdrLocksDBState *db = &bdr_locks_ctl->dbstate[free_off];
		db->dboid = MyDatabaseId;
		db->in_use = true;
		return db;
	}

	ereport(ERROR,
			(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
			errmsg("Too many databases BDR-enabled for bdr.max_databases"),
			errhint("Increase bdr.max_databases above the current limit of %d", bdr_max_databases)));
}

static void
bdr_locks_find_my_database(bool create)
{
	Assert(IsUnderPostmaster);
	Assert(OidIsValid(MyDatabaseId));

	if (bdr_my_locks_database != NULL)
		return;

	bdr_my_locks_database = bdr_locks_find_database(MyDatabaseId, create);
	Assert(bdr_my_locks_database != NULL);
}

/*
 * This node has just started up. Init its local state and send a startup
 * announcement message.
 *
 * Called from the per-db worker.
 */
void
bdr_locks_startup()
{
	Relation		rel;
	ScanKey			key;
	SysScanDesc		scan;
	Snapshot		snap;
	HeapTuple		tuple;

	XLogRecPtr	lsn;
	StringInfoData s;

	Assert(IsUnderPostmaster);
	Assert(!IsTransactionState());
	Assert(bdr_worker_type == BDR_WORKER_PERDB);

	bdr_locks_find_my_database(true);

	/*
	 * Don't initialize database level lock state twice. An crash requiring
	 * that has to be severe enough to trigger a crash-restart cycle.
	 */
	if (bdr_my_locks_database->locked_and_loaded)
		return;

	/* We haven't yet established how many nodes we're connected to. */
	bdr_my_locks_database->nnodes = 0;

	initStringInfo(&s);

	/*
	 * Send restart message causing all other backends to release global locks
	 * possibly held by us. We don't necessarily remember sending the request
	 * out.
	 */
	bdr_prepare_message(&s, BDR_MESSAGE_START);

	elog(DEBUG1, "sending DDL lock startup message");
	lsn = LogStandbyMessage(s.data, s.len, false);
	resetStringInfo(&s);
	XLogFlush(lsn);

	/* reacquire all old ddl locks in table */
	StartTransactionCommand();
	snap = RegisterSnapshot(GetLatestSnapshot());
	rel = heap_open(BdrLocksRelid, RowExclusiveLock);

	key = (ScanKey) palloc(sizeof(ScanKeyData) * 1);

	ScanKeyInit(&key[0],
				8,
				BTEqualStrategyNumber, F_OIDEQ,
				bdr_my_locks_database->dboid);

	scan = systable_beginscan(rel, 0, true, snap, 1, key);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Datum		values[10];
		bool		isnull[10];
		const char *state;
		uint64		sysid;
		RepNodeId	node_id;

		heap_deform_tuple(tuple, RelationGetDescr(rel),
						  values, isnull);

		/* lookup the lock owner's node id */
		state = TextDatumGetCString(values[9]);
		if (sscanf(TextDatumGetCString(values[1]), UINT64_FORMAT, &sysid) != 1)
			elog(ERROR, "could not parse sysid %s",
				 TextDatumGetCString(values[1]));
		node_id = bdr_fetch_node_id_via_sysid(
			sysid, DatumGetObjectId(values[2]), DatumGetObjectId(values[3]));

		if (strcmp(state, "acquired") == 0)
		{
			/* XXX: There should be only one item so this is probably fine. */
			bdr_my_locks_database->lock_holder = node_id;
			bdr_my_locks_database->lockcount++;
			/* A remote node might have held the local DDL lock before restart */
			elog(DEBUG1, "reacquiring local DDL lock held before shutdown");
		}
		else if (strcmp(state, "catchup") == 0)
		{
			XLogRecPtr		wait_for_lsn;

			/*
			 * Restart the catchup period. There shouldn't be any need to
			 * kickof sessions here, because we're starting early.
			 */
			wait_for_lsn = GetXLogInsertRecPtr();
			bdr_prepare_message(&s, BDR_MESSAGE_REQUEST_REPLAY_CONFIRM);
			pq_sendint64(&s, wait_for_lsn);
			lsn = LogStandbyMessage(s.data, s.len, false);
			XLogFlush(lsn);
			resetStringInfo(&s);

			/* XXX: There should be only one item so this is probably fine. */
			bdr_my_locks_database->lock_holder = node_id;
			bdr_my_locks_database->lockcount++;
			bdr_my_locks_database->replay_confirmed = 0;
			bdr_my_locks_database->replay_confirmed_lsn = wait_for_lsn;

			elog(DEBUG1, "restarting DDL lock replay catchup phase");
		}
		else
			elog(PANIC, "unknown lockstate '%s'", state);
	}

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, NoLock);

	CommitTransactionCommand();

	elog(DEBUG2, "DDL locking startup completed, local DML enabled");

	/* allow local DML */
	bdr_my_locks_database->locked_and_loaded = true;
}

void
bdr_locks_set_nnodes(Size nnodes)
{
	Assert(IsBackgroundWorker);
	Assert(bdr_my_locks_database != NULL);

	/*
	 * XXX DYNCONF No protection against node addition during DDL lock acquire
	 *
	 * Node counts are currently grabbed straight from the perdb worker's shmem
	 * and could change whenever someone adds a worker, with no locking or
	 * protection.
	 *
	 * We could acquire the local DDL lock before setting the nodecount, which
	 * would cause requests from other nodes to get rejected and cause other
	 * local tx's to fail to request the global DDL lock. However, we'd have to
	 * acquire it when we committed to adding the new worker, which happens in
	 * a user backend, and release it from the perdb worker once the new worker
	 * is registered. Fragile.
	 *
	 * Doing so also fails to solve the other half of the problem, which is
	 * that DDL locking expects there to be one bdr walsender for each apply
	 * worker, i.e. each connection should be reciprocal. We could connect to
	 * the other end and register a connection back to us, but that's getting
	 * complicated for what's always going to be a temporary option before a
	 * full part/join protocol is added.
	 *
	 * So we're just going to cross our fingers. Worst case is that DDL locking
	 * gets stuck and we have to restart all the nodes.
	 *
	 * The full part/join protocol will solve this by acquiring the DDL lock
	 * before joining.
	 */
	bdr_my_locks_database->nnodes = nnodes;
}


static void
bdr_prepare_message(StringInfo s, BdrMessageType message_type)
{
	/* channel */
	pq_sendint(s, strlen("bdr"), 4);
	pq_sendbytes(s, "bdr", strlen("bdr"));
	/* message type */
	pq_sendint(s, message_type, 4);
	/* node identifier */
	pq_sendint64(s, GetSystemIdentifier()); /* sysid */
	pq_sendint(s, ThisTimeLineID, 4); /* tli */
	pq_sendint(s, MyDatabaseId, 4); /* database */
	pq_sendint(s, 0, 4); /* name, always empty for now */

	/* caller's data will follow */
}

static void
bdr_lock_xact_callback(XactEvent event, void *arg)
{
	if (!this_xact_acquired_lock)
		return;

	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT)
	{
		XLogRecPtr lsn;
		StringInfoData s;

		initStringInfo(&s);
		bdr_prepare_message(&s, BDR_MESSAGE_RELEASE_LOCK);

		pq_sendint64(&s, GetSystemIdentifier()); /* sysid */
		pq_sendint(&s, ThisTimeLineID, 4); /* tli */
		pq_sendint(&s, MyDatabaseId, 4); /* database */
		/* no name! locks are db wide */

		lsn = LogStandbyMessage(s.data, s.len, false);
		XLogFlush(lsn);

		LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
		if (bdr_my_locks_database->lockcount > 0)
			bdr_my_locks_database->lockcount--;
		else
			elog(WARNING, "Releasing unacquired DDL lock");

		this_xact_acquired_lock = false;
		bdr_my_locks_database->replay_confirmed = 0;
		bdr_my_locks_database->replay_confirmed_lsn = InvalidXLogRecPtr;
		bdr_my_locks_database->waiting_latch = NULL;

		LWLockRelease(bdr_locks_ctl->lock);
	}
}

static void
register_xact_callback()
{
	static bool registered;

	if (!registered)
	{
		RegisterXactCallback(bdr_lock_xact_callback, NULL);
		registered = true;
	}
}

static SysScanDesc
locks_begin_scan(Relation rel, Snapshot snap, uint64 sysid, TimeLineID tli, Oid datid)
{
	ScanKey			key;
	char			buf[30];
	key = (ScanKey) palloc(sizeof(ScanKeyData) * 4);

	sprintf(buf, UINT64_FORMAT, sysid);

	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum("ddl_lock"));
	ScanKeyInit(&key[1],
				2,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(buf));
	ScanKeyInit(&key[2],
				3,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(tli));
	ScanKeyInit(&key[3],
				4,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(datid));

	return systable_beginscan(rel, 0, true, snap, 4, key);
}

/*
 * Acquire DDL lock on the side that wants to perform DDL.
 *
 * Called from a user backend when the command filter spots a DDL attempt; runs
 * in the user backend.
 */
void
bdr_acquire_ddl_lock(void)
{
	XLogRecPtr	lsn;
	StringInfoData s;

	Assert(IsTransactionState());
	/* Not called from within a BDR worker */
	Assert(bdr_worker_type == BDR_WORKER_EMPTY_SLOT);

	if (this_xact_acquired_lock)
		return;

	if (!bdr_permit_ddl_locking)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Global DDL locking attempt rejected by configuration"),
				 errdetail("bdr.permit_ddl_locking is false and the attempted command "
						   "would require the global DDL lock to be acquired. "
						   "Command rejected."),
				 errhint("See the 'DDL replication' chapter of the documentation.")));
	}

	initStringInfo(&s);

	bdr_locks_find_my_database(false);

	if (bdr_my_locks_database->nnodes == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("No peer nodes or peer node count unknown, cannot acquire DDL lock"),
				 errhint("BDR is probably still starting up, wait a while")));
	}

	elog(DEBUG2, "attempting to acquire global DDL lock for (" BDR_LOCALID_FORMAT ")", BDR_LOCALID_FORMAT_ARGS);

	/* send message about ddl lock */
	bdr_prepare_message(&s, BDR_MESSAGE_ACQUIRE_LOCK);

	/* register an XactCallback to release the lock */
	register_xact_callback();

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);

	/* first check whether the lock can't actually be acquired */
	if (bdr_my_locks_database->lockcount > 0)
	{
		uint64		holder_sysid;
		TimeLineID	holder_tli;
		Oid			holder_datid;

		bdr_fetch_sysid_via_node_id(bdr_my_locks_database->lock_holder,
									&holder_sysid, &holder_tli,
									&holder_datid);

		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("database is locked against ddl by another node"),
				 errhint("Node ("UINT64_FORMAT",%u,%u) in the cluster is already performing DDL",
						 holder_sysid, holder_tli, holder_datid)));
	}

	START_CRIT_SECTION();

	/*
	 * NB: We need to setup the state as if we'd have already acquired the
	 * lock - otherwise concurrent transactions could acquire the lock; and we
	 * wouldn't send a release message when we fail to fully acquire the lock.
	 */
	bdr_my_locks_database->lockcount++;
	this_xact_acquired_lock = true;
	bdr_my_locks_database->acquire_confirmed = 0;
	bdr_my_locks_database->acquire_declined = 0;
	bdr_my_locks_database->waiting_latch = &MyProc->procLatch;

	/* lock looks to be free, try to acquire it */

	lsn = LogStandbyMessage(s.data, s.len, false);
	XLogFlush(lsn);

	END_CRIT_SECTION();

	LWLockRelease(bdr_locks_ctl->lock);

	/* ---
	 * Now wait for standbys to ack ddl lock
	 * ---
	 */
	elog(DEBUG2, "sent DDL lock request, waiting for confirmation");

	while (true)
	{
		int rc;

		ResetLatch(&MyProc->procLatch);

		LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);

		/* check for confirmations in shared memory */
		if (bdr_my_locks_database->acquire_declined > 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("could not acquire DDL lock - another node has declined our lock request"),
					 errhint("Likely the other node is acquiring the DDL lock itself.")));
		}

		/* wait till all have given their consent */
		if (bdr_my_locks_database->acquire_confirmed >= bdr_my_locks_database->nnodes)
		{
			LWLockRelease(bdr_locks_ctl->lock);
			break;
		}
		LWLockRelease(bdr_locks_ctl->lock);

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10000L);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();
	}

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);

	/* TODO: recheck it's ours */
	bdr_my_locks_database->acquire_confirmed = 0;
	bdr_my_locks_database->acquire_declined = 0;
	bdr_my_locks_database->waiting_latch = NULL;

	elog(DEBUG1, "global DDL lock acquired successfully by (" BDR_LOCALID_FORMAT ")", BDR_LOCALID_FORMAT_ARGS);

	LWLockRelease(bdr_locks_ctl->lock);

}

static bool
check_is_my_origin_node(uint64 sysid, TimeLineID tli, Oid datid)
{
	uint64 replay_sysid;
	TimeLineID replay_tli;
	Oid replay_datid;

	Assert(!IsTransactionState());

	StartTransactionCommand();
	bdr_fetch_sysid_via_node_id(replication_origin_id, &replay_sysid,
								&replay_tli, &replay_datid);
	CommitTransactionCommand();

	if (sysid != replay_sysid ||
		tli != replay_tli ||
		datid != replay_datid)
		return false;
	return true;
}

static bool
check_is_my_node(uint64 sysid, TimeLineID tli, Oid datid)
{
	if (sysid != GetSystemIdentifier() ||
		tli != ThisTimeLineID ||
		datid != MyDatabaseId)
		return false;
	return true;
}

/*
 * Another node has asked for a DDL lock. Try to acquire the local ddl lock.
 *
 * Runs in the apply worker.
 */
void
bdr_process_acquire_ddl_lock(uint64 sysid, TimeLineID tli, Oid datid)
{
	StringInfoData	s;
	XLogRecPtr lsn;

	Assert(!IsTransactionState());
	Assert(bdr_worker_type == BDR_WORKER_APPLY);

	/* Don't care about locks acquired locally. Already held. */
	if (!check_is_my_origin_node(sysid, tli, datid))
		return;

	bdr_locks_find_my_database(false);

	elog(DEBUG1, "global DDL lock requested by node ("UINT64_FORMAT",%u,%u)", sysid, tli, datid);

	initStringInfo(&s);

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);

	if (bdr_my_locks_database->lockcount == 0)
	{
		XLogRecPtr wait_for_lsn;
		VirtualTransactionId *conflicts;
		Relation rel;
		Datum	values[10];
		bool	nulls[10];
		HeapTuple tup;

		/*
		 * No previous DDL lock found. Start acquiring it.
		 */
		elog(DEBUG1, "no prior DDL lock found, acquiring local DDL lock");

		/* Add a row to bdr_locks */
		StartTransactionCommand();

		memset(nulls, 0, sizeof(nulls));

		rel = heap_open(BdrLocksRelid, RowExclusiveLock);

		values[0] = CStringGetTextDatum("ddl_lock");

		appendStringInfo(&s, UINT64_FORMAT, sysid);
		values[1] = CStringGetTextDatum(s.data);
		resetStringInfo(&s);
		values[2] = ObjectIdGetDatum(tli);
		values[3] = ObjectIdGetDatum(datid);

		values[4] = TimestampTzGetDatum(GetCurrentTimestamp());

		appendStringInfo(&s, UINT64_FORMAT, GetSystemIdentifier());
		values[5] = CStringGetTextDatum(s.data);
		resetStringInfo(&s);
		values[6] = ObjectIdGetDatum(ThisTimeLineID);
		values[7] = ObjectIdGetDatum(MyDatabaseId);

		nulls[8] = true;

		values[9] = PointerGetDatum(cstring_to_text("catchup"));

		PG_TRY();
		{
			tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
			simple_heap_insert(rel, tup);
			CatalogUpdateIndexes(rel, tup);
			ForceSyncCommit(); /* async commit would be too complicated */
			heap_close(rel, NoLock);
			CommitTransactionCommand();
		}
		PG_CATCH();
		{
			if (geterrcode() == ERRCODE_UNIQUE_VIOLATION)
			{
				elog(DEBUG1, "declining DDL lock because a conflicting DDL lock exists in bdr_global_locks");
				AbortOutOfAnyTransaction();
				goto decline;
			}
			else
				PG_RE_THROW();
		}
		PG_END_TRY();

		/* setup ddl lock */
		bdr_my_locks_database->lockcount++;
		bdr_my_locks_database->lock_holder = replication_origin_id;
		LWLockRelease(bdr_locks_ctl->lock);

		/*
		 * Now kill all local processes that are still writing. We can't just
		 * prevent them from writing via the acquired lock as they are still
		 * running. We're using drastic measures here because it'd be a bad
		 * idea to wait: The primary is waiting for us and during that time
		 * the entire flock has to wait.
		 *
		 * TODO: It'd be *far* nicer to only cancel other transactions if they
		 * held conflicting locks, but that's not easiliy possible at the
		 * moment.
		 */
		elog(DEBUG1, "terminating any local processes that conflict with the DDL lock");
		conflicts = GetConflictingVirtualXIDs(InvalidTransactionId, MyDatabaseId);
		while (conflicts->backendId != InvalidBackendId)
		{
			pid_t p;

			/* Don't kill ourselves */
			if (conflicts->backendId == MyBackendId)
			{
				conflicts++;
				continue;
			}

			/* try to kill */
			p = CancelVirtualTransaction(*conflicts, PROCSIG_RECOVERY_CONFLICT_LOCK);

			/*
			 * Either confirm kill or sleep a bit to prevent the other node
			 * being busy with signal processing.
			 */
			if (p == 0)
				conflicts++;
			else
				pg_usleep(5000);

			elog(DEBUG2, "signaled pid %d to terminate because it conflicts with a DDL lock requested by another node", p);
		}

		/*
		 * We now have to wait till all our local pending changes have been
		 * streamed out. We do this by sending a message which is then acked
		 * by all other nodes. When the required number of messages is back we
		 * can confirm the lock to the original requestor
		 * (c.f. bdr_process_replay_confirm()).
		 *
		 * If we didn't wait for everyone to replay local changes then a DDL
		 * change that caused those local changes not to apply on remote
		 * nodes might occur, causing a divergent conflict.
		 */
		elog(DEBUG1, "requesting replay confirmation from all other nodes before confirming global DDL lock granted");

		wait_for_lsn = GetXLogInsertRecPtr();
		bdr_prepare_message(&s, BDR_MESSAGE_REQUEST_REPLAY_CONFIRM);
		pq_sendint64(&s, wait_for_lsn);

		LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
		lsn = LogStandbyMessage(s.data, s.len, false);
		XLogFlush(lsn);

		bdr_my_locks_database->replay_confirmed = 0;
		bdr_my_locks_database->replay_confirmed_lsn = wait_for_lsn;
		LWLockRelease(bdr_locks_ctl->lock);

		resetStringInfo(&s);

		elog(DEBUG1, "DDL lock granted to remote node (" BDR_LOCALID_FORMAT ")",
			 sysid, tli, datid, "");
	}
	else
	{
		uint64 replay_sysid;
		TimeLineID replay_tli;
		Oid replay_datid;
		LWLockRelease(bdr_locks_ctl->lock);
decline:
		ereport(LOG,
				(errmsg("Declining remote DDL lock request, this node is already locked")));
		bdr_prepare_message(&s, BDR_MESSAGE_DECLINE_LOCK);

		Assert(!IsTransactionState());
		StartTransactionCommand();
		bdr_fetch_sysid_via_node_id(bdr_my_locks_database->lock_holder,
									&replay_sysid, &replay_tli,
									&replay_datid);
		CommitTransactionCommand();

		pq_sendint64(&s, replay_sysid); /* sysid */
		pq_sendint(&s, replay_tli, 4); /* tli */
		pq_sendint(&s, replay_datid, 4); /* database */
		/* no name! locks are db wide */

		lsn = LogStandbyMessage(s.data, s.len, false);
		XLogFlush(lsn);
		resetStringInfo(&s);
	}
}

/*
 * Another node has released the global DDL lock, update our local state.
 *
 * Runs in the apply worker.
 */
void
bdr_process_release_ddl_lock(uint64 origin_sysid, TimeLineID origin_tli, Oid origin_datid,
							 uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid)
{
	Relation		rel;
	Snapshot		snap;
	SysScanDesc		scan;
	HeapTuple		tuple;
	bool			found = false;
	Latch		   *latch;
	StringInfoData	s;

	Assert(bdr_worker_type == BDR_WORKER_APPLY);

	if (!check_is_my_origin_node(origin_sysid, origin_tli, origin_datid))
		return;

	/* FIXME: check db */

	bdr_locks_find_my_database(false);

	initStringInfo(&s);

	elog(DEBUG1, "DDL lock released by (" BDR_LOCALID_FORMAT ")",
		 lock_sysid, lock_tli, lock_datid, "");

	/*
	 * Remove row from bdr_locks *before* releasing the in memory lock. If we
	 * crash we'll replay the event again.
	 */
	StartTransactionCommand();
	snap = RegisterSnapshot(GetLatestSnapshot());
	rel = heap_open(BdrLocksRelid, RowExclusiveLock);

	scan = locks_begin_scan(rel, snap, origin_sysid, origin_tli, origin_datid);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		elog(DEBUG1, "found DDL lock entry to delete in response to DDL lock release message");
		simple_heap_delete(rel, &tuple->t_self);
		ForceSyncCommit(); /* async commit would be too complicated */
		found = true;
	}

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, NoLock);
	CommitTransactionCommand();

	/*
	 * Note that it's not unexpected to receive release requests for locks
	 * this node hasn't acquired. It e.g. happens if lock acquisition failed
	 * halfway through.
	 */

	if (!found)
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Did not find local DDL lock entry for a remotely released global DDL lock"),
				 errdetail("node ("BDR_LOCALID_FORMAT") sent a release message but the lock isn't held locally",
						   lock_sysid, lock_tli, lock_datid, "")));

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	if (bdr_my_locks_database->lockcount > 0)
	{
		bdr_my_locks_database->lockcount--;
		bdr_my_locks_database->lock_holder = InvalidRepNodeId;
		/* XXX: recheck owner of lock */
	}

	latch = bdr_my_locks_database->waiting_latch;

	bdr_my_locks_database->replay_confirmed = 0;
	bdr_my_locks_database->replay_confirmed_lsn = InvalidXLogRecPtr;
	bdr_my_locks_database->waiting_latch = NULL;

	LWLockRelease(bdr_locks_ctl->lock);

	elog(DEBUG1, "local DDL lock released");

	/* notify an eventual waiter */
	if(latch)
		SetLatch(latch);
}

/*
 * Another node has confirmed that a node has acquired the DDL lock
 * successfully. If the acquiring node was us, change shared memory state and
 * wake up the user backend that was trying to acquire the lock.
 *
 * Runs in the apply worker.
 */
void
bdr_process_confirm_ddl_lock(uint64 origin_sysid, TimeLineID origin_tli, Oid origin_datid,
							 uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid)
{
	Latch *latch;

	Assert(bdr_worker_type == BDR_WORKER_APPLY);

	if (!check_is_my_origin_node(origin_sysid, origin_tli, origin_datid))
		return;

	/* don't care if another database has gotten the lock */
	if (!check_is_my_node(lock_sysid, lock_tli, lock_datid))
		return;

	bdr_locks_find_my_database(false);

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	bdr_my_locks_database->acquire_confirmed++;
	latch = bdr_my_locks_database->waiting_latch;

	elog(DEBUG2, "received DDL lock confirmation number %d/%zu from ("BDR_LOCALID_FORMAT")",
		 bdr_my_locks_database->acquire_confirmed, bdr_my_locks_database->nnodes,
		 origin_sysid, origin_tli, origin_datid, "");
	LWLockRelease(bdr_locks_ctl->lock);

	if(latch)
		SetLatch(latch);
}

/*
 * Another node has declined a lock. If it was us, change shared memory state
 * and wakeup the user backend that tried to acquire the lock.
 *
 * Runs in the apply worker.
 */
void
bdr_process_decline_ddl_lock(uint64 origin_sysid, TimeLineID origin_tli, Oid origin_datid,
							 uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid)
{
	Latch *latch;

	Assert(bdr_worker_type == BDR_WORKER_APPLY);

	/* don't care if another database has been declined a lock */
	if (!check_is_my_origin_node(origin_sysid, origin_tli, origin_datid))
		return;

	bdr_locks_find_my_database(false);

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	bdr_my_locks_database->acquire_declined++;
	latch = bdr_my_locks_database->waiting_latch;
	LWLockRelease(bdr_locks_ctl->lock);
	if(latch)
		SetLatch(latch);

	elog(DEBUG2, "global lock request declined by node ("BDR_LOCALID_FORMAT")",
		 origin_sysid, origin_tli, origin_datid, "");
}

/*
 * Another node has asked us to confirm that we've replayed up to a given LSN.
 * We've seen the request message, so send the requested confirmation.
 *
 * Runs in the apply worker.
 */
void
bdr_process_request_replay_confirm(uint64 sysid, TimeLineID tli,
								   Oid datid, XLogRecPtr request_lsn)
{
	XLogRecPtr lsn;
	StringInfoData s;

	Assert(bdr_worker_type == BDR_WORKER_APPLY);

	if (!check_is_my_origin_node(sysid, tli, datid))
		return;

	bdr_locks_find_my_database(false);

	elog(DEBUG2, "replay confirmation requested by node ("BDR_LOCALID_FORMAT"); sending",
		 sysid, tli, datid, "");

	initStringInfo(&s);
	bdr_prepare_message(&s, BDR_MESSAGE_REPLAY_CONFIRM);
	pq_sendint64(&s, request_lsn);
	lsn = LogStandbyMessage(s.data, s.len, false);
	XLogFlush(lsn);

}

/*
 * A remote node has seen a replay confirmation request and replied to it.
 *
 * If we sent the original request, update local state appropriately.
 *
 * If a DDL lock request has reached quorum as a result of this confirmation,
 * write a log acquisition confirmation and bdr_global_locks update to xlog.
 *
 * Runs in the apply worker.
 */
void
bdr_process_replay_confirm(uint64 sysid, TimeLineID tli,
						   Oid datid, XLogRecPtr request_lsn)
{
	bool quorum_reached = false;

	Assert(bdr_worker_type == BDR_WORKER_APPLY);

	if (!check_is_my_origin_node(sysid, tli, datid))
		return;

	bdr_locks_find_my_database(false);

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	elog(DEBUG2, "processing replay confirmation from node ("BDR_LOCALID_FORMAT") for request %X/%X at %X/%X",
		 sysid, tli, datid, "",
		 (uint32)(bdr_my_locks_database->replay_confirmed_lsn >> 32),
		 (uint32)bdr_my_locks_database->replay_confirmed_lsn,
		 (uint32)(request_lsn >> 32),
		 (uint32)request_lsn);

	/* request matches the one we're interested in */
	if (bdr_my_locks_database->replay_confirmed_lsn == request_lsn)
	{
		bdr_my_locks_database->replay_confirmed++;

		elog(DEBUG2, "confirming replay %u/%zu",
			 bdr_my_locks_database->replay_confirmed,
			 bdr_my_locks_database->nnodes);

		quorum_reached =
			bdr_my_locks_database->replay_confirmed >= bdr_my_locks_database->nnodes;
	}

	if (quorum_reached)
	{
		Relation		rel;
		SysScanDesc		scan;
		Snapshot		snap;
		HeapTuple		tuple;

		uint64			replay_sysid;
		TimeLineID		replay_tli;
		Oid				replay_datid;
		StringInfoData	s;
		bool			found = false;

		initStringInfo(&s);

		elog(DEBUG2, "DDL lock quorum reached, logging confirmation of this node's acquisition of global DDL lock");

		bdr_my_locks_database->replay_confirmed = 0;
		bdr_my_locks_database->replay_confirmed_lsn = InvalidXLogRecPtr;
		bdr_my_locks_database->waiting_latch = NULL;

		bdr_prepare_message(&s, BDR_MESSAGE_CONFIRM_LOCK);

		Assert(!IsTransactionState());
		StartTransactionCommand();
		bdr_fetch_sysid_via_node_id(bdr_my_locks_database->lock_holder,
									&replay_sysid, &replay_tli,
									&replay_datid);

		pq_sendint64(&s, replay_sysid); /* sysid */
		pq_sendint(&s, replay_tli, 4); /* tli */
		pq_sendint(&s, replay_datid, 4); /* database */
		/* no name! locks are db wide */

		LogStandbyMessage(s.data, s.len, true); /* transactional */

		/*
		 * Update state of lock. Do so in the same xact that confirms the
		 * lock. That way we're safe against crashes.
		 */
		/* Scan for a matching lock whose state needs to be updated */
		snap = RegisterSnapshot(GetLatestSnapshot());
		rel = heap_open(BdrLocksRelid, RowExclusiveLock);

		scan = locks_begin_scan(rel, snap, replay_sysid, replay_tli, replay_datid);

		while ((tuple = systable_getnext(scan)) != NULL)
		{
			HeapTuple	newtuple;
			Datum		values[10];
			bool		isnull[10];

			if (found)
				elog(PANIC, "Duplicate lock?");

			elog(DEBUG1, "updating DDL lock state from 'catchup' to 'acquired'");

			heap_deform_tuple(tuple, RelationGetDescr(rel),
							  values, isnull);
			/* status column */
			values[9] = CStringGetTextDatum("acquired");

			newtuple = heap_form_tuple(RelationGetDescr(rel),
									   values, isnull);
			simple_heap_update(rel, &tuple->t_self, newtuple);
			CatalogUpdateIndexes(rel, newtuple);
			found = true;
		}

		if (!found)
			elog(PANIC, "got confirmation for unknown lock");

		systable_endscan(scan);
		UnregisterSnapshot(snap);
		heap_close(rel, NoLock);

		CommitTransactionCommand();

		elog(DEBUG2, "sent confirmation of successful DDL lock acquisition");
	}

	LWLockRelease(bdr_locks_ctl->lock);
}

/*
 * A remote node has sent a startup message. Update any appropriate local state
 * like any locally held DDL locks for it.
 *
 * Runs in the apply worker.
 */
void
bdr_locks_process_remote_startup(uint64 sysid, TimeLineID tli, Oid datid)
{
	Relation rel;
	Snapshot snap;
	SysScanDesc scan;
	HeapTuple tuple;
	StringInfoData s;

	Assert(bdr_worker_type == BDR_WORKER_APPLY);

	bdr_locks_find_my_database(false);

	initStringInfo(&s);

	elog(DEBUG2, "got startup message from node ("BDR_LOCALID_FORMAT"), clearing any locks it held",
		 sysid, tli, datid, "");

	StartTransactionCommand();
	snap = RegisterSnapshot(GetLatestSnapshot());
	rel = heap_open(BdrLocksRelid, RowExclusiveLock);

	scan = locks_begin_scan(rel, snap, sysid, tli, datid);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		elog(DEBUG2, "found remote lock to delete (after remote restart)");

		simple_heap_delete(rel, &tuple->t_self);
		/* FIXME: locks */
		LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
		if (bdr_my_locks_database->lockcount == 0)
			elog(WARNING, "bdr_global_locks row exists without corresponding in memory state");
		else
		{
			bdr_my_locks_database->lockcount--;
			bdr_my_locks_database->lock_holder = InvalidRepNodeId;
			bdr_my_locks_database->replay_confirmed = 0;
			bdr_my_locks_database->replay_confirmed_lsn = InvalidXLogRecPtr;
		}
		LWLockRelease(bdr_locks_ctl->lock);
	}

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, NoLock);
	CommitTransactionCommand();
}

/*
 * Function for checking if there is no conflicting BDR lock.
 *
 * Should be caled from ExecutorStart_hook.
 */
void
bdr_locks_check_dml(void)
{

	if (bdr_skip_ddl_locking)
		return;

	bdr_locks_find_my_database(false);

	/* is the database still starting up and hasn't loaded locks */
	if (!bdr_my_locks_database->locked_and_loaded)
		/*
		 * TODO: sleep here instead of ERRORing, and let statement_timeout
		 * ERROR if appropriate
		 */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Database is not yet ready for DDL operations"),
				 errdetail("BDR DDL locking is still starting up"),
				 errhint("Wait for a short time and retry.")));

	/* Is this database locked against user initiated ddl? */
	pg_memory_barrier();
	if (bdr_my_locks_database->lockcount > 0 && !this_xact_acquired_lock)
	{
		uint64		holder_sysid;
		TimeLineID	holder_tli;
		Oid			holder_datid;

		bdr_fetch_sysid_via_node_id(bdr_my_locks_database->lock_holder,
									&holder_sysid, &holder_tli,
									&holder_datid);

		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("Database is locked against DDL operations"),
				 errhint("Node ("UINT64_FORMAT",%u,%u) in the cluster is already performing DDL",
						 holder_sysid, holder_tli, holder_datid)));

	}
}

#else

/* bdr_locks are not used by UDR at the moment */
void
bdr_locks_startup()
{
}

void
bdr_locks_shmem_init()
{
}

void
bdr_acquire_ddl_lock(void)
{
}

void
bdr_locks_check_dml(void)
{
}
#endif
