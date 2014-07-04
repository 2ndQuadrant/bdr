/* -------------------------------------------------------------------------
 *
 * bdr_locks.c
 *		global ddl/dml interlocking locks
 *
 *
 * Copyright (C) 2014, PostgreSQL Global Development Group
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
 *    DDL lock acquiration basically works like this:
 *
 *    1) A utility command notices that it needs the ddl lock. If there
 *       already is a local ddl lock it'll ERROR out.
 *	  2) It sends out a 'acquire_lock' message to all other nodes.
 *    3) When another node receives a 'acquire_lock' message it checks whether
 *       the ddl lock is already held locally. If so it'll send a
 *       'decline_lock' message back causing the lock acquiration to fail.
 *    4) If a 'acquire_lock' message is received and the DDL lock is not held
 *    	 locally it'll be acquired and an entry into the 'bdr_global_locks'
 *    	 table will be made marking the lock to be in the 'catchup' phase.
 *    5) All concurrent user transactions will be cancelled.
 *	  6) A 'request_replay_confirm' message will be sent to all other nodes
 *	     containing a lsn that has to be replayed.
 *    7) When a 'request_replay_confirm' message is received, a
 *       'replay_confirm' message will be sent back.
 *    8) Once all other nodes have replied with 'replay_confirm' the DDL lock
 *       has been successfully acquired on the node reading the 'acquire_lock'
 *       message (from 3)). The corresponding bdr_global_locks entry will be
 *       updated to the 'acquired' state and a 'confirm_lock' message will be sent out.
 *    9) Once all nodes have replied with 'confirm_lock' messages the ddl lock
 *    	 has been acquired.
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
 *		contrib/bdr/bdr_locks.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"
#include "bdr_locks.h"

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
static void BdrExecutorStart(QueryDesc *queryDesc, int eflags);
static void bdr_prepare_message(StringInfo s, BdrMessageType message_type);

static BdrLocksCtl *bdr_locks_ctl;

/* shmem init hook to chain to on startup, if any */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type PrevExecutorStart_hook = NULL;

/* number of per database slots */
static int bdr_locks_num_databases;

/* this database's state */
static BdrLocksDBState *bdr_my_locks_database = NULL;

static bool bdr_always_allow_writes = false;

static bool this_xact_acquired_lock = false;

static size_t
bdr_locks_shmem_size(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(BdrLocksCtl));
	size = add_size(size, mul_size(sizeof(BdrLocksDBState), bdr_locks_num_databases));

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
bdr_locks_shmem_init(Size num_used_databases)
{
	PrevExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = BdrExecutorStart;

	bdr_locks_ctl = NULL;
	bdr_locks_num_databases = num_used_databases;

	RequestAddinShmemSpace(bdr_locks_shmem_size());
	RequestAddinLWLocks(1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = bdr_locks_shmem_startup;
}

/*
 * Find, and create if neccessary, the lock state entry for dboid.
 */
static BdrLocksDBState*
bdr_locks_find_database(Oid dboid, bool create)
{
	int off;
	int free_off = -1;

	for(off = 0; off < bdr_locks_num_databases; off++)
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
		elog(ERROR, "database %u is not configured for bdr", dboid);

	if (free_off != -1)
	{
		BdrLocksDBState *db = &bdr_locks_ctl->dbstate[free_off];
		db->dboid = MyDatabaseId;
		db->in_use = true;
		return db;
	}
	elog(PANIC, "too many databases in use for bdr");
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
 *
 */
void
bdr_locks_startup(Size nnodes)
{
	Relation		rel;
	SysScanDesc		scan;
	Snapshot		snap;
	HeapTuple		tuple;

	XLogRecPtr	lsn;
	StringInfoData s;

	Assert(IsUnderPostmaster);
	Assert(!IsTransactionState());

	bdr_locks_find_my_database(true);

	/*
	 * Don't initialize database level lock state twice. An crash requiring
	 * that has to be severe enough to trigger a crash-restart cycle.
	 */
	if (bdr_my_locks_database->locked_and_loaded)
		return;

	bdr_my_locks_database->nnodes = nnodes;

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

	scan = systable_beginscan(rel, 0, true, snap, 0, NULL);

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
			BdrLocksDBState *db =
				bdr_locks_find_database(DatumGetObjectId(values[7]), false);
			db->lock_holder = node_id;
			db->lockcount++;
			elog(DEBUG1, "reacquiring DDL lock held before shutdown");
		}
		else if (strcmp(state, "catchup") == 0)
		{
			XLogRecPtr		wait_for_lsn;
			BdrLocksDBState *db;

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

			db = bdr_locks_find_database(DatumGetObjectId(values[7]), false);
			db->lock_holder = node_id;
			db->lockcount++;
			db->replay_confirmed = 0;
			db->replay_confirmed_lsn = wait_for_lsn;

			elog(DEBUG1, "restarting DDL lock replay catchup phase");
		}
		else
			elog(PANIC, "unknown lockstate");
	}

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, NoLock);

	CommitTransactionCommand();

	/* allow local DML */
	bdr_my_locks_database->locked_and_loaded = true;
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
			elog(WARNING, "releasing unacquired DDL lock");
		LWLockRelease(bdr_locks_ctl->lock);
		this_xact_acquired_lock = false;
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
 */
void
bdr_acquire_ddl_lock(void)
{
	XLogRecPtr	lsn;
	StringInfoData s;

	Assert(IsTransactionState());

	if (this_xact_acquired_lock)
		return;

	initStringInfo(&s);

	bdr_locks_find_my_database(false);

	/* send message about ddl lock */
	bdr_prepare_message(&s, BDR_MESSAGE_ACQUIRE_LOCK);

	/* register an XactCallback to release the lock */
	register_xact_callback();

	/* send message about ddl lock */
	lsn = LogStandbyMessage(s.data, s.len, false);
	XLogFlush(lsn);

	/* ---
	 * Now wait for standbys to ack ddl lock
	 * ---
	 */

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	if (bdr_my_locks_database->lockcount > 0)
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("database is locked against ddl by another node"),
				 errhint("Some node in the cluster is performing DDL")));

	bdr_my_locks_database->acquire_confirmed = 0;
	bdr_my_locks_database->acquire_declined = 0;
	bdr_my_locks_database->waiting_latch = &MyProc->procLatch;
	LWLockRelease(bdr_locks_ctl->lock);

	elog(DEBUG1, "waiting for the other nodes acks");

	while (true)
	{
		int rc;

		ResetLatch(&MyProc->procLatch);

		LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);

		/* check for confirmations in shared memory */
		if (bdr_my_locks_database->acquire_declined > 0)
		{
			LWLockRelease(bdr_locks_ctl->lock);
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
	bdr_my_locks_database->lockcount++;
	this_xact_acquired_lock = true;
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
 */
void
bdr_process_acquire_ddl_lock(uint64 sysid, TimeLineID tli, Oid datid)
{
	StringInfoData	s;
	XLogRecPtr lsn;

	Assert(!IsTransactionState());

	/* Don't care about locks acquired locally. Already held. */
	if (!check_is_my_origin_node(sysid, tli, datid))
		return;

	bdr_locks_find_my_database(false);

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
		 * Now kill all local processes that are still writing. We won't
		 * prevent them from writing via the acquired lock as they are still
		 * running. We're using drastic measures here because it'd be a bad
		 * idea to wait: The primary is waiting for us and during that time
		 * the entire flock has to wait.
		 *
		 * TODO: It'd be *far* nicer to only cancel other transactions if they
		 * held conflicting locks, but that's not easiliy possible at the
		 * moment.
		 */
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
		}

		/*
		 * We now have to wait till all our local pending changes have been
		 * streamed out. We do this by sending a message which is then acked
		 * by all other nodes. When the required number of messages is back we
		 * can confirm the lock to the original requestor
		 * (c.f. bdr_process_replay_confirm()).
		 */
		elog(DEBUG1, "requesting replay confirmation from all other nodes");

		wait_for_lsn = GetXLogInsertRecPtr();
		bdr_prepare_message(&s, BDR_MESSAGE_REQUEST_REPLAY_CONFIRM);
		pq_sendint64(&s, wait_for_lsn);
		lsn = LogStandbyMessage(s.data, s.len, false);
		XLogFlush(lsn);
		resetStringInfo(&s);

		LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
		bdr_my_locks_database->replay_confirmed = 0;
		bdr_my_locks_database->replay_confirmed_lsn = wait_for_lsn;
		LWLockRelease(bdr_locks_ctl->lock);
	}
	else
	{
		uint64 replay_sysid;
		TimeLineID replay_tli;
		Oid replay_datid;
		LWLockRelease(bdr_locks_ctl->lock);
decline:
		ereport(LOG,
				(errmsg("declining remote DDL lock request, already locked")));
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

	if (!check_is_my_origin_node(origin_sysid, origin_tli, origin_datid))
		return;

	/* FIXME: check db */

	bdr_locks_find_my_database(false);

	initStringInfo(&s);

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
		elog(DEBUG1, "found ddl lock entry to delete in response to ddl lock release message");
		simple_heap_delete(rel, &tuple->t_self);
		ForceSyncCommit(); /* async commit would be too complicated */
		found = true;
	}

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, NoLock);
	CommitTransactionCommand();

	if (!found)
		elog(WARNING, "did not find local DDL lock entry about a remotely released lock");

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	if (bdr_my_locks_database->lockcount > 0)
	{
		bdr_my_locks_database->lockcount--;
		bdr_my_locks_database->lock_holder = InvalidRepNodeId;
	}
	else
		elog(WARNING, "releasing DDL lock without corresponding in-memory state");

	latch = bdr_my_locks_database->waiting_latch;
	LWLockRelease(bdr_locks_ctl->lock);

	/* notify an eventual waiter */
	if(latch)
		SetLatch(latch);
}

/*
 * Another node has confirmed a lock. Changed shared memory state and wakeup
 * the locker.
 */
void
bdr_process_confirm_ddl_lock(uint64 origin_sysid, TimeLineID origin_tli, Oid origin_datid,
							 uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid)
{
	Latch *latch;

	if (!check_is_my_origin_node(origin_sysid, origin_tli, origin_datid))
		return;

	/* don't care if another database has gotten the lock */
	if (!check_is_my_node(lock_sysid, lock_tli, lock_datid))
		return;

	bdr_locks_find_my_database(false);

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	bdr_my_locks_database->acquire_confirmed++;
	latch = bdr_my_locks_database->waiting_latch;

	elog(DEBUG1, "received ddl lock confirmation number %d/%zu",
		 bdr_my_locks_database->acquire_confirmed, bdr_my_locks_database->nnodes);
	LWLockRelease(bdr_locks_ctl->lock);

	if(latch)
		SetLatch(latch);
}

/*
 * Another node has declined a lock. Changed shared memory state and wakeup
 * the locker.
 */
void
bdr_process_decline_ddl_lock(uint64 origin_sysid, TimeLineID origin_tli, Oid origin_datid,
							 uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid)
{
	Latch *latch;

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
}

void
bdr_process_request_replay_confirm(uint64 sysid, TimeLineID tli,
								   Oid datid, XLogRecPtr request_lsn)
{
	XLogRecPtr lsn;
	StringInfoData s;

	if (!check_is_my_origin_node(sysid, tli, datid))
		return;

	bdr_locks_find_my_database(false);

	initStringInfo(&s);
	bdr_prepare_message(&s, BDR_MESSAGE_REPLAY_CONFIRM);
	pq_sendint64(&s, request_lsn);
	lsn = LogStandbyMessage(s.data, s.len, false);
	XLogFlush(lsn);
}

void
bdr_process_replay_confirm(uint64 sysid, TimeLineID tli,
						   Oid datid, XLogRecPtr request_lsn)
{
	bool quorum_reached = false;

	if (!check_is_my_origin_node(sysid, tli, datid))
		return;

	bdr_locks_find_my_database(false);

	LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
	elog(DEBUG1, "processing replay confirmation for request %X/%X at %X/%X",
		 (uint32)(bdr_my_locks_database->replay_confirmed_lsn >> 32),
		 (uint32)bdr_my_locks_database->replay_confirmed_lsn,
		 (uint32)(request_lsn >> 32),
		 (uint32)request_lsn);

	/* request matches the one we're interested in */
	if (bdr_my_locks_database->replay_confirmed_lsn == request_lsn)
	{
		bdr_my_locks_database->replay_confirmed++;

		elog(DEBUG1, "confirming replay %u/%zu",
			 bdr_my_locks_database->replay_confirmed,
			 bdr_my_locks_database->nnodes);

		quorum_reached =
			bdr_my_locks_database->replay_confirmed >= bdr_my_locks_database->nnodes;
	}
	LWLockRelease(bdr_locks_ctl->lock);

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

		/* clear out information about requested confirmations */
		LWLockAcquire(bdr_locks_ctl->lock, LW_EXCLUSIVE);
		bdr_my_locks_database->replay_confirmed = 0;
		bdr_my_locks_database->replay_confirmed_lsn = InvalidXLogRecPtr;
		bdr_my_locks_database->waiting_latch = NULL;
		LWLockRelease(bdr_locks_ctl->lock);

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
				elog(PANIC, "duplicate lock?");

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
			elog(PANIC, "unknown lock");

		systable_endscan(scan);
		UnregisterSnapshot(snap);
		heap_close(rel, NoLock);

		CommitTransactionCommand();

		elog(DEBUG1, "sending confirmation for DDL lock replay confirmation request");
	}
}

void
bdr_locks_always_allow_writes(bool always_allow)
{
	Assert(IsUnderPostmaster);
	bdr_always_allow_writes = always_allow;
}

void
bdr_locks_process_remote_startup(uint64 sysid, TimeLineID tli, Oid datid)
{
	Relation rel;
	Snapshot snap;
	SysScanDesc scan;
	HeapTuple tuple;
	StringInfoData s;

	bdr_locks_find_my_database(false);

	initStringInfo(&s);

	StartTransactionCommand();
	snap = RegisterSnapshot(GetLatestSnapshot());
	rel = heap_open(BdrLocksRelid, RowExclusiveLock);

	scan = locks_begin_scan(rel, snap, sysid, tli, datid);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		elog(DEBUG1, "found remote lock to delete (after remote restart)");

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

static void
BdrExecutorStart(QueryDesc *queryDesc, int eflags)
{
	bool performs_writes = false;

	if (bdr_always_allow_writes || !bdr_is_bdr_activated_db())
		goto done;

	/* identify whether this is a modifying statement */
	if (queryDesc->plannedstmt != NULL &&
		queryDesc->plannedstmt->hasModifyingCTE)
		performs_writes = true;
	else if (queryDesc->operation != CMD_SELECT)
		performs_writes = true;

	if (!performs_writes)
		goto done;

	bdr_locks_find_my_database(false);

	/* is the database still starting up and hasn't loaded locks */
	if (!bdr_my_locks_database->locked_and_loaded)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("database is not yet ready for writes"),
				 errhint("Wait for a short time and retry.")));

	/* Is this database locked against user initiated ddl? */
	pg_memory_barrier();
	if (bdr_my_locks_database->lockcount > 0 && !this_xact_acquired_lock)
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("database is locked against writes"),
				 errhint("Some node in the cluster is performing DDL")));

done:
	if (PrevExecutorStart_hook)
		(*PrevExecutorStart_hook) (queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}
