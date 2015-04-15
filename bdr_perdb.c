/* -------------------------------------------------------------------------
 *
 * bdr_perdb.c
 *		Per database supervisor worker.
 *
 * Copyright (C) 2014-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_perdb.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"
#include "bdr_locks.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"

#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "executor/spi.h"

#include "postmaster/bgworker.h"

#include "lib/stringinfo.h"

/* For struct Port only! */
#include "libpq/libpq-be.h"

#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/ipc.h"

#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(bdr_connections_changed);

/* In the commit hook, should we attempt to start a per-db worker? */
static bool xacthook_registered = false;
static bool xacthook_connections_changed = false;

/*
 * Scan shmem looking for a perdb worker for the named DB and
 * return its offset. If not found, return -1.
 *
 * Must hold the LWLock on the worker control segment in at
 * least share mode.
 *
 * Note that there's no guarantee that the worker is actually
 * started up.
 */
int
find_perdb_worker_slot(Oid dboid, BdrWorker **worker_found)
{
	int i, found = -1;

	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *w = &BdrWorkerCtl->slots[i];
		if (w->worker_type == BDR_WORKER_PERDB)
		{
			BdrPerdbWorker *pw = &w->data.perdb;
			if (pw->database_oid == dboid)
			{
				found = i;
				if (worker_found != NULL)
					*worker_found = w;
				break;
			}
		}
	}

	return found;
}

/*
 * Scan shmem looking for an apply worker for the current perdb worker and
 * specified target node identifier and return its offset. If not found, return
 * -1.
 *
 * Must hold the LWLock on the worker control segment in at least share mode.
 *
 * Note that there's no guarantee that the worker is actually started up.
 */
static int
find_apply_worker_slot(uint64 sysid, TimeLineID timeline, Oid dboid, BdrWorker **worker_found)
{
	int i, found = -1;

	Assert(bdr_worker_type == BDR_WORKER_PERDB);
	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *w = &BdrWorkerCtl->slots[i];
		if (w->worker_type == BDR_WORKER_APPLY)
		{
			BdrApplyWorker *aw = &w->data.apply;
			if (aw->dboid == MyDatabaseId
				&& aw->remote_sysid == sysid
				&& aw->remote_timeline == timeline
				&& aw->remote_dboid == dboid)
			{
				found = i;
				if (worker_found != NULL)
					*worker_found = w;
				break;
			}
		}
	}

	return found;
}

static void
bdr_perdb_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
			if (xacthook_connections_changed)
			{
				int slotno;
				BdrWorker *w;

				xacthook_connections_changed = false;

				LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

				/*
				 * If a perdb worker already exists, wake it and tell it to
				 * check for new connections.
				 */
				slotno = find_perdb_worker_slot(MyDatabaseId, &w);
				if (slotno >= 0)
				{
					/*
					 * The worker is registered, but might not be started yet
					 * (or could be crashing and restarting). If it's not
					 * started the latch will be zero. If it's started but
					 * dead, the latch will be bogus, but it's safe to set a
					 * proclatch to a dead process. At worst we'll set a latch
					 * for the wrong process, and that's fine. If it's zero
					 * then the worker is still starting and will see our new
					 * changes anyway.
					 */
					if (w->data.perdb.proclatch != NULL)
						SetLatch(w->data.perdb.proclatch);
				}
				else
				{
					/*
					 * Per-db worker doesn't exist, ask the supervisor to check for
					 * changes and register new per-db workers for labeled
					 * databases.
					 */
					if (BdrWorkerCtl->supervisor_latch)
						SetLatch(BdrWorkerCtl->supervisor_latch);
				}

				LWLockRelease(BdrWorkerCtl->lock);
			}
			break;
		default:
			/* We're not interested in other tx events */
			break;
	}
}

/*
 * Prepare to launch a perdb worker for the current DB if it's not already
 * running, and register a XACT_EVENT_COMMIT hook to perform the actual launch
 * when the addition of the worker commits.
 *
 * If a perdb worker is already running, notify it to check for new
 * connections.
 */
Datum
bdr_connections_changed(PG_FUNCTION_ARGS)
{
	if (!xacthook_registered)
	{
		RegisterXactCallback(bdr_perdb_xact_callback, NULL);
		xacthook_registered = true;
	}
	xacthook_connections_changed = true;
	PG_RETURN_VOID();
}

static int
getattno(const char *colname)
{
	int attno;

	attno = SPI_fnumber(SPI_tuptable->tupdesc, colname);
	if (attno == SPI_ERROR_NOATTRIBUTE)
		elog(ERROR, "SPI error while reading %s from bdr.bdr_connections", colname);

	return attno;
}

/*
 * Launch a dynamic bgworker to run bdr_apply_main for each bdr connection on
 * the database identified by dbname.
 *
 * Scans the bdr.bdr_connections table for workers and launch a worker for any
 * connection that doesn't already have one.
 */
void
bdr_maintain_db_workers(void)
{
	BackgroundWorker	bgw;
	int					i, ret;
	Size				nnodes = 0;
#define BDR_CON_Q_NARGS 3
	Oid					argtypes[BDR_CON_Q_NARGS] = { TEXTOID, OIDOID, OIDOID };
	Datum				values[BDR_CON_Q_NARGS];
	char				sysid_str[33];
	char				our_status;

	/* Should be called from the perdb worker */
	Assert(IsBackgroundWorker);
	Assert(bdr_worker_type == BDR_WORKER_PERDB);

	Assert(!LWLockHeldByMe(BdrWorkerCtl->lock));


	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, GetSystemIdentifier());
	sysid_str[sizeof(sysid_str)-1] = '\0';

	elog(DEBUG2, "launching apply workers");

	/*
	 * It's easy enough to make this tolerant of an open tx, but in general
	 * rollback doesn't make sense here.
	 */
	Assert(!IsTransactionState());

	/* Common apply worker values */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	strncpy(bgw.bgw_library_name, BDR_LIBRARY_NAME, BGW_MAXLEN);
	strncpy(bgw.bgw_function_name, "bdr_apply_main", BGW_MAXLEN);
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;

	StartTransactionCommand();

	SPI_connect();

	PushActiveSnapshot(GetTransactionSnapshot());

	our_status = bdr_nodes_get_local_status(
		GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId);

	/*
	 * First check whether any existing processes to/from this database need
	 * to be killed of because of the node status.
	 */
	ret = SPI_execute(
		"SELECT node_sysid, node_timeline, node_dboid\n"
		"FROM bdr.bdr_nodes\n"
		"WHERE bdr_nodes.node_status = 'k'",
		false, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI error while querying bdr.bdr_nodes");

	for (i = 0; i < SPI_processed; i++)
	{
		/*
		 * If the connection is dead, iterate over all shem slots and kill
		 * everything using that slot.
		 */
		HeapTuple	tuple;
		int			slotoff;
		bool		found_alive = false;
		Oid			node_datoid;
		uint64		node_sysid;
		char	   *node_sysid_s;
		TimeLineID	node_timeline;

		bool		isnull;

		tuple = SPI_tuptable->vals[i];

		node_sysid_s = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);

		if (sscanf(node_sysid_s, UINT64_FORMAT, &node_sysid) != 1)
			elog(ERROR, "Parsing sysid uint64 from %s failed", node_sysid_s);

		node_timeline = DatumGetObjectId(
			SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2,
						  &isnull));
		Assert(!isnull);

		node_datoid = DatumGetObjectId(
			SPI_getbinval(tuple, SPI_tuptable->tupdesc, 3,
						  &isnull));
		Assert(!isnull);

		LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

		for (slotoff = 0; slotoff < bdr_max_workers; slotoff++)
		{
			BdrWorker  *w = &BdrWorkerCtl->slots[slotoff];
			bool		kill_proc = false;

			/* unused slot */
			if (w->worker_type == BDR_WORKER_EMPTY_SLOT)
				continue;

			/* unconnected slot */
			if (w->worker_proc == NULL)
				continue;

			if (w->worker_type == BDR_WORKER_APPLY)
			{
				BdrApplyWorker *apply = &w->data.apply;

				/*
				 * Kill apply workers either if they're running on the
				 * to-be-killed node or connecting to it.
				 */

				if (our_status == 'k' && w->worker_proc->databaseId == node_datoid)
				{
					/*
					 * NB: It's sufficient to check the database oid, the
					 * others have to be the same
					 */
					kill_proc = true;
				}
				else if (apply->remote_sysid == node_sysid &&
						 apply->remote_timeline == node_timeline &&
						 apply->remote_dboid == node_datoid)
				{
					kill_proc = true;
				}
			}
			else if (w->worker_type == BDR_WORKER_WALSENDER)
			{
				BdrWalsenderWorker *walsnd = &w->data.walsnd;

				if (our_status == 'k' && w->worker_proc->databaseId == node_datoid)
					kill_proc = true;
				else if (walsnd->remote_sysid == node_sysid &&
						 walsnd->remote_timeline == node_timeline &&
						 walsnd->remote_dboid == node_datoid)
				{
					kill_proc = true;
				}
			}

			if (kill_proc)
			{
				found_alive = true;

				elog(LOG, "need to kill node: %u type: %u",
					 w->worker_pid, w->worker_type);
				kill(w->worker_pid, SIGTERM);
			}
		}

		if (found_alive)
		{
			/* check again next time round, soon please */
			SetLatch(&MyProc->procLatch);
		}
		else
		{
			/* Drop slots of dead node */
			elog(LOG, "need to drop slots for remote (a,b,c)");
		}

		LWLockRelease(BdrWorkerCtl->lock);
		continue;
	}

	/* If our own node is dead, don't start new connections to other nodes */
	if (our_status == 'k')
	{
		elog(LOG, "FIXME: skipping");
		goto out;
	}

	/*
	 * Look up connection entries for all nodes other than our own.
	 *
	 * If an entry with our origin (sysid,tlid,dboid) exists, treat that as
	 * overriding the generic one.
	 */
	values[0] = CStringGetTextDatum(sysid_str);
	values[1] = ObjectIdGetDatum(ThisTimeLineID);
	values[2] = ObjectIdGetDatum(MyDatabaseId);

	ret = SPI_execute_with_args(
			"SELECT DISTINCT ON (conn_sysid, conn_timeline, conn_dboid) "
			"  conn_sysid, conn_timeline, conn_dboid, "
			"  conn_is_unidirectional, "
			"  conn_origin_dboid <> 0 AS origin_is_my_id, "
			"  node_status "
			"FROM bdr.bdr_connections "
			"    JOIN bdr.bdr_nodes ON ("
			"          conn_sysid = node_sysid AND "
			"          conn_timeline = node_timeline AND "
			"          conn_dboid = node_dboid "
			"    )"
			"WHERE ( "
			"         (conn_origin_sysid = '0' AND "
			"          conn_origin_timeline = 0 AND "
			"          conn_origin_dboid = 0) "
			"         OR "
			"         (conn_origin_sysid = $1 AND "
			"          conn_origin_timeline = $2 AND "
			"          conn_origin_dboid = $3) "
			"      ) AND NOT ( "
			"          conn_sysid = $1 AND "
			"          conn_timeline = $2 AND "
			"          conn_dboid = $3"
			"      ) "
			"ORDER BY conn_sysid, conn_timeline, conn_dboid, "
			"         conn_origin_sysid ASC NULLS LAST, "
			"         conn_timeline ASC NULLS LAST, "
			"         conn_dboid ASC NULLS LAST ",
		BDR_CON_Q_NARGS, argtypes, values, NULL,
		false, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI error while querying bdr.bdr_connections");

	for (i = 0; i < SPI_processed; i++)
	{
		BackgroundWorkerHandle *bgw_handle;
		HeapTuple				tuple;
		uint32					slot;
		uint32					worker_arg;
		BdrWorker			   *worker;
		BdrApplyWorker		   *apply;
		Datum					temp_datum;
		bool					isnull;
		uint64					target_sysid;
		TimeLineID				target_timeline;
		Oid						target_dboid;
		char*					tmp_sysid;
		bool					origin_is_my_id,
								conn_is_unidirectional;
		char					node_status;

		tuple = SPI_tuptable->vals[i];

		tmp_sysid = SPI_getvalue(tuple, SPI_tuptable->tupdesc,
								 getattno("conn_sysid"));

		if (sscanf(tmp_sysid, UINT64_FORMAT, &target_sysid) != 1)
			elog(ERROR, "Parsing sysid uint64 from %s failed", tmp_sysid);

		temp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								   getattno("conn_timeline"),
								   &isnull);
		Assert(!isnull);
		target_timeline = DatumGetObjectId(temp_datum);

		temp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								   getattno("conn_dboid"),
								   &isnull);
		Assert(!isnull);
		target_dboid = DatumGetObjectId(temp_datum);

		temp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								   getattno("conn_is_unidirectional"),
								   &isnull);
		Assert(!isnull);
		conn_is_unidirectional = DatumGetBool(temp_datum);

		temp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								   getattno("origin_is_my_id"),
								   &isnull);
		Assert(!isnull);
		origin_is_my_id = DatumGetBool(temp_datum);

		temp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								   getattno("node_status"),
								   &isnull);
		Assert(!isnull);
		node_status = DatumGetChar(temp_datum);

		elog(DEBUG1, "Found bdr_connections entry for "BDR_LOCALID_FORMAT" (origin specific: %d, unidirectional: %d, status: %c)",
			 target_sysid, target_timeline, target_dboid,
			 EMPTY_REPLICATION_NAME,
			 (int) origin_is_my_id, (int) conn_is_unidirectional, node_status);

		if(node_status == 'k')
		{
			elog(LOG, "skip registration as killed");
			continue;
		}

		nnodes++;

		LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

		/*
		 * Is there already a worker registered for this connection?
		 *
		 * TODO DYNCONF Each apply worker should have its latch set and respond
		 * by checking to see whether it needs to apply any new configuration.
		 */
		if (find_apply_worker_slot(target_sysid, target_timeline, target_dboid, NULL) != -1)
		{
			elog(DEBUG2, "Skipping registration of worker for node "BDR_LOCALID_FORMAT" on db oid=%u: already registered",
				 target_sysid, target_timeline, target_dboid,
				 EMPTY_REPLICATION_NAME, MyDatabaseId);
			LWLockRelease(BdrWorkerCtl->lock);
			continue;
		}

		/* Set the display name in 'ps' etc */
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 BDR_LOCALID_FORMAT"->"BDR_LOCALID_FORMAT,
				 BDR_LOCALID_FORMAT_ARGS,
				 target_sysid, target_timeline, target_dboid,
				 EMPTY_REPLICATION_NAME);

		/* Allocate a new shmem slot for this apply worker */
		worker = bdr_worker_shmem_alloc(BDR_WORKER_APPLY, &slot);

		/* Tell the apply worker what its shmem slot is */
		Assert(slot <= UINT16_MAX);
		worker_arg = (((uint32)BdrWorkerCtl->worker_generation) << 16) | (uint32)slot;
		bgw.bgw_main_arg = Int32GetDatum(worker_arg);

		/*
		 * Apply workers (other than in catchup mode, which are registered
		 * elsewhere) should not be using the local node's connection entry.
		 */
		Assert(!(target_sysid == GetSystemIdentifier() &&
				 target_timeline == ThisTimeLineID &&
				 target_dboid == MyDatabaseId));

		/* Now populate the apply worker state */
		apply = &worker->data.apply;
		apply->dboid = MyDatabaseId;
		apply->remote_sysid = target_sysid;
		apply->remote_timeline = target_timeline;
		apply->remote_dboid = target_dboid;
		apply->replay_stop_lsn = InvalidXLogRecPtr;
		apply->forward_changesets = false;
		apply->perdb = bdr_worker_slot;
		LWLockRelease(BdrWorkerCtl->lock);

		/*
		 * Finally, register the worker for launch.
		 */
		if (!RegisterDynamicBackgroundWorker(&bgw,
											 &bgw_handle))
		{
			/*
			 * Already-registered workers will keep on running.  We need to
			 * make sure the slot we just acquired but failed to launch a
			 * worker for gets released again though.
			 */
			LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);
			apply->dboid = InvalidOid;
			apply->remote_sysid = 0;
			apply->remote_timeline = 0;
			apply->remote_dboid = InvalidOid;
			worker->worker_type = BDR_WORKER_EMPTY_SLOT;
			LWLockRelease(BdrWorkerCtl->lock);

			ereport(ERROR,
					(errmsg("bdr: Failed to register background worker"
							" for "BDR_LOCALID_FORMAT", see previous log messages",
							BDR_LOCALID_FORMAT_ARGS)));
		}
		else
		{
			elog(DEBUG2, "registered apply worker for "BDR_LOCALID_FORMAT,
				 target_sysid, target_timeline, target_dboid,
				 EMPTY_REPLICATION_NAME);
		}
	}

out:
	PopActiveSnapshot();
	SPI_finish();

	CommitTransactionCommand();

	elog(DEBUG2, "done registering apply workers");

	/*
	 * Now we need to tell the lock manager and the sequence
	 * manager about the changed node count.
	 *
	 * There's no truly safe way to do this without a proper
	 * part/join protocol, so all we're going to do is update
	 * the node count in shared memory.
	 */
	bdr_worker_slot->data.perdb.nnodes = nnodes;
#ifdef BUILDING_BDR
	bdr_locks_set_nnodes(nnodes);
	bdr_sequencer_set_nnodes(nnodes);
#endif

	elog(DEBUG2, "updated worker counts");
}

/*
 * Each database with BDR enabled on it has a static background worker,
 * registered at shared_preload_libraries time during postmaster start. This is
 * the entry point for these bgworkers.
 *
 * This worker handles BDR startup on the database and launches apply workers
 * for each BDR connection.
 *
 * Since the worker is fork()ed from the postmaster, all globals initialised in
 * _PG_init remain valid.
 *
 * This worker can use the SPI and shared memory.
 */
void
bdr_perdb_worker_main(Datum main_arg)
{
	int					rc = 0;
	BdrPerdbWorker		*perdb;
	StringInfoData		si;
	bool				wait;

	initStringInfo(&si);

	bdr_bgworker_init(DatumGetInt32(main_arg), BDR_WORKER_PERDB);

	perdb = &bdr_worker_slot->data.perdb;

	perdb->nnodes = 0;

	elog(DEBUG1, "per-db worker for node " BDR_LOCALID_FORMAT " starting", BDR_LOCALID_FORMAT_ARGS);

	appendStringInfo(&si, BDR_LOCALID_FORMAT": %s", BDR_LOCALID_FORMAT_ARGS, "perdb");
	SetConfigOption("application_name", si.data, PGC_USERSET, PGC_S_SESSION);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	/*
	 * It's necessary to acquire a a lock here so that a concurrent
	 * bdr_perdb_xact_callback can't try to set our latch at the same
	 * time as we write to it.
	 *
	 * There's no per-worker lock, so we just take the lock on the
	 * whole segment.
	 */
	LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);
	perdb->proclatch = &MyProc->procLatch;
	perdb->database_oid = MyDatabaseId;
	LWLockRelease(BdrWorkerCtl->lock);

	/* need to be able to perform writes ourselves */
	bdr_executor_always_allow_writes(true);
	bdr_locks_startup();

	{
		int				spi_ret;
		MemoryContext	saved_ctx;
		BDRNodeInfo	   *local_node;

		/*
		 * Check the local bdr.bdr_nodes table to see if there's an entry for
		 * our node.
		 *
		 * Note that we don't have to explicitly SPI_finish(...) on error paths;
		 * that's taken care of for us.
		 */
		StartTransactionCommand();
		spi_ret = SPI_connect();
		if (spi_ret != SPI_OK_CONNECT)
			elog(ERROR, "SPI already connected; this shouldn't be possible");

		saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
		local_node = bdr_nodes_get_local_info(GetSystemIdentifier(), ThisTimeLineID,
										  MyDatabaseId);
		MemoryContextSwitchTo(saved_ctx);

		if (local_node == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("local node record not found")));

		SPI_finish();
		CommitTransactionCommand();

		/*
		 * Do we need to init the local DB from a remote node?
		 */
		if (local_node->status != 'r' /* initialized */
			&& local_node->status != 'k' /* kill */)
			bdr_init_replica(local_node);

		bdr_bdr_node_free(local_node);
	}

	elog(DEBUG1, "Starting bdr apply workers for "BDR_LOCALID_FORMAT" (%s)",
		 BDR_LOCALID_FORMAT_ARGS, NameStr(perdb->dbname));

	/* Launch the apply workers */
	bdr_maintain_db_workers();

#ifdef BUILDING_BDR
	elog(DEBUG1, "BDR starting sequencer on db \"%s\"",
		 NameStr(perdb->dbname));

	/* initialize sequencer */
	bdr_sequencer_init(perdb->seq_slot, perdb->nnodes);
#endif

	while (!got_SIGTERM)
	{
		wait = true;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

#ifdef BUILDING_BDR
		/* check whether we need to start new elections */
		if (bdr_sequencer_start_elections())
			wait = false;

		/* check whether we need to vote */
		if (bdr_sequencer_vote())
			wait = false;

		/* check whether any of our elections needs to be tallied */
		bdr_sequencer_tally();

		/* check all bdr sequences for used up chunks */
		bdr_sequencer_fill_sequences();
#endif

		pgstat_report_activity(STATE_IDLE, NULL);

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 *
		 * We wake up everytime our latch gets set or if 180 seconds have
		 * passed without events. That's a stopgap for the case a backend
		 * committed sequencer changes but died before setting the latch.
		 */
		if (wait)
		{
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   180000L);

			ResetLatch(&MyProc->procLatch);

			/* emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);

			if (rc & WL_LATCH_SET)
			{
				/*
				 * If the perdb worker's latch is set we're being asked
				 * to rescan and launch new apply workers.
				 */
				bdr_maintain_db_workers();
			}
		}
	}

	perdb->database_oid = InvalidOid;
	proc_exit(0);
}
