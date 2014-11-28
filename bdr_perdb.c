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

PG_FUNCTION_INFO_V1(bdr_start_perdb_worker);

Datum
bdr_start_perdb_worker(PG_FUNCTION_ARGS);

/* In the commit hook, should we attempt to start a per-db worker? */
static bool xacthook_connection_added = false;

/*
 * Offset of this perdb worker in shmem; must be retained so it can
 * be passed to apply workers.
 */
uint16 perdb_worker_idx = -1;

/* 
 * Scan shmem looking for a perdb worker for the named DB and
 * return its offset. If not found, return -1.
 *
 * Must hold the LWLock on the worker control segment in at
 * least share mode.
 */
int
find_perdb_worker_slot(const char *dbname, BdrWorker **worker_found)
{
	int i, found = -1;

	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *w = &BdrWorkerCtl->slots[i];
		if (w->worker_type == BDR_WORKER_PERDB)
		{
			BdrPerdbWorker *pw = &w->worker_data.perdb_worker;
			if (strcmp(NameStr(pw->dbname), dbname) == 0)
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
 * specified apply worker name and return its offset. If not found, return
 * -1.
 *
 * Must hold the LWLock on the worker control segment in at
 * least share mode.
 */
static int
find_apply_worker_slot(const char *worker_name, BdrWorker **worker_found)
{
	int i, found = -1;

	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *w = &BdrWorkerCtl->slots[i];
		if (w->worker_type == BDR_WORKER_APPLY)
		{
			BdrApplyWorker *aw = &w->worker_data.apply_worker;
			if (aw->perdb_worker_idx == perdb_worker_idx
				&& strcmp(NameStr(aw->conn_local_name), worker_name) == 0)
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
	/* This hook is only called from normal backends */
	Assert(!IsBackgroundWorker);
	/* ... so it's safe to use the dbname from the procport */
	Assert(MyProcPort->database_name != NULL);

	switch (event)
	{
		case XACT_EVENT_COMMIT:
			if (xacthook_connection_added)
			{
				int slotno;
				BdrWorker *w;

				xacthook_connection_added = false;

				LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

				/*
				 * If a perdb worker already exists, wake it and tell it to
				 * check for new connections.
				 */
				slotno = find_perdb_worker_slot(MyProcPort->database_name, &w);
				if (slotno >= 0)
					SetLatch(w->worker_data.perdb_worker.proclatch);
				else
				{
					/*
					 * Per-db worker doesn't exist, ask the supervisor to check for
					 * changes and register new per-db workers for labeled
					 * databases.
					 */
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
 */
Datum
bdr_start_perdb_worker(PG_FUNCTION_ARGS)
{
	/* XXX DYNCONF Check to make sure the security label exists and is valid? */

	/* If there's already a per-db worker for our DB we have nothing to do */
	if (!xacthook_connection_added)
	{
		RegisterXactCallback(bdr_perdb_xact_callback, NULL);
		xacthook_connection_added = true;
	}
	PG_RETURN_VOID();
}

/*
 * Launch a dynamic bgworker to run bdr_apply_main for each bdr connection on
 * the database identified by dbname.
 *
 * Scans the bdr.bdr_connections table for workers and launch a worker for any
 * connection that doesn't already have one.
 */
static List*
bdr_launch_apply_workers(char *dbname)
{
	List			   *apply_workers = NIL;
	BackgroundWorker	bgw;
	int					i, ret;
	Size				nnodes = 0;
	int					attno_conn_local_name;
#define BDR_CON_Q_NARGS 3
	Oid					argtypes[BDR_CON_Q_NARGS] = { OIDOID, OIDOID, OIDOID };
	Datum				values[BDR_CON_Q_NARGS];

	/* Should be called from the perdb worker */
	Assert(IsBackgroundWorker);
	Assert(perdb_worker_idx != -1);

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

	values[0] = ObjectIdGetDatum(GetSystemIdentifier());
	values[1] = ObjectIdGetDatum(ThisTimeLineID);
	values[2] = ObjectIdGetDatum(MyDatabaseId);

	/* Query for connections */
	SPI_connect();

	ret = SPI_execute_with_args("SELECT * FROM bdr.bdr_connections "
								"WHERE conn_sysid = $1 "
								"  AND conn_timeline = $2 "
								"  AND conn_dboid = $3 ",
								BDR_CON_Q_NARGS, argtypes, values, NULL,
								false, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI error while querying bdr.bdr_connections");

	attno_conn_local_name = SPI_fnumber(SPI_tuptable->tupdesc,
										"conn_local_name");

	if (attno_conn_local_name == SPI_ERROR_NOATTRIBUTE)
		elog(ERROR, "SPI error while reading conn_local_name from bdr.bdr_connections");

	nnodes = SPI_processed;

	for (i = 0; i < SPI_processed; i++)
	{
		BackgroundWorkerHandle *bgw_handle;
		HeapTuple				tuple;
		char				   *conn_local_name;
		uint32					slot;
		uint32					worker_arg;
		BdrWorker			   *worker;
		BdrApplyWorker		   *apply;
		MemoryContext			oldcontext;

		tuple = SPI_tuptable->vals[i];

		conn_local_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc,
									   attno_conn_local_name);

		Assert(!LWLockHeldByMe(BdrWorkerCtl->lock));
		LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

		/*
		 * Is there already a worker registered for this
		 * connection?
		 */
		if (find_apply_worker_slot(conn_local_name, NULL) != -1)
		{
			elog(DEBUG2, "Skipping registration of worker %s on db %s: already registered",
				 conn_local_name, dbname);
			LWLockRelease(BdrWorkerCtl->lock);
			continue;
		}

		/* Set the display name in 'ps' etc */
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 BDR_LOCALID_FORMAT": %s: apply",
				 BDR_LOCALID_FORMAT_ARGS, conn_local_name);

		/* Allocate a new shmem slot for this apply worker */
		worker = bdr_worker_shmem_alloc(BDR_WORKER_APPLY, &slot);

		/* Tell the apply worker what its shmem slot is */
		Assert(slot <= UINT16_MAX);
		worker_arg = (((uint32)BdrWorkerCtl->worker_generation) << 16) | (uint32)slot;
		bgw.bgw_main_arg = Int32GetDatum(worker_arg);

		/* Now populate the apply worker state */
		apply = &worker->worker_data.apply_worker;
		strncpy(NameStr(apply->conn_local_name), conn_local_name, NAMEDATALEN);
		NameStr(apply->conn_local_name)[NAMEDATALEN-1] = '\0';
		apply->replay_stop_lsn = InvalidXLogRecPtr;
		apply->forward_changesets = false;
		apply->perdb_worker_idx = perdb_worker_idx;

		LWLockRelease(BdrWorkerCtl->lock);

		/*
		 * Finally, register the worker for launch.
		 *
		 * TopMemoryContext is used so we can retain the handle
		 * after this SPI transaction finishes.
		 */
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		if (!RegisterDynamicBackgroundWorker(&bgw,
											 &bgw_handle))
		{
			/* XXX DYNCONF Should clean up already-registered workers? */
			ereport(ERROR,
					(errmsg("bdr: Failed to register background worker"
							" %s, see previous log messages",
							conn_local_name)));
		}
		apply_workers = lcons(bgw_handle, apply_workers);
		MemoryContextSwitchTo(oldcontext);
	}

	SPI_finish();

	CommitTransactionCommand();

	/*
	 * Now we need to tell the lock manager and the sequence
	 * manager about the changed node count.
	 *
	 * There's no truly safe way to do this without a proper
	 * part/join protocol, so all we're going to do is update
	 * the node count in shared memory.
	 */
	bdr_worker_slot->worker_data.perdb_worker.nnodes = nnodes;
	bdr_locks_set_nnodes(nnodes);
	bdr_sequencer_set_nnodes(nnodes);

	return apply_workers;
}

/* XXX DYNCONF */
static void
bdr_copy_connections_from_config(void);

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
	int				  rc = 0;
	List			 *apply_workers;
	ListCell		 *c;
	BdrPerdbWorker   *bdr_perdb_worker;
	StringInfoData	  si;
	bool			  wait;
	uint32			  worker_arg;
	uint16			  worker_generation;

	initStringInfo(&si);

	Assert(IsBackgroundWorker);

	worker_arg = DatumGetInt32(main_arg);

	worker_generation = (uint16)(worker_arg >> 16);
	perdb_worker_idx = (uint16)(worker_arg & 0x0000FFFF);

	if (worker_generation != BdrWorkerCtl->worker_generation)
	{
		elog(DEBUG1, "perdb worker from generation %d exiting after finding shmem generation is %d",
			 worker_generation, BdrWorkerCtl->worker_generation);
		proc_exit(0);
	}

	bdr_worker_slot = &BdrWorkerCtl->slots[perdb_worker_idx];
	Assert(bdr_worker_slot->worker_type == BDR_WORKER_PERDB);
	bdr_perdb_worker = &bdr_worker_slot->worker_data.perdb_worker;
	bdr_worker_type = BDR_WORKER_PERDB;

	bdr_worker_init(NameStr(bdr_perdb_worker->dbname));

	bdr_perdb_worker->nnodes = 0;

	elog(DEBUG1, "per-db worker for node " BDR_LOCALID_FORMAT " starting", BDR_LOCALID_FORMAT_ARGS);

	appendStringInfo(&si, BDR_LOCALID_FORMAT": %s", BDR_LOCALID_FORMAT_ARGS, "perdb worker");
	SetConfigOption("application_name", si.data, PGC_USERSET, PGC_S_SESSION);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	/* need to be able to perform writes ourselves */
	bdr_executor_always_allow_writes(true);
	bdr_locks_startup();

	bdr_copy_connections_from_config(); /* XXX DYNCONF */

	/*
	 * Do we need to init the local DB from a remote node?
	 *
	 * Checks bdr.bdr_nodes.status, does any remote initialization required if
	 * there's an init_replica connection, and ensures that
	 * bdr.bdr_nodes.status=r for our entry before continuing.
	 */
	bdr_init_replica(&bdr_perdb_worker->dbname);

	elog(DEBUG1, "Starting bdr apply workers for db %s", NameStr(bdr_perdb_worker->dbname));

	/* Launch the apply workers */
	apply_workers = bdr_launch_apply_workers(NameStr(bdr_perdb_worker->dbname));

	/*
	 * For now, just free the bgworker handles. Later we'll probably want them
	 * for adding/removing/reconfiguring bgworkers.
	 */
	foreach(c, apply_workers)
	{
		BackgroundWorkerHandle *h = (BackgroundWorkerHandle *) lfirst(c);
		pfree(h);
	}

#ifdef BUILDING_BDR
	elog(DEBUG1, "BDR starting sequencer on db \"%s\"",
		 NameStr(bdr_perdb_worker->dbname));

	/* initialize sequencer */
	bdr_sequencer_init(bdr_perdb_worker->seq_slot, bdr_perdb_worker->nnodes);
#endif
	bdr_perdb_worker->proclatch = &MyProc->procLatch;
	bdr_perdb_worker->database_oid = MyDatabaseId;

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
				bdr_launch_apply_workers(NameStr(bdr_perdb_worker->dbname));
			}
		}
	}

	bdr_perdb_worker->database_oid = InvalidOid;
	proc_exit(0);
}

/*
 * XXX DYNCONF throwaway code
 *
 * Read the configured connections from the config file and copy them into
 * bdr.bdr_connections.
 */
static void
bdr_copy_connections_from_config()
{
#define BDR_CONNECTIONS_NATTS 10
	int   i, ret;
	char *my_dbname;
	bool save_unsafe;

	save_unsafe = bdr_permit_unsafe_commands;

	StartTransactionCommand();

	my_dbname = get_database_name(MyDatabaseId);

	SPI_connect();

	bdr_permit_unsafe_commands = true;

	ret = SPI_exec("DELETE FROM bdr.bdr_connections WHERE from_config_file;", 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "Couldn't clean out bdr.bdr_connections");

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrConnectionConfig *cfg = bdr_connection_configs[i];
		NameData conname, replication_name;
		char  nulls[BDR_CONNECTIONS_NATTS];
		Datum values[BDR_CONNECTIONS_NATTS];
		Oid   argtypes[BDR_CONNECTIONS_NATTS] = {OIDOID, OIDOID, OIDOID, NAMEOID, NAMEOID, TEXTOID, BOOLOID, TEXTOID, INT4OID, TEXTARRAYOID};

		if (cfg == NULL)
			continue;

		if (strcmp(cfg->dbname, my_dbname) != 0)
			continue;

		memset(values, 0, sizeof(values));
		strncpy(nulls, "         ", BDR_CONNECTIONS_NATTS);

		values[0] = ObjectIdGetDatum(GetSystemIdentifier());
		values[1] = ObjectIdGetDatum(ThisTimeLineID);
		values[2] = ObjectIdGetDatum(MyDatabaseId);
		/* local conn name */
		strncpy(NameStr(conname), cfg->name, NAMEDATALEN);
		NameStr(conname)[NAMEDATALEN-1] = '\0';
		values[3] = NameGetDatum(&conname);
		/* replication name is unused and must be the empty string */
		NameStr(replication_name)[0] = '\0';
		values[4] = NameGetDatum(&replication_name);
		/* DSN to connect to */
		values[5] = CStringGetTextDatum(cfg->dsn);
		values[6] = BoolGetDatum(cfg->init_replica);
		if (cfg->init_replica)
			values[7] = CStringGetTextDatum(cfg->replica_local_dsn);
		else
			nulls[7] = 'n';
		if (cfg->apply_delay >= 0)
			values[8] = Int32GetDatum(cfg->apply_delay);
		else
			nulls[8] = 'n';

		if (cfg->replication_sets != NULL)
		{
			/*
			 * cfg->replication_sets is a comma separated list of quoted identifiers, which is what
			 * the output plugin expects to receive. For database storage we'd rather store an array
			 * of identifiers, so we need to split them up.
			 *
			 * We leak a bunch here, but it's throwaway code inside a transaction memory context,
			 * so who cares.
			 */
			List *namelist;

			if (!SplitIdentifierString(pstrdup(cfg->replication_sets),
									   ',', &namelist))
				elog(ERROR, "Whoops, couldn't parse replication identifier list");

			if (namelist == NIL)
				nulls[9] = 'n';
			else 
			{
				int len = 0;
				Datum *idents = palloc(list_length(namelist) * sizeof(Datum));
				ListCell *c;

				foreach(c, namelist)
					idents[len++] = CStringGetTextDatum(lfirst(c));

				values[9] = (Datum)construct_array(idents, len, TEXTOID, -1, false, 'i');
			}
		}
		else
			nulls[9] = 'n';

		ret = SPI_execute_with_args(
					"INSERT INTO bdr.bdr_connections "
					"(conn_sysid, conn_timeline, conn_dboid, conn_local_name, conn_replication_name, conn_dsn, conn_init_replica, conn_replica_local_dsn, conn_apply_delay, conn_replication_sets, from_config_file) "
					"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::text[], 't')",
					BDR_CONNECTIONS_NATTS, argtypes, values, nulls, false, 0
					);

		if (ret != SPI_OK_INSERT)
			elog(ERROR, "Failed to copy record into bdr.bdr_connections");
	}

	SPI_finish();
	pfree(my_dbname);
	CommitTransactionCommand();

	bdr_permit_unsafe_commands = save_unsafe;
}
