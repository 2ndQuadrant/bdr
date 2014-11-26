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
static bool start_perdb_worker = false;

/*
 * Offset of this perdb worker in shmem; must be retained so it can
 * be passed to apply workers.
 */
int perdb_worker_idx = -1;

static void
bdr_perdb_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
			if (start_perdb_worker)
			{
				start_perdb_worker = false;
				bdr_register_perdb_worker(get_database_name(MyDatabaseId));
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
	if (!bdr_is_bdr_activated_db())
	{
		start_perdb_worker = true;
		RegisterXactCallback(bdr_perdb_xact_callback, NULL);
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
	int					attno_conn_local_name;
#define BDR_CON_Q_NARGS 3
	Oid					argtypes[BDR_CON_Q_NARGS] = { OIDOID, OIDOID, OIDOID };
	Datum				values[BDR_CON_Q_NARGS];

	Assert(IsBackgroundWorker);

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
								true, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI error while querying bdr.bdr_connections");

	attno_conn_local_name = SPI_fnumber(SPI_tuptable->tupdesc,
										"conn_local_name");

	if (attno_conn_local_name == SPI_ERROR_NOATTRIBUTE)
		elog(ERROR, "SPI error while reading conn_local_name from bdr.bdr_connections");

	/* XXX DYNCONF prevent repeat launch */

	for (i = 0; i < SPI_processed; i++)
	{
		BackgroundWorkerHandle *bgw_handle;
		HeapTuple				tuple;
		char				   *conn_local_name;
		unsigned int			slot;
		BdrWorker			   *worker;
		BdrApplyWorker		   *apply;

		tuple = SPI_tuptable->vals[i];

		conn_local_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc,
									   attno_conn_local_name);

		/* Set the display name in 'ps' etc */
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 BDR_LOCALID_FORMAT": %s: apply",
				 BDR_LOCALID_FORMAT_ARGS, conn_local_name);

		/* Allocate a new shmem slot for this apply worker */
		LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);
		worker = bdr_worker_shmem_alloc(BDR_WORKER_APPLY, &slot);
		LWLockRelease(BdrWorkerCtl->lock);

		/* Tell the apply worker what its shmem slot is */
		bgw.bgw_main_arg = Int32GetDatum(slot);

		/* Now populate the apply worker state */
		apply = &worker->worker_data.apply_worker;
		strncpy(NameStr(apply->conn_local_name), conn_local_name, NAMEDATALEN);
		NameStr(apply->conn_local_name)[NAMEDATALEN-1] = '\0';
		apply->replay_stop_lsn = InvalidXLogRecPtr;
		apply->forward_changesets = false;
		apply->perdb_worker_idx = perdb_worker_idx;

		/* Finally, register the worker for launch */
		if (!RegisterDynamicBackgroundWorker(&bgw,
											 &bgw_handle))
		{
			ereport(ERROR,
					(errmsg("bdr: Failed to register background worker"
							" %s, see previous log messages",
							conn_local_name)));
		}

		apply_workers = lcons(bgw_handle, apply_workers);
	}

	SPI_finish();

	CommitTransactionCommand();

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

	initStringInfo(&si);

	Assert(IsBackgroundWorker);

	perdb_worker_idx = DatumGetInt32(main_arg);
	bdr_worker_slot = &BdrWorkerCtl->slots[perdb_worker_idx];
	Assert(bdr_worker_slot->worker_type == BDR_WORKER_PERDB);
	bdr_perdb_worker = &bdr_worker_slot->worker_data.perdb_worker;
	bdr_worker_type = BDR_WORKER_PERDB;

	bdr_worker_init(NameStr(bdr_perdb_worker->dbname));

	elog(DEBUG1, "per-db worker for node " BDR_LOCALID_FORMAT " starting", BDR_LOCALID_FORMAT_ARGS);

	appendStringInfo(&si, BDR_LOCALID_FORMAT": %s", BDR_LOCALID_FORMAT_ARGS, "perdb worker");
	SetConfigOption("application_name", si.data, PGC_USERSET, PGC_S_SESSION);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	/* need to be able to perform writes ourselves */
	bdr_executor_always_allow_writes(true);
	bdr_locks_startup(bdr_perdb_worker->nnodes);

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
#define BDR_CONNECTIONS_NATTS 9
	int   i, ret;
	char *my_dbname;
	bool save_unsafe;

	save_unsafe = bdr_permit_unsafe_commands;

	StartTransactionCommand();

	my_dbname = get_database_name(MyDatabaseId);

	SPI_connect();

	bdr_permit_unsafe_commands = true;

	ret = SPI_exec("DELETE FROM bdr.bdr_connections;", 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "Couldn't clean out bdr.bdr_connections");

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrConnectionConfig *cfg = bdr_connection_configs[i];
		NameData conname, replication_name;
		char  nulls[BDR_CONNECTIONS_NATTS];
		Datum values[BDR_CONNECTIONS_NATTS];
		Oid   argtypes[BDR_CONNECTIONS_NATTS] = {OIDOID, OIDOID, OIDOID, NAMEOID, NAMEOID, TEXTOID, BOOLOID, TEXTOID, INT4OID};

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
		/* TODO DYNCONF replication sets */

		ret = SPI_execute_with_args(
					"INSERT INTO bdr.bdr_connections "
					"(conn_sysid, conn_timeline, conn_dboid, conn_local_name, conn_replication_name, conn_dsn, conn_init_replica, conn_replica_local_dsn, conn_apply_delay) "
					"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
					9, argtypes, values, nulls, false, 0
					);

		if (ret != SPI_OK_INSERT)
			elog(ERROR, "Failed to copy record into bdr.bdr_connections");

	}

	SPI_finish();
	pfree(my_dbname);
	CommitTransactionCommand();

	bdr_permit_unsafe_commands = save_unsafe;
}
