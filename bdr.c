/* -------------------------------------------------------------------------
 *
 * bdr.c
 *		Replication!!!
 *
 * Replication???
 *
 * Copyright (C) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"
#include "bdr_locks.h"
#include "bdr_label.h"

#include "libpq-fe.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port.h"

#ifdef BUILDING_BDR
#include "access/committs.h"
#endif
#include "access/heapam.h"
#include "access/xact.h"

#include "catalog/namespace.h"
#include "catalog/pg_extension.h"

#include "commands/dbcommands.h"
#include "commands/extension.h"

#include "lib/stringinfo.h"

#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"

#include "nodes/execnodes.h"

#include "postmaster/bgworker.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define MAXCONNINFO		1024

volatile sig_atomic_t got_SIGTERM = false;
volatile sig_atomic_t got_SIGHUP = false;

extern uint64		origin_sysid;
extern TimeLineID	origin_timeline;
extern Oid			origin_dboid;
/* end externs for bdr apply state */

ResourceOwner bdr_saved_resowner;
Oid   BdrNodesRelid;
Oid   BdrConflictHistoryRelId;
Oid   BdrLocksRelid;
Oid   BdrLocksByOwnerRelid;
Oid   BdrReplicationSetConfigRelid;

/* GUC storage */
static char *connections = NULL;
static bool bdr_synchronous_commit;
int bdr_default_apply_delay;
int bdr_max_workers;
int bdr_max_databases;
static bool bdr_skip_ddl_replication;
bool bdr_skip_ddl_locking;
bool bdr_do_not_replicate;

/* shmem init hook to chain to on startup, if any */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Store kind of BDR worker for the current proc, mainly for debugging */
BdrWorkerType bdr_worker_type = BDR_WORKER_EMPTY_SLOT;

/* shortcut for finding the the worker shmem block */
BdrWorkerControl *BdrWorkerCtl = NULL;

/* This worker's block within BdrWorkerCtl - only valid in bdr workers */
BdrWorker  *bdr_worker_slot = NULL;

/* Worker generation number; see bdr_worker_shmem_startup comments */
static uint16 bdr_worker_generation;


PG_MODULE_MAGIC;

void		_PG_init(void);
static void bdr_worker_shmem_startup(void);

Datum bdr_apply_pause(PG_FUNCTION_ARGS);
Datum bdr_apply_resume(PG_FUNCTION_ARGS);
Datum bdr_version(PG_FUNCTION_ARGS);
Datum bdr_variant(PG_FUNCTION_ARGS);
Datum bdr_get_local_nodeid(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bdr_apply_pause);
PG_FUNCTION_INFO_V1(bdr_apply_resume);
PG_FUNCTION_INFO_V1(bdr_version);
PG_FUNCTION_INFO_V1(bdr_variant);
PG_FUNCTION_INFO_V1(bdr_get_local_nodeid);

void
bdr_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;

	/*
	 * For now allow to interrupt all queries. It'd be better if we were more
	 * granular, only allowing to interrupt some things, but that's a bit
	 * harder than we have time for right now.
	 */
	InterruptPending = true;
	ProcDiePending = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

void
bdr_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Get database Oid of the remotedb.
 */
static Oid
bdr_get_remote_dboid(const char *conninfo_db)
{
	PGconn	   *dbConn;
	PGresult   *res;
	char	   *remote_dboid;
	Oid			remote_dboid_i;

	elog(DEBUG3, "Fetching database oid via standard connection");

	dbConn = PQconnectdb(conninfo_db);
	if (PQstatus(dbConn) != CONNECTION_OK)
	{
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the primary server: %s",
						PQerrorMessage(dbConn)),
				 errdetail("Connection string is '%s'", conninfo_db)));
	}

	res = PQexec(dbConn, "SELECT oid FROM pg_database WHERE datname = current_database()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(FATAL, "could not fetch database oid: %s",
			 PQerrorMessage(dbConn));
	}
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
	{
		elog(FATAL, "could not identify system: got %d rows and %d fields, expected %d rows and %d fields\n",
			 PQntuples(res), PQnfields(res), 1, 1);
	}

	remote_dboid = PQgetvalue(res, 0, 0);
	if (sscanf(remote_dboid, "%u", &remote_dboid_i) != 1)
		elog(ERROR, "could not parse remote database OID %s", remote_dboid);

	PQclear(res);
	PQfinish(dbConn);

	return remote_dboid_i;
}

/*
 * Establish a BDR connection
 *
 * Connects to the remote node, identifies it, and generates local and remote
 * replication identifiers and slot name.
 *
 * The local replication identifier is not saved, the caller must do that.
 *
 * Returns the PGconn for the established connection.
 *
 * Sets out parameters:
 *   remote_ident
 *   slot_name
 *   remote_sysid_i
 *   remote_tlid_i
 */
PGconn*
bdr_connect(const char *conninfo_repl,
			const char *conninfo_db,
			char* remote_ident, size_t remote_ident_length,
			NameData* slot_name,
			uint64* remote_sysid_i, TimeLineID *remote_tlid_i,
			Oid *remote_dboid_i)
{
	PGconn	   *streamConn;
	PGresult   *res;
	StringInfoData query;
	char	   *remote_sysid;
	char	   *remote_tlid;
	char		local_sysid[32];
	NameData	replication_name;

	initStringInfo(&query);
	NameStr(replication_name)[0] = '\0';

	streamConn = PQconnectdb(conninfo_repl);
	if (PQstatus(streamConn) != CONNECTION_OK)
	{
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the primary server: %s",
						PQerrorMessage(streamConn)),
				 errdetail("Connection string is '%s'", conninfo_repl)));
	}

	elog(DEBUG3, "Sending replication command: IDENTIFY_SYSTEM");

	res = PQexec(streamConn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(FATAL, "could not send replication command \"%s\": %s",
			 "IDENTIFY_SYSTEM", PQerrorMessage(streamConn));
	}
	if (PQntuples(res) != 1 || PQnfields(res) < 4 || PQnfields(res) > 5)
	{
		elog(FATAL, "could not identify system: got %d rows and %d fields, expected %d rows and %d or %d fields\n",
			 PQntuples(res), PQnfields(res), 1, 4, 5);
	}

	remote_sysid = PQgetvalue(res, 0, 0);
	remote_tlid = PQgetvalue(res, 0, 1);
	if (PQnfields(res) == 5)
	{
		char	   *remote_dboid = PQgetvalue(res, 0, 4);

		if (sscanf(remote_dboid, "%u", remote_dboid_i) != 1)
			elog(ERROR, "could not parse remote database OID %s", remote_dboid);
	}
	else
	{
		*remote_dboid_i = bdr_get_remote_dboid(conninfo_db);
	}

	if (sscanf(remote_sysid, UINT64_FORMAT, remote_sysid_i) != 1)
		elog(ERROR, "could not parse remote sysid %s", remote_sysid);

	if (sscanf(remote_tlid, "%u", remote_tlid_i) != 1)
		elog(ERROR, "could not parse remote tlid %s", remote_tlid);

	snprintf(local_sysid, sizeof(local_sysid), UINT64_FORMAT,
			 GetSystemIdentifier());

	if (strcmp(remote_sysid, local_sysid) == 0
		&& ThisTimeLineID == *remote_tlid_i
		&& MyDatabaseId == *remote_dboid_i)
	{
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("The system identifier, timeline ID and/or database oid must differ between the nodes"),
				 errdetail("Both keys are (sysid, timelineid, dboid) = (%s,%u,%u)",
				 remote_sysid, *remote_tlid_i, *remote_dboid_i)));
	}
	else
		elog(DEBUG2, "local node (%s,%u,%u), remote node (%s,%s,%u)",
			 local_sysid, ThisTimeLineID, MyDatabaseId, remote_sysid,
			 remote_tlid, *remote_dboid_i);

	/*
	 * build slot name.
	 *
	 * FIXME: This might truncate the identifier if replication_name is
	 * somewhat longer...
	 */
	snprintf(NameStr(*slot_name), NAMEDATALEN, BDR_SLOT_NAME_FORMAT,
			 *remote_dboid_i, local_sysid, ThisTimeLineID,
			 MyDatabaseId, NameStr(replication_name));
	NameStr(*slot_name)[NAMEDATALEN - 1] = '\0';

	/*
	 * Build replication identifier.
	 */
	snprintf(remote_ident, remote_ident_length,
			 BDR_NODE_ID_FORMAT,
			 *remote_sysid_i, *remote_tlid_i, *remote_dboid_i, MyDatabaseId,
			 NameStr(replication_name));

	/* no parts of IDENTIFY_SYSTEM's response needed anymore */
	PQclear(res);

	return streamConn;
}

/*
 * ----------
 * Create a slot on a remote node, and the corresponding local replication
 * identifier.
 *
 * Arguments:
 *   streamConn		Connection to use for slot creation
 *   slot_name		Name of the slot to create
 *   remote_ident	Identifier for the remote end
 *
 * Out parameters:
 *   replication_identifier		Created local replication identifier
 *   snapshot					If !NULL, snapshot ID of slot snapshot
 *
 * If a snapshot is returned it must be pfree()'d by the caller.
 * ----------
 */
/*
 * TODO we should really handle the case where the slot already exists but
 * there's no local replication identifier, by dropping and recreating the
 * slot.
 */
static void
bdr_create_slot(PGconn *streamConn, Name slot_name,
				char *remote_ident, RepNodeId *replication_identifier,
				char **snapshot)
{
	StringInfoData query;
	PGresult   *res;

	initStringInfo(&query);

	StartTransactionCommand();

	/* we want the new identifier on stable storage immediately */
	ForceSyncCommit();

	/* acquire remote decoding slot */
	resetStringInfo(&query);
	appendStringInfo(&query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
					 NameStr(*slot_name), "bdr");

	elog(DEBUG3, "Sending replication command: %s", query.data);

	res = PQexec(streamConn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		/* TODO: Should test whether this error is 'already exists' and carry on */

		elog(FATAL, "could not send replication command \"%s\": status %s: %s\n",
			 query.data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	/* acquire new local identifier, but don't commit */
	*replication_identifier = CreateReplicationIdentifier(remote_ident);

	/* now commit local identifier */
	CommitTransactionCommand();
	CurrentResourceOwner = bdr_saved_resowner;
	elog(DEBUG1, "created replication identifier %u", *replication_identifier);

	if (snapshot)
		*snapshot = pstrdup(PQgetvalue(res, 0, 2));

	PQclear(res);
}

/*
 * Perform setup work common to all bdr worker types, such as:
 *
 * - set signal handers and unblock signals
 * - Establish db connection
 * - set search_path
 *
 */
void
bdr_worker_init(char *dbname)
{
	Assert(IsBackgroundWorker);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, bdr_sighup);
	pqsignal(SIGTERM, bdr_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(dbname, NULL);

	/* make sure BDR extension exists */
	bdr_executor_always_allow_writes(true);
	StartTransactionCommand();
	bdr_maintain_schema(true);
	CommitTransactionCommand();
	bdr_executor_always_allow_writes(false);

	/* always work in our own schema */
	SetConfigOption("search_path", "bdr, pg_catalog",
					PGC_BACKEND, PGC_S_OVERRIDE);

	/* setup synchronous commit according to the user's wishes */
	SetConfigOption("synchronous_commit",
					bdr_synchronous_commit ? "local" : "off",
					PGC_BACKEND, PGC_S_OVERRIDE);	/* other context? */

	/*
	 * Disable function body checks during replay. That's necessary because a)
	 * the creator of the function might have had it disabled b) the function
	 * might be search_path dependant and we don't fix the contents of
	 * functions.
	 */
	SetConfigOption("check_function_bodies", "off",
					PGC_INTERNAL, PGC_S_OVERRIDE);

}

/*
 *----------------------
 * Connect to the BDR remote end, IDENTIFY_SYSTEM, and CREATE_SLOT if necessary.
 * Generates slot name, replication identifier.
 *
 * Raises an error on failure, will not return null.
 *
 * Arguments:
 * 	  connection_name:  bdr conn name from bdr.connections to get dsn from
 *
 * Returns:
 *    the libpq connection
 *
 * Out parameters:
 *    out_slot_name: the generated name of the slot on the remote end
 *    out_sysid:     the remote end's system identifier
 *    out_timeline:  the remote end's current timeline
 *    out_replication_identifier: The replication identifier for this connection
 *
 *----------------------
 */
PGconn*
bdr_establish_connection_and_slot(const char *dsn,
	const char *application_name_suffix, Name out_slot_name, uint64 *out_sysid,
	TimeLineID* out_timeline, Oid *out_dboid,
	RepNodeId *out_replication_identifier, char **out_snapshot)
{
	char		remote_ident[256];
	PGconn	   *streamConn;
	StringInfoData conninfo_repl;
	bool		tx_started = false;

	initStringInfo(&conninfo_repl);

	appendStringInfo(&conninfo_repl,
					 "%s replication=database fallback_application_name='"BDR_LOCALID_FORMAT": %s'",
					 dsn, BDR_LOCALID_FORMAT_ARGS,
					 application_name_suffix);

	/* Establish BDR conn and IDENTIFY_SYSTEM */
	streamConn = bdr_connect(
		conninfo_repl.data,
		dsn,
		remote_ident, sizeof(remote_ident),
		out_slot_name, out_sysid, out_timeline, out_dboid
		);

	if (!IsTransactionState())
	{
		tx_started = true;
		StartTransactionCommand();
	}
	*out_replication_identifier = GetReplicationIdentifier(remote_ident, true);
	if (tx_started)
		CommitTransactionCommand();

	if (OidIsValid(*out_replication_identifier))
	{
		elog(DEBUG1, "found valid replication identifier %u",
			 *out_replication_identifier);
		if (out_snapshot)
			*out_snapshot = NULL;
	}
	else
	{
		/*
		 * Slot doesn't exist, create it.
		 *
		 * The per-db worker will create slots when we first init BDR, but new workers
		 * added afterwards are expected to create their own slots at connect time; that's
		 * when this runs.
		 */

		/* create local replication identifier and a remote slot */
		elog(DEBUG1, "Creating new slot %s", NameStr(*out_slot_name));
		bdr_create_slot(streamConn, out_slot_name, remote_ident,
						out_replication_identifier, out_snapshot);
	}

	return streamConn;
}

static size_t
bdr_worker_shmem_size()
{
	Size		size = 0;

	size = add_size(size, sizeof(BdrWorkerControl));
	size = add_size(size, mul_size(bdr_max_workers, sizeof(BdrWorker)));

	return size;
}

/*
 * Allocate a shared memory segment big enough to hold bdr_max_workers entries
 * in the array of BDR worker info structs (BdrApplyWorker).
 *
 * Called during _PG_init, but not during postmaster restart.
 */
static void
bdr_worker_alloc_shmem_segment()
{
	Assert(process_shared_preload_libraries_in_progress);

	/* Allocate enough shmem for the worker limit ... */
	RequestAddinShmemSpace(bdr_worker_shmem_size());

	/*
	 * We'll need to be able to take exclusive locks so only one per-db backend
	 * tries to allocate or free blocks from this array at once.  There won't
	 * be enough contention to make anything fancier worth doing.
	 */
	RequestAddinLWLocks(1);

	/*
	 * Whether this is a first startup or crash recovery, we'll be re-initing
	 * the bgworkers.
	 */
	BdrWorkerCtl = NULL;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = bdr_worker_shmem_startup;
}

/*
 * Init the header for our shm segment, if not already done.
 *
 * Called during postmaster start or restart, in the context of the postmaster.
 */
static void
bdr_worker_shmem_startup(void)
{
	bool        found;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	BdrWorkerCtl = ShmemInitStruct("bdr_worker",
								  bdr_worker_shmem_size(),
								  &found);
	if (!found)
	{
		/* Must be in postmaster its self */
		Assert(IsPostmasterEnvironment && !IsUnderPostmaster);

		/* Init shm segment header after postmaster start or restart */
		memset(BdrWorkerCtl, 0, bdr_worker_shmem_size());
		BdrWorkerCtl->lock = LWLockAssign();
 		/* Assigned on supervisor launch */
		BdrWorkerCtl->supervisor_latch = NULL;

		/*
		 * The postmaster keeps track of a generation number for BDR workers
		 * and increments it at each restart.
		 *
		 * Background workers aren't unregistered when the postmaster restarts
		 * and clears shared memory, so after a restart the supervisor and
		 * per-db workers have no idea what workers are/aren't running, nor any
		 * way to control them. To make a clean BDR restart possible the
		 * workers registered before the restart need to find out about the
		 * restart and terminate.
		 *
		 * To make that possible we pass the generation number to the worker
		 * in its main argument, and also set it in shared memory. The two
		 * must match. If they don't, the worker will proc_exit(0), causing its
		 * self to be unregistered.
		 *
		 * This should really be part of the bgworker API its self, handled via
		 * a BGW_NO_RESTART_ON_CRASH flag or by providing a generation number
		 * as a bgworker argument. However, for now we're stuck with this
		 * workaround.
		 */
		if (bdr_worker_generation == UINT16_MAX)
			/* We could handle wrap-around, but really ... */
			elog(FATAL, "Too many postmaster crash/restart cycles. Restart the PostgreSQL server.");

		BdrWorkerCtl->worker_generation = ++bdr_worker_generation;
	}
	LWLockRelease(AddinShmemInitLock);

	/*
	 * We don't have anything to preserve on shutdown and don't support being
	 * unloaded from a running Pg, so don't register any shutdown hook.
	 */
}


/*
 * Allocate a block from the bdr_worker shm segment in BdrWorkerCtl, or ERROR
 * if there are no free slots.
 *
 * The block is zeroed. The worker type is set in the header.
 *
 * ctl_idx, if passed, is set to the index of the worker within BdrWorkerCtl.
 *
 * To release a block, use bdr_worker_shmem_release(...)
 *
 * You must hold BdrWorkerCtl->lock in LW_EXCLUSIVE mode for
 * this call.
 */
BdrWorker*
bdr_worker_shmem_alloc(BdrWorkerType worker_type, uint32 *ctl_idx)
{
	int i;

	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));
	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *new_entry = &BdrWorkerCtl->slots[i];
		if (new_entry->worker_type == BDR_WORKER_EMPTY_SLOT)
		{
			memset(new_entry, 0, sizeof(BdrWorker));
			new_entry->worker_type = worker_type;
			if (ctl_idx)
				*ctl_idx = i;
			return new_entry;
		}
	}
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			errmsg("No free bdr worker slots - bdr.max_workers is too low")));
	/* unreachable */
}

/*
 * Release a block allocated by bdr_worker_shmem_alloc so it can be
 * re-used.
 *
 * The bgworker *must* no longer be running.
 *
 * If passed, the bgworker handle is checked to ensure the worker
 * is not still running before the slot is released.
 */
void
bdr_worker_shmem_release(BdrWorker* worker, BackgroundWorkerHandle *handle)
{
	LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

	/* Already free? Do nothing */
	if (worker->worker_type != BDR_WORKER_EMPTY_SLOT)
	{
		/* Sanity check - ensure any associated dynamic bgworker is stopped */
		if (handle)
		{
			pid_t pid;
			BgwHandleStatus status;
			status = GetBackgroundWorkerPid(handle, &pid);
			if (status == BGWH_STARTED)
			{
				LWLockRelease(BdrWorkerCtl->lock);
				elog(ERROR, "BUG: Attempt to release shm segment for bdr worker type=%d pid=%d that's still alive",
					 worker->worker_type, pid);
			}
		}

		/* Mark it as free */
		worker->worker_type = BDR_WORKER_EMPTY_SLOT;
		/* and for good measure, zero it so problems are seen immediately */
		memset(worker, 0, sizeof(BdrWorker));
	}
	LWLockRelease(BdrWorkerCtl->lock);
}

/*
 * Is the current database configured for bdr?
 *
 * TODO: Right now this is rechecked for every call. It'd be good to cache it
 * in future. To do that, we can register a syscache invalidation callback on
 * our pg_database row. Then when BDR is enabled or disabled on a DB, we
 * invalidate the pg_database cache entry. Callbacks are registered with
 * CacheRegisterSyscacheCallback . This can be used to set/clear an enabled
 * flag.
 *
 * We don't check pg_shseclabel for the DB because that's very expensive,
 * requiring a snapshot and a heavyweight lock. There's no syscache for
 * pg_shseclable to make it sane.
 */
bool
bdr_is_bdr_activated_db(void)
{
	const char *mydb;
	int			i;
	bool		bdr_active = false;

	/* won't know until we've forked/execed */
	Assert(IsUnderPostmaster);

	/* potentially need to access syscaches */
	Assert(IsTransactionState());

	/*
	 * Accessing the database name via MyProcPort is faster, but only works in
	 * user initiated connections. Not background workers.
	 */
	if (MyProcPort != NULL)
		mydb = MyProcPort->database_name;
	else
		mydb = get_database_name(MyDatabaseId);

	/*
	 * Look for the perdb worker's shmem entries, they have the database name.
	 *
	 * These will be present even if the worker isn't actually running (not
	 * started yet, crashed & restarting, etc).
	 */
	LWLockAcquire(BdrWorkerCtl->lock, LW_SHARED);
	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *worker;
		const char *workerdb;

		worker = &BdrWorkerCtl->slots[i];

		if (worker->worker_type != BDR_WORKER_PERDB)
			continue;

		workerdb = NameStr(worker->worker_data.perdb_worker.dbname);

		if (strcmp(mydb, workerdb) == 0)
		{
			bdr_active = true;
			break;
		}
	}
	LWLockRelease(BdrWorkerCtl->lock);

	if (bdr_active)
		return true;

	/*
	 * Make sure nobody changes the replication slot list concurrently
	 */
	LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);

	/* If no worker was found, try searching for slot with bdr output plugin */
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

		Oid			database;
		NameData	plugin;

		/* XXX: is the spinlock necessary? */
		SpinLockAcquire(&slot->mutex);
		if (!slot->in_use)
		{
			SpinLockRelease(&slot->mutex);
			continue;
		}
		else
		{
			database = slot->data.database;
			namecpy(&plugin, &slot->data.plugin);
		}
		SpinLockRelease(&slot->mutex);

		if (database == MyDatabaseId && strcmp(NameStr(plugin), "bdr") == 0)
		{
			LWLockRelease(ReplicationSlotControlLock);
			return true;
		}
	}

	LWLockRelease(ReplicationSlotControlLock);

	return false;
}

static bool
bdr_do_not_replicate_check_hook(bool *newvalue, void **extra, GucSource source)
{
	if (!(*newvalue))
		/* False is always acceptable */
		return true;

	/*
	 * Only set bdr.do_not_replicate if configured via startup packet from the
	 * client application. This prevents possibly unsafe accesses to the
	 * replication identifier state in postmaster context, etc.
	 */
	if (source != PGC_S_CLIENT)
		return false;

	Assert(IsUnderPostmaster);
	Assert(!IsBackgroundWorker);

	return true;
}

/*
 * Override the origin replication identifier that this session will record for
 * its transactions. We need this mainly when applying dumps during
 * init_replica.
 */
static void
bdr_do_not_replicate_assign_hook(bool newvalue, void *extra)
{
	if (newvalue)
	{
		/* Mark these transactions as not to be replicated to other nodes */
		SetupCachedReplicationIdentifier(DoNotReplicateRepNodeId);
	}
}


/*
 * Entrypoint of this module - called at shared_preload_libraries time in the
 * context of the postmaster.
 *
 * Can't use SPI, and should do as little as sensibly possible. Must initialize
 * any PGC_POSTMASTER custom GUCs, register static bgworkers, as that can't be
 * done later.
 */
void
_PG_init(void)
{
	MemoryContext old_context;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("bdr can only be loaded via shared_preload_libraries")));

#ifdef BUILDING_BDR
	if (!commit_ts_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bdr requires \"track_commit_timestamp\" to be enabled")));
#endif
	
	/*
	 * _PG_init only runs on first load, not on postmaster restart, so
	 * set the worker generation here. See bdr_worker_shmem_startup.
	 *
	 * It starts at 1 because the postmaster zeroes shmem on restart, so 0 can
	 * mean "just restarted, hasn't run shmem setup callback yet".
	 */
	bdr_worker_generation = 1;

	/*
	 * Force btree_gist to be loaded - its absolutely not required at this
	 * point, but since it's required for BDR to be used it's much easier to
	 * debug if we error out during start than failing during background
	 * worker initialization.
	 */
	load_external_function("btree_gist", "gbtreekey_in", true, NULL);

	/* guc's et al need to survive outside the lifetime of the library init */
	old_context = MemoryContextSwitchTo(TopMemoryContext);

	DefineCustomStringVariable("bdr.connections",
							   "List of connections",
							   NULL,
							   &connections,
							   NULL, PGC_POSTMASTER,
							   GUC_LIST_INPUT | GUC_LIST_QUOTE,
							   NULL, NULL, NULL);

	/* XXX: make it changeable at SIGHUP? */
	DefineCustomBoolVariable("bdr.synchronous_commit",
							   "bdr specific synchronous commit value",
							   NULL,
							   &bdr_synchronous_commit,
							   false, PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("bdr.log_conflicts_to_table",
							 "Log BDR conflicts to bdr.conflict_history table",
							 NULL,
							 &bdr_log_conflicts_to_table,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("bdr.conflict_logging_include_tuples",
							 "Log whole tuples when logging BDR conflicts",
							 NULL,
							 &bdr_conflict_logging_include_tuples,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

#ifdef BUILDING_UDR
	DefineCustomBoolVariable("bdr.conflict_default_apply",
							 "Apply conflicting changes by default",
							 NULL,
							 &bdr_conflict_default_apply,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);
#endif

	/*
	 * Limit on worker count - number of slots to allocate in fixed shared
	 * memory array.
	 */
	DefineCustomIntVariable("bdr.max_workers",
							"max number of bdr connections + distinct databases.",
							NULL,
							&bdr_max_workers,
							20, 2, 100,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("bdr.max_databases",
							"max number of distinct databases on which BDR may be active",
							NULL,
							&bdr_max_databases,
							-1, -1, 50,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);


	DefineCustomBoolVariable("bdr.permit_unsafe_ddl_commands",
							 "Allow commands that might cause data or " \
							 "replication problems under BDR to run",
							 NULL,
							 &bdr_permit_unsafe_commands,
							 false, PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("bdr.skip_ddl_replication",
							 "Internal. Set during local restore during init_replica only",
							 NULL,
							 &bdr_skip_ddl_replication,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("bdr.skip_ddl_locking",
							 "Don't acquire global DDL locks while performing DDL.",
							 "Note that it's quite dangerous to do so.",
							 &bdr_skip_ddl_locking,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("bdr.default_apply_delay",
							"default replication apply delay, can be overwritten per connection",
							NULL,
							&bdr_default_apply_delay,
							0, 0, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	/*
	 * We can't use the temp_tablespace safely for our dumps, because Pg's
	 * crash recovery is very careful to delete only particularly formatted
	 * files. Instead for now just allow user to specify dump storage.
	 */
	DefineCustomStringVariable("bdr.temp_dump_directory",
							   "Directory to store dumps for local restore",
							   NULL,
							   &bdr_temp_dump_directory,
							   "/tmp", PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("bdr.do_not_replicate",
							 "Internal. Set during local initialization from basebackup only",
							 NULL,
							 &bdr_do_not_replicate,
							 false,
							 PGC_BACKEND,
							 0,
							 bdr_do_not_replicate_check_hook,
							 bdr_do_not_replicate_assign_hook,
							 NULL);

	bdr_label_init();

	bdr_supervisor_register();

	/*
	 * Sanity check max_worker_processes to make sure it's at least big enough
	 * to hold all our BDR workers. There's no way to reserve them or guarantee
	 * anyone else won't claim some, but this'll spot the most obvious
	 * misconfiguration.
	 */
	if (max_worker_processes < bdr_max_workers)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("bdr_max_workers is greater than max_worker_processes, may fail to start workers"),
				 errhint("Set max_worker_processes to at least %d", bdr_max_workers)));
	}

	/*
	 * If bdr.max_databases is not explicitly specified, assume the worst case
	 * of many DBs with one connection per DB.
	 */
	if (bdr_max_databases == -1)
	{
		bdr_max_databases = bdr_max_workers / 2;
		elog(DEBUG1, "Autoconfiguring bdr.max_databases to %d (bdr.max_workers/2)",
			 bdr_max_databases);
	}

	/*
	 * Allocate a shared memory segment to store the bgworker connection
	 * information we must pass to each worker we launch.
	 *
	 * This registers a hook on shm initialization, bdr_worker_shmem_startup(),
	 * which populates the shm segment with configured apply workers using data
	 * in bdr_connection_configs.
	 */
	bdr_worker_alloc_shmem_segment();

	EmitWarningsOnPlaceholders("bdr");

	/*
	 * initialize other modules that need shared memory
	 */

	/* register a slot for every remote node */
	bdr_count_shmem_init(bdr_max_workers);
	bdr_executor_init();
#ifdef BUILDING_BDR
	bdr_sequencer_shmem_init(bdr_max_workers, bdr_max_databases);
#endif
	bdr_locks_shmem_init();
	/* Set up a ProcessUtility_hook to stop unsupported commands being run */
	init_bdr_commandfilter();

	MemoryContextSwitchTo(old_context);
}

Oid
bdr_lookup_relid(const char *relname, Oid schema_oid)
{
	Oid			relid;

	relid = get_relname_relid(relname, schema_oid);

	if (!relid)
		elog(ERROR, "cache lookup failed for relation %s.%s",
			 get_namespace_name(schema_oid), relname);

	return relid;
}

/*
 * Make sure all required extensions are installed in the correct version for
 * the current database.
 *
 * Concurrent executions will block, but not fail.
 *
 * Must be called inside transaction.
 *
 * If update_extensions is true, ALTER EXTENSION commands will be issued to
 * ensure the required extension(s) are at the current version.
 */
void
bdr_maintain_schema(bool update_extensions)
{
	Relation	extrel;
	Oid			btree_gist_oid;
	Oid			bdr_oid;
	Oid			schema_oid;

	PushActiveSnapshot(GetTransactionSnapshot());

	set_config_option("bdr.skip_ddl_replication", "true",
					  PGC_SUSET, PGC_S_OVERRIDE, GUC_ACTION_LOCAL,
					  true, 0);

	/* make sure we're operating without other bdr workers interfering */
	extrel = heap_open(ExtensionRelationId, ShareUpdateExclusiveLock);

	btree_gist_oid = get_extension_oid("btree_gist", true);
	bdr_oid = get_extension_oid("bdr", true);

	if (btree_gist_oid == InvalidOid)
		elog(ERROR, "btree_gist is required by BDR but not installed in the current database");

	if (bdr_oid == InvalidOid)
		elog(ERROR, "bdr extension is not installed in the current database");

	if (update_extensions)
	{
		AlterExtensionStmt alter_stmt;

		/* TODO: only do this if necessary */
		alter_stmt.options = NIL;
		alter_stmt.extname = (char *)"btree_gist";
		ExecAlterExtensionStmt(&alter_stmt);

		/* TODO: only do this if necessary */
		alter_stmt.options = NIL;
		alter_stmt.extname = (char *)"bdr";
		ExecAlterExtensionStmt(&alter_stmt);
	}

	heap_close(extrel, NoLock);

	/* setup initial queued_cmds OID */
	schema_oid = get_namespace_oid("bdr", false);
	QueuedDDLCommandsRelid =
		bdr_lookup_relid("bdr_queued_commands", schema_oid);
	BdrConflictHistoryRelId =
		bdr_lookup_relid("bdr_conflict_history", schema_oid);

#ifdef BUILDING_BDR
	BdrSequenceValuesRelid =
		bdr_lookup_relid("bdr_sequence_values", schema_oid);
	BdrSequenceElectionsRelid =
		bdr_lookup_relid("bdr_sequence_elections", schema_oid);
	BdrVotesRelid =
		bdr_lookup_relid("bdr_votes", schema_oid);
	BdrNodesRelid =
		bdr_lookup_relid("bdr_nodes", schema_oid);
	QueuedDropsRelid =
		bdr_lookup_relid("bdr_queued_drops", schema_oid);
	BdrLocksRelid =
		bdr_lookup_relid("bdr_global_locks", schema_oid);
	BdrLocksByOwnerRelid =
		bdr_lookup_relid("bdr_global_locks_byowner", schema_oid);
#endif

	BdrReplicationSetConfigRelid  =
		bdr_lookup_relid("bdr_replication_set_config", schema_oid);

	bdr_conflict_handlers_init();

	PopActiveSnapshot();
}

Datum
bdr_apply_pause(PG_FUNCTION_ARGS)
{
	BdrWorkerCtl->pause_apply = true;
	PG_RETURN_VOID();
}

Datum
bdr_apply_resume(PG_FUNCTION_ARGS)
{
	BdrWorkerCtl->pause_apply = false;
	PG_RETURN_VOID();
}

Datum
bdr_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(BDR_VERSION_STR));
}

Datum
bdr_variant(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(BDR_VARIANT));
}

/* Return a tuple of (sysid oid, tlid oid, dboid oid) */
Datum
bdr_get_local_nodeid(PG_FUNCTION_ARGS)
{
	Datum		values[3];
	bool		isnull[3] = {false, false, false};
	TupleDesc	tupleDesc;
	HeapTuple	returnTuple;
	char		sysid_str[33];

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, GetSystemIdentifier());
	sysid_str[sizeof(sysid_str)-1] = '\0';

	values[0] = CStringGetTextDatum(sysid_str);
	values[1] = ObjectIdGetDatum(ThisTimeLineID);
	values[2] = ObjectIdGetDatum(MyDatabaseId);

	returnTuple = heap_form_tuple(tupleDesc, values, isnull);

	PG_RETURN_DATUM(HeapTupleGetDatum(returnTuple));
}
