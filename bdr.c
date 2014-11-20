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

#include "catalog/catversion.h"
#include "catalog/namespace.h"
#include "catalog/pg_extension.h"

#include "commands/dbcommands.h"
#include "commands/extension.h"

#include "lib/stringinfo.h"

#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

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
static bool bdr_is_restart = false;
Oid   BdrNodesRelid;
Oid   BdrConflictHistoryRelId;
Oid   BdrLocksRelid;
Oid   BdrLocksByOwnerRelid;
Oid   BdrReplicationSetConfigRelid;

BdrConnectionConfig  **bdr_connection_configs;
/* All databases for which BDR is configured, valid after _PG_init */
char **bdr_distinct_dbnames;
uint32 bdr_distinct_dbnames_count = 0;

/* GUC storage */
static char *connections = NULL;
static bool bdr_synchronous_commit;
int bdr_default_apply_delay;
int bdr_max_workers;
static bool bdr_skip_ddl_replication;
bool bdr_skip_ddl_locking;

/* shmem init hook to chain to on startup, if any */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Store kind of BDR worker for the current proc, mainly for debugging */
BdrWorkerType bdr_worker_type = BDR_WORKER_EMPTY_SLOT;

/* shortcut for finding the the worker shmem block */
BdrWorkerControl *BdrWorkerCtl = NULL;

PG_MODULE_MAGIC;

void		_PG_init(void);
static void bdr_worker_shmem_startup(void);
static void bdr_worker_shmem_create_workers(void);

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

static void
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

static void
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
bdr_get_remote_dboid(char *conninfo_db)
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
bdr_connect(char *conninfo_repl,
			char *conninfo_db,
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
	char	   *remote_dbname;
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
	remote_dbname = PQgetvalue(res, 0, 3);
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
				 errdetail("Both keys are (sysid, timelineid, dboid) = (%s,%s,%s)",
				 remote_sysid, remote_tlid, remote_dbname)));
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
	bdr_maintain_schema();
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
bdr_establish_connection_and_slot(BdrConnectionConfig *cfg,
	const char *application_name_suffix, Name out_slot_name, uint64 *out_sysid,
	TimeLineID* out_timeline, Oid *out_dboid,
	RepNodeId *out_replication_identifier, char **out_snapshot)
{
	char		remote_ident[256];
	PGconn	   *streamConn;
	StringInfoData conninfo_repl;

	initStringInfo(&conninfo_repl);

	appendStringInfo(&conninfo_repl,
					 "%s replication=database fallback_application_name='"BDR_LOCALID_FORMAT": %s'",
					 cfg->dsn, BDR_LOCALID_FORMAT_ARGS,
					 application_name_suffix);

	/* Establish BDR conn and IDENTIFY_SYSTEM */
	streamConn = bdr_connect(
		conninfo_repl.data,
		cfg->dsn,
		remote_ident, sizeof(remote_ident),
		out_slot_name, out_sysid, out_timeline, out_dboid
		);

	StartTransactionCommand();
	*out_replication_identifier = GetReplicationIdentifier(remote_ident, true);
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

/*
 * In postmaster, at shared_preload_libaries time, create the GUCs for a
 * connection. They'll be accessed by the apply worker that uses these GUCs
 * later.
 *
 * Returns false if the config wasn't created for some reason (missing
 * required options, etc); true if it's ok. Out parameters are not changed if
 * false is returned.
 *
 * Params:
 *
 *  name
 *  Name of this conn - bdr.<name>
 *
 *  used_databases
 *  Array of char*, names of distinct databases named in configured conns
 *
 *  num_used_databases
 *  Number of distinct databases named in conns
 *
 *	out_config
 *  Assigned a palloc'd pointer to GUC storage for this config'd connection
 *
 * out_config is set even if false is returned, as the GUCs have still been
 * created. Test out_config->is_valid to see whether the connection is usable.
 */
static bool
bdr_create_con_gucs(char  *name,
					char **used_databases,
					Size  *num_used_databases,
					char **database_initcons,
					BdrConnectionConfig **out_config)
{
	Size		off;
	char	   *errormsg = NULL;
	PQconninfoOption *options;
	PQconninfoOption *cur_option;
	BdrConnectionConfig *opts;

	/* don't free, referenced by the guc machinery! */
	char	   *optname_dsn = palloc(strlen(name) + 30);
	char	   *optname_delay = palloc(strlen(name) + 30);
	char	   *optname_replica = palloc(strlen(name) + 30);
	char	   *optname_local_dsn = palloc(strlen(name) + 30);
	char	   *optname_local_dbname = palloc(strlen(name) + 30);
	char	   *optname_replication_sets = palloc(strlen(name) + 30);

	Assert(process_shared_preload_libraries_in_progress);

	/* Ensure the connection name is legal */
	if (strchr(name, '_') != NULL)
	{
		ereport(ERROR,
				(errmsg("bdr.connections entry '%s' contains the '_' character, which is not permitted", name)));
	}

	/* allocate storage for connection parameters */
	opts = palloc0(sizeof(BdrConnectionConfig));
	opts->is_valid = false;
	*out_config = opts;

	opts->name = pstrdup(name);

	/* Define GUCs for this connection */
	sprintf(optname_dsn, "bdr.%s_dsn", name);
	DefineCustomStringVariable(optname_dsn,
							   optname_dsn,
							   NULL,
							   &opts->dsn,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	sprintf(optname_delay, "bdr.%s_apply_delay", name);
	DefineCustomIntVariable(optname_delay,
							optname_delay,
							NULL,
							&opts->apply_delay,
							-1, -1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	sprintf(optname_replica, "bdr.%s_init_replica", name);
	DefineCustomBoolVariable(optname_replica,
							 optname_replica,
							 NULL,
							 &opts->init_replica,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	sprintf(optname_local_dsn, "bdr.%s_replica_local_dsn", name);
	DefineCustomStringVariable(optname_local_dsn,
							   optname_local_dsn,
							   NULL,
							   &opts->replica_local_dsn,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	sprintf(optname_local_dbname, "bdr.%s_local_dbname", name);
	DefineCustomStringVariable(optname_local_dbname,
							   optname_local_dbname,
							   NULL,
							   &opts->dbname,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	sprintf(optname_replication_sets, "bdr.%s_replication_sets", name);
	DefineCustomStringVariable(optname_replication_sets,
							   optname_replication_sets,
	                           NULL,
							   &opts->replication_sets,
							   NULL, PGC_POSTMASTER,
							   GUC_LIST_INPUT | GUC_LIST_QUOTE,
							   NULL, NULL, NULL);


	if (!opts->dsn)
	{
		elog(WARNING, "bdr %s: no connection information", name);
		return false;
	}

	elog(DEBUG2, "bdr %s: dsn=%s", name, opts->dsn);

	options = PQconninfoParse(opts->dsn, &errormsg);
	if (errormsg != NULL)
	{
		char	   *str = pstrdup(errormsg);

		PQfreemem(errormsg);
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("bdr %s: error in dsn: %s", name, str)));
	}

	if (opts->dbname == NULL)
	{
		cur_option = options;
		while (cur_option->keyword != NULL)
		{
			if (strcmp(cur_option->keyword, "dbname") == 0)
			{
				if (cur_option->val == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("bdr %s: no dbname set", name)));

				opts->dbname = pstrdup(cur_option->val);
				elog(DEBUG2, "bdr %s: dbname=%s", name, opts->dbname);
			}

			if (cur_option->val != NULL)
			{
				elog(DEBUG3, "bdr %s: opt %s, val: %s",
					 name, cur_option->keyword, cur_option->val);
			}
			cur_option++;
		}
	}

	/* cleanup */
	PQconninfoFree(options);

	/*
	 * If this is a DB name we haven't seen yet, add it to our set of known
	 * DBs.
	 */
	for (off = 0; off < *num_used_databases; off++)
	{
		if (strcmp(opts->dbname, used_databases[off]) == 0)
			break;
	}

	if (off == *num_used_databases)
	{
		/* Didn't find a match, add new db name */
		used_databases[(*num_used_databases)++] =
			pstrdup(opts->dbname);
		elog(DEBUG2, "bdr %s: Saw new database %s, now %i known dbs",
			 name, opts->dbname, (int)(*num_used_databases));
	}

	/*
	 * Make sure that at most one of the worker configs for each DB can be
	 * configured to run initialization.
	 */
	if (opts->init_replica)
	{
		elog(DEBUG2, "bdr %s: has init_replica=t", name);
		if (database_initcons[off] != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("Connections %s and %s on database %s both have bdr_init_replica enabled, cannot continue",
							name, database_initcons[off], used_databases[off])));
		else
			database_initcons[off] = name; /* no need to pstrdup, see _PG_init */
	}

	opts->is_valid = true;

	/* optname vars intentionally leaked, see above */
	return true;
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

		/*
		 * Now that the shm segment is initialized, we can populate it with
		 * BdrWorker entries for the connections we created GUCs for during
		 * _PG_init.
		 *
		 * We must do this whether it's initial launch or a postmaster restart,
		 * as shmem gets cleared on postmaster restart.
		 */
		bdr_worker_shmem_create_workers();
	}
	LWLockRelease(AddinShmemInitLock);

	/*
	 * We don't have anything to preserve on shutdown and don't support being
	 * unloaded from a running Pg, so don't register any shutdown hook.
	 */
}

/*
 * After _PG_init we've read the GUCs for the workers but haven't populated the
 * shared memory segment at BdrWorkerCtl with BDRWorker entries yet.
 *
 * The shm segment is initialized now, so do that.
 */
static void
bdr_worker_shmem_create_workers(void)
{
	uint32 off;

	/*
	 * Create a BdrPerdbWorker for each distinct database found during
	 * _PG_init. The bgworker for each has already been registered and assigned
	 * a slot position during _PG_init, but the slot doesn't have anything
	 * useful in it yet. Because it was already registered we don't need
	 * any protection against duplicate launches on restart here.
	 *
	 * Because these slots are pre-assigned before shmem is bought up they
	 * MUST be reserved first, before any shmem entries are allocated, so
	 * they get the first slots.
	 *
	 * When started, this worker will continue setup - doing any required
	 * initialization of the database, then registering dynamic bgworkers for
	 * the DB's individual BDR connections.
	 *
	 * If we ever want to support dynamically adding/removing DBs from BDR at
	 * runtime, this'll need to move into a static bgworker because dynamic
	 * bgworkers can't be launched directly from the postmaster. We'll need a
	 * "bdr manager" static bgworker.
	 */

	for (off = 0; off < bdr_distinct_dbnames_count; off++)
	{
		BdrWorker	   *shmworker;
		BdrPerdbWorker *perdb;
		uint32		ctl_idx;

		shmworker = (BdrWorker *) bdr_worker_shmem_alloc(BDR_WORKER_PERDB, &ctl_idx);
		Assert(shmworker->worker_type == BDR_WORKER_PERDB);
		/*
		 * The workers have already been assigned shmem indexes during
		 * _PG_init, so they MUST get the same index here. So long as these
		 * entries are assigned before any other shmem slots they will.
		 */
		Assert(ctl_idx == off);
		perdb = &shmworker->worker_data.perdb_worker;

		strncpy(NameStr(perdb->dbname), bdr_distinct_dbnames[off], NAMEDATALEN);
		NameStr(perdb->dbname)[NAMEDATALEN-1] = '\0';

		perdb->nnodes = 0;
		perdb->seq_slot = off;

		elog(DEBUG1, "Assigning shmem bdr database worker for db %s",
			 NameStr(perdb->dbname));
	}

	/*
	 * Populate shmem with a BdrApplyWorker for each valid BdrConnectionConfig
	 * found during _PG_init so that the per-db worker will register it for
	 * startup after performing any BDR initialisation work.
	 *
	 * Use of shared memory for this is required for EXEC_BACKEND (windows)
	 * where we can't share postmaster memory, and for when we're launching a
	 * bgworker from another bgworker where the fork() from postmaster doesn't
	 * provide access to the launching bgworker's memory.
	 *
	 * The workers aren't actually launched here, they get launched by
	 * launch_apply_workers(), called by the database's per-db static worker.
	 */
	for (off = 0; off < bdr_max_workers; off++)
	{
		BdrConnectionConfig *cfg = bdr_connection_configs[off];
		BdrWorker	   *shmworker;
		BdrApplyWorker *worker;
		int				i;
		bool			found_perdb = false;

		if (cfg == NULL || !cfg->is_valid)
			continue;

		shmworker = (BdrWorker *) bdr_worker_shmem_alloc(BDR_WORKER_APPLY, NULL);
		Assert(shmworker->worker_type == BDR_WORKER_APPLY);
		worker = &shmworker->worker_data.apply_worker;
		worker->connection_config_idx = off;
		worker->replay_stop_lsn = InvalidXLogRecPtr;
		worker->forward_changesets = false;

		/*
		 * Now search for the perdb worker belonging to this slot.
		 */
		for (i = 0; i < bdr_max_workers; i++)
		{
			BdrPerdbWorker *perdb;
			BdrWorker *entry = &BdrWorkerCtl->slots[i];

			if (entry->worker_type != BDR_WORKER_PERDB)
				continue;

			perdb = &entry->worker_data.perdb_worker;

			if (strcmp(NameStr(perdb->dbname), cfg->dbname) != 0)
				continue;

			/*
			 * Remember how many connections there are for this node. This
			 * will, e.g., be used to determine the quorum for ddl locks and
			 * sequencer votes.
			 */
			perdb->nnodes++;
			found_perdb = true;
			worker->perdb_worker_off = i;
			break;
		}

		if (!found_perdb)
			elog(ERROR, "couldn't find perdb entry for apply worker");

		/*
		 * If this is a postmaster restart, don't register the worker a second
		 * time when the per-db worker starts up.
		 */
		worker->bgw_is_registered = bdr_is_restart;
	}

	/*
	 * Make sure that we don't register workers if the postmaster restarts and
	 * clears shmem, by keeping a record that we've asked for registration once
	 * already.
	 */
	bdr_is_restart = true;

	/*
	 * We might need to re-populate shared memory after a postmaster restart.
	 * So we don't free the bdr_startup_context or its contents.
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
 */
BdrWorker*
bdr_worker_shmem_alloc(BdrWorkerType worker_type, uint32 *ctl_idx)
{
	int i;
	LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *new_entry = &BdrWorkerCtl->slots[i];
		if (new_entry->worker_type == BDR_WORKER_EMPTY_SLOT)
		{
			memset(new_entry, 0, sizeof(BdrWorker));
			new_entry->worker_type = worker_type;
			LWLockRelease(BdrWorkerCtl->lock);
			if (ctl_idx)
				*ctl_idx = i;
			return new_entry;
		}
	}
	LWLockRelease(BdrWorkerCtl->lock);
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
 */
bool
bdr_is_bdr_activated_db(void)
{
	const char *mydb;
	static int	is_bdr_db = -1;
	int			i;

	/* won't know until we've forked/execed */
	Assert(IsUnderPostmaster);

	/* potentially need to access syscaches */
	Assert(IsTransactionState());

	/* fast path after the first call */
	if (is_bdr_db != -1)
		return is_bdr_db;

	/*
	 * Accessing the database name via MyProcPort is faster, but only works in
	 * user initiated connections. Not background workers.
	 */
	if (MyProcPort != NULL)
		mydb = MyProcPort->database_name;
	else
		mydb = get_database_name(MyDatabaseId);

	/* Look for the perdb worker's entries, they have the database name */
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
			is_bdr_db = true;
			return true;
		}
	}

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

			/*
			 * XXX: we might not wan't to set is_bdr_db here because slot can
			 * be dropped without restart.
			 */
			is_bdr_db = true;
			return true;
		}
	}

	LWLockRelease(ReplicationSlotControlLock);

	is_bdr_db = false;
	return false;
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
	List	   *connames;
	ListCell   *c;
	MemoryContext old_context;
	char	   *connections_tmp;

	char	  **used_databases;
	char      **database_initcons;
	Size		num_used_databases = 0;
	int			connection_config_idx;
	BackgroundWorker bgw;
	uint32		off;

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
							"max number of bdr connections + distinct databases. -1 auto-calculates.",
							NULL,
							&bdr_max_workers,
							-1, -1, 100,
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

	DefineCustomBoolVariable("bdr.init_from_basedump",
							 "Internal. Set during local initialization from basebackup only",
							 NULL,
							 &bdr_init_from_basedump,
							 false,
							 PGC_BACKEND,
							 0,
							 NULL, NULL, NULL);
	bdr_label_init();

	/* if nothing is configured, we're done */
	if (connections == NULL)
	{
		/* If worker count autoconfigured, use zero */
		if (bdr_max_workers == -1)
			bdr_max_workers = 0;
		goto out;
	}

	/* Copy 'connections' guc so SplitIdentifierString can modify it in-place */
	connections_tmp = pstrdup(connections);

	/* Get the list of BDR connection names to iterate over. */
	if (!SplitIdentifierString(connections_tmp, ',', &connames))
	{
		/* syntax error in list */
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax for \"bdr.connections\"")));
	}

	/*
	 * If bdr.max_connections is -1, the default, auto-set it with the
	 * most workers we might need with the current number of connections
	 * configured. Per-db workers are due to use shmem too, so we might
	 * have up to one per-db worker for each configured connection if
	 * each is on a different DB.
	 */
	if (bdr_max_workers == -1)
	{
		bdr_max_workers = list_length(connames) * 3;
		elog(DEBUG1, "bdr: bdr_max_workers unset, configuring for %d workers",
				bdr_max_workers);
	}

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
	 * Allocate a shared memory segment to store the bgworker connection
	 * information we must pass to each worker we launch.
	 *
	 * This registers a hook on shm initialization, bdr_worker_shmem_startup(),
	 * which populates the shm segment with configured apply workers using data
	 * in bdr_connection_configs.
	 */
	bdr_worker_alloc_shmem_segment();

	/* Allocate space for BDR connection GUCs */
	bdr_connection_configs = (BdrConnectionConfig**)
		palloc0(bdr_max_workers * sizeof(BdrConnectionConfig*));

	/* Names of all databases we're going to be doing BDR for */
	used_databases = palloc0(sizeof(char *) * list_length(connames));
	/*
	 * For each db named in used_databases, the corresponding index is the name
	 * of the conn with bdr_init_replica=t if any.
	 */
	database_initcons = palloc0(sizeof(char *) * list_length(connames));

	/*
	 * Read all connections, create/validate parameters for them and do sanity
	 * checks as we go.
	 */
	connection_config_idx = 0;
	foreach(c, connames)
	{
		char		   *name;
		name = (char *) lfirst(c);

		if (!bdr_create_con_gucs(name, used_databases, &num_used_databases,
								 database_initcons,
								 &bdr_connection_configs[connection_config_idx]))
			continue;

		Assert(bdr_connection_configs[connection_config_idx] != NULL);
		connection_config_idx++;
	}

	/*
	 * Free the connames list cells. The strings are just pointers into
	 * 'connections' and must not be freed'd.
	 */
	list_free(connames);
	connames = NIL;

	/*
	 * We've ensured there are no duplicate init connections, no need to
	 * remember which conn is the bdr_init_replica conn anymore. The contents
	 * are just pointers into connections_tmp so we don't want to free them.
	 */
	pfree(database_initcons);

	/*
	 * Copy the list of used databases into a global where we can
	 * use it for registering the per-database workers during shmem init.
	 */
	bdr_distinct_dbnames = palloc(sizeof(char*)*num_used_databases);
	memcpy(bdr_distinct_dbnames, used_databases,
		   sizeof(char*)*num_used_databases);
	bdr_distinct_dbnames_count = num_used_databases;
	pfree(used_databases);
	num_used_databases = 0;
	used_databases = NULL;

	/*
	 * Register the per-db workers and assign them an index in shmem. The
	 * memory doesn't actually exist yet, it'll be allocated in shmem init.
	 *
	 * No protection against multiple launches is requried because this
	 * only runs once, in _PG_init.
	 */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	/* TODO: For EXEC_BACKEND we must use bgw_library_name & bgw_function_name */
	bgw.bgw_main = bdr_perdb_worker_main;
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	for (off = 0; off < bdr_distinct_dbnames_count; off++)
	{
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "bdr: %s", bdr_distinct_dbnames[off]);
		/*
		 * This index into BdrWorkerCtl shmem hasn't been populated yet. It'll
		 * be set up in bdr_worker_shmem_create_workers .
		 */
		bgw.bgw_main_arg = Int32GetDatum(off);
		RegisterBackgroundWorker(&bgw);
	}

	EmitWarningsOnPlaceholders("bdr");

	pfree(connections_tmp);

out:

	/*
	 * initialize other modules that need shared memory
	 *
	 * Do so even if we haven't any remote nodes setup, the shared memory might
	 * still be needed for some sql callable functions or such.
	 */

	/* register a slot for every remote node */
	bdr_count_shmem_init(bdr_max_workers);
	bdr_executor_init();
#ifdef BUILDING_BDR
	bdr_sequencer_shmem_init(bdr_max_workers, bdr_distinct_dbnames_count);
#endif
	bdr_locks_shmem_init(bdr_distinct_dbnames_count);
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
 */
void
bdr_maintain_schema(void)
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

	/* create required extension if they don't exists yet */
	if (btree_gist_oid == InvalidOid)
	{
		CreateExtensionStmt create_stmt;

		create_stmt.if_not_exists = false;
		create_stmt.options = NIL;
		create_stmt.extname = (char *)"btree_gist";
		CreateExtension(&create_stmt);
	}
	else
	{
		AlterExtensionStmt alter_stmt;

		/* TODO: only do this if necessary */
		alter_stmt.options = NIL;
		alter_stmt.extname = (char *)"btree_gist";
		ExecAlterExtensionStmt(&alter_stmt);
	}

	if (bdr_oid == InvalidOid)
	{
		CreateExtensionStmt create_stmt;

		create_stmt.if_not_exists = false;
		create_stmt.options = NIL;
		create_stmt.extname = (char *)"bdr";
		CreateExtension(&create_stmt);
	}
	else
	{
		AlterExtensionStmt alter_stmt;

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

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = ObjectIdGetDatum(GetSystemIdentifier());
	values[1] = ObjectIdGetDatum(ThisTimeLineID);
	values[2] = ObjectIdGetDatum(MyDatabaseId);

	returnTuple = heap_form_tuple(tupleDesc, values, isnull);

	PG_RETURN_DATUM(HeapTupleGetDatum(returnTuple));
}
