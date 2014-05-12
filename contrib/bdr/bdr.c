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

#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port.h"

#include "access/committs.h"
#include "access/heapam.h"
#include "access/xact.h"

#include "catalog/catversion.h"
#include "catalog/namespace.h"
#include "catalog/pg_extension.h"

#include "commands/extension.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "postmaster/bgworker.h"

#include "replication/replication_identifier.h"

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

/* Really should be private to bdr_apply.c */
extern bool exit_worker;

static int   n_configured_bdr_nodes = 0;
ResourceOwner bdr_saved_resowner;
static bool bdr_is_restart = false;
Oid   BdrNodesRelid;
Oid   BdrConflictHistoryRelId;
BdrConnectionConfig  **bdr_connection_configs;

/* GUC storage */
static char *connections = NULL;
static char *bdr_synchronous_commit = NULL;
int bdr_default_apply_delay;
int bdr_max_workers;
static bool bdr_skip_ddl_replication;

/*
 * These globals are valid only for apply bgworkers, not for
 * bdr running in the postmaster or for per-db workers.
 *
 * TODO: move into bdr_apply.c when bdr_apply_main moved.
 */
extern BdrApplyWorker *bdr_apply_worker;
extern BdrConnectionConfig *bdr_apply_config;

/* shmem init hook to chain to on startup, if any */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* shortcut for finding the the worker shmem block */
BdrWorkerControl *BdrWorkerCtl = NULL;

PG_MODULE_MAGIC;

void		_PG_init(void);
static void bdr_maintain_schema(void);
static void bdr_worker_shmem_startup(void);
static void bdr_worker_shmem_create_workers(void);

Datum bdr_apply_pause(PG_FUNCTION_ARGS);
Datum bdr_apply_resume(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bdr_apply_pause);
PG_FUNCTION_INFO_V1(bdr_apply_resume);

/*
 * Converts an int64 to network byte order.
 */
static void
bdr_sendint64(int64 i, char *buf)
{
	uint32		n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32) (i >> 32);
	n32 = htonl(n32);
	memcpy(&buf[0], &n32, 4);

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = htonl(n32);
	memcpy(&buf[4], &n32, 4);
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
bdr_send_feedback(PGconn *conn, XLogRecPtr blockpos, int64 now, bool replyRequested,
			 bool force)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;
	static XLogRecPtr lastpos = InvalidXLogRecPtr;

	Assert(blockpos != InvalidXLogRecPtr);

	if (!force && (blockpos <= lastpos))
		return true;

	if (blockpos < lastpos)
		blockpos = lastpos;

	replybuf[len] = 'r';
	len += 1;
	bdr_sendint64(blockpos, &replybuf[len]);		/* write */
	len += 8;
	bdr_sendint64(blockpos, &replybuf[len]);		/* flush */
	len += 8;
	bdr_sendint64(blockpos, &replybuf[len]);		/* apply */
	len += 8;
	bdr_sendint64(now, &replybuf[len]);				/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0;		/* replyRequested */
	len += 1;

	elog(DEBUG1, "sending feedback (force %d, reply requested %d) to %X/%X",
		 force, replyRequested,
		 (uint32) (blockpos >> 32), (uint32) blockpos);

	lastpos = blockpos;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		ereport(ERROR,
				(errmsg("could not send feedback packet: %s",
						PQerrorMessage(conn))));
		return false;
	}

	return true;
}

static void
bdr_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	exit_worker = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
bdr_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
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
			char* remote_ident, size_t remote_ident_length,
			NameData* slot_name,
			uint64* remote_sysid_i, TimeLineID *remote_tlid_i)
{
	PGconn	   *streamConn;
	PGresult   *res;
	StringInfoData query;
	char	   *remote_sysid;
	char	   *remote_tlid;
#ifdef NOT_USED
	char	   *remote_dbname;
#endif
	char	   *remote_dboid;
	Oid			remote_dboid_i;
	char		local_sysid[32];
	NameData	replication_name;

	initStringInfo(&query);
	NameStr(replication_name)[0] = '\0';

	streamConn = PQconnectdb(conninfo_repl);
	if (PQstatus(streamConn) != CONNECTION_OK)
	{
		ereport(FATAL,
				(errmsg("could not connect to the primary server: %s",
						PQerrorMessage(streamConn))));
	}

	res = PQexec(streamConn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(FATAL, "could not send replication command \"%s\": %s",
			 "IDENTIFY_SYSTEM", PQerrorMessage(streamConn));
	}
	if (PQntuples(res) != 1 || PQnfields(res) != 5)
	{
		elog(FATAL, "could not identify system: got %d rows and %d fields, expected %d rows and %d fields\n",
			 PQntuples(res), PQnfields(res), 1, 5);
	}

	remote_sysid = PQgetvalue(res, 0, 0);
	remote_tlid = PQgetvalue(res, 0, 1);
#ifdef NOT_USED
	remote_dbname = PQgetvalue(res, 0, 3);
#endif
	remote_dboid = PQgetvalue(res, 0, 4);

	if (sscanf(remote_sysid, UINT64_FORMAT, remote_sysid_i) != 1)
		elog(ERROR, "could not parse remote sysid %s", remote_sysid);

	if (sscanf(remote_tlid, "%u", remote_tlid_i) != 1)
		elog(ERROR, "could not parse remote tlid %s", remote_tlid);

	if (sscanf(remote_dboid, "%u", &remote_dboid_i) != 1)
		elog(ERROR, "could not parse remote database OID %s", remote_dboid);

	snprintf(local_sysid, sizeof(local_sysid), UINT64_FORMAT,
			 GetSystemIdentifier());

	if (strcmp(remote_sysid, local_sysid) == 0)
	{
		ereport(FATAL,
				(errmsg("system identifiers must differ between the nodes"),
				 errdetail("Both system identifiers are %s.", remote_sysid)));
	}
	else
		elog(DEBUG1, "local sysid %s, remote: %s",
			 local_sysid, remote_sysid);

	/*
	 * build slot name.
	 *
	 * FIXME: This might truncate the identifier if replication_name is
	 * somewhat longer...
	 */
	snprintf(NameStr(*slot_name), NAMEDATALEN, BDR_SLOT_NAME_FORMAT,
			 remote_dboid_i, local_sysid, ThisTimeLineID,
			 MyDatabaseId, NameStr(replication_name));
	NameStr(*slot_name)[NAMEDATALEN - 1] = '\0';

	/*
	 * Build replication identifier.
	 */
	snprintf(remote_ident, remote_ident_length,
			 BDR_NODE_ID_FORMAT,
			 *remote_sysid_i, *remote_tlid_i, remote_dboid_i, MyDatabaseId,
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
					 NameStr(*slot_name), "bdr_output");
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
static void
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
	bdr_maintain_schema();

	/* always work in our own schema */
	SetConfigOption("search_path", "bdr, pg_catalog",
					PGC_BACKEND, PGC_S_OVERRIDE);

	/* setup synchronous commit according to the user's wishes */
	if (bdr_synchronous_commit != NULL)
		SetConfigOption("synchronous_commit", bdr_synchronous_commit,
						PGC_BACKEND, PGC_S_OVERRIDE);	/* other context? */
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
bdr_establish_connection_and_slot(BdrConnectionConfig *cfg, Name out_slot_name,
	uint64 *out_sysid, TimeLineID* out_timeline, RepNodeId
	*out_replication_identifier, char **out_snapshot)
{
	char		conninfo_repl[MAXCONNINFO + 75];
	char		remote_ident[256];
	PGconn	   *streamConn;

	snprintf(conninfo_repl, sizeof(conninfo_repl),
			 "%s replication=database fallback_application_name=bdr",
			 cfg->dsn);

	/* Establish BDR conn and IDENTIFY_SYSTEM */
	streamConn = bdr_connect(
		conninfo_repl,
		remote_ident, sizeof(remote_ident),
		out_slot_name, out_sysid, out_timeline
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
 * Entry point and main loop for a BDR apply worker.
 *
 * Responsible for establishing a replication connection, creating slots,
 * replaying.
 *
 * TODO: move to bdr_apply.c
 */
void
bdr_apply_main(Datum main_arg)
{
	PGconn	   *streamConn;
	PGresult   *res;
	int			fd;
	StringInfoData query;
	XLogRecPtr	last_received = InvalidXLogRecPtr;
	char	   *sqlstate;
	RepNodeId	replication_identifier;
	XLogRecPtr	start_from;
	NameData	slot_name;
	BdrWorker  *bdr_worker_slot;

	Assert(IsBackgroundWorker);

	initStringInfo(&query);

	bdr_worker_slot = &BdrWorkerCtl->slots[ DatumGetInt32(main_arg) ];
	Assert(bdr_worker_slot->worker_type == BDR_WORKER_APPLY);
	bdr_apply_worker = &bdr_worker_slot->worker_data.apply_worker;

	bdr_apply_config = bdr_connection_configs[bdr_apply_worker->connection_config_idx];
	Assert(bdr_apply_config != NULL);

	bdr_worker_init(NameStr(bdr_apply_config->dbname));

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr apply top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	elog(DEBUG1, "%s initialized on %s",
		 MyBgworkerEntry->bgw_name, NameStr(bdr_apply_config->dbname));

	streamConn = bdr_establish_connection_and_slot(
		bdr_apply_config, &slot_name, &bdr_apply_worker->sysid,
		&bdr_apply_worker->timeline, &replication_identifier, NULL);

	bdr_apply_worker->origin_id = replication_identifier;

	/* initialize stat subsystem, our id won't change further */
	bdr_count_set_current_node(replication_identifier);

	/*
	 * tell replication_identifier.c about our identifier so it can cache the
	 * search in shared memory.
	 */
	SetupCachedReplicationIdentifier(replication_identifier);

	/*
	 * Check whether we already replayed something so we don't replay it
	 * multiple times.
	 */
	start_from = RemoteCommitFromCachedReplicationIdentifier();

	elog(INFO, "starting up replication at %u from %X/%X",
		 replication_identifier,
		 (uint32) (start_from >> 32), (uint32) start_from);

	resetStringInfo(&query);
	appendStringInfo(&query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X (",
					 NameStr(slot_name), (uint32) (start_from >> 32),
					 (uint32) start_from);
	appendStringInfo(&query, "pg_version '%u'", PG_VERSION_NUM);
	appendStringInfo(&query, ", pg_catversion '%u'", CATALOG_VERSION_NO);
	appendStringInfo(&query, ", bdr_version '%u'", BDR_VERSION_NUM);
	appendStringInfo(&query, ", sizeof_int '%zu'", sizeof(int));
	appendStringInfo(&query, ", sizeof_long '%zu'", sizeof(long));
	appendStringInfo(&query, ", sizeof_datum '%zu'", sizeof(Datum));
	appendStringInfo(&query, ", maxalign '%d'", MAXIMUM_ALIGNOF);
	appendStringInfo(&query, ", float4_byval '%d'", bdr_get_float4byval());
	appendStringInfo(&query, ", float8_byval '%d'", bdr_get_float8byval());
	appendStringInfo(&query, ", integer_datetimes '%d'", bdr_get_integer_timestamps());
	appendStringInfo(&query, ", bigendian '%d'", bdr_get_bigendian());
	appendStringInfo(&query, ", db_encoding '%s'", GetDatabaseEncodingName());
	if (bdr_apply_worker->forward_changesets)
		appendStringInfo(&query, ", forward_changesets 't'");

	appendStringInfoChar(&query, ')');
	res = PQexec(streamConn, query.data);

	sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		elog(FATAL, "could not send replication command \"%s\": %s\n, sqlstate: %s",
			 query.data, PQresultErrorMessage(res), sqlstate);
	}
	PQclear(res);

	fd = PQsocket(streamConn);

	replication_origin_id = replication_identifier;

	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	bdr_conflict_logging_startup();

	while (!exit_worker)
	{
		/* int		 ret; */
		int			rc;
		int			r;
		char	   *copybuf = NULL;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatchOrSocket(&MyProc->procLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET |
							   WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   fd, 1000L);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (PQstatus(streamConn) == CONNECTION_BAD)
		{
			bdr_count_disconnect();
			elog(ERROR, "connection to other side has died");
		}

		if (rc & WL_SOCKET_READABLE)
			PQconsumeInput(streamConn);

		for (;;)
		{
			if (exit_worker)
				break;

			r = PQgetCopyData(streamConn, &copybuf, 1);

			if (r == -1)
			{
				elog(DEBUG1, "data stream ended");
				return;
			}
			else if (r == -2)
			{
				elog(ERROR, "could not read COPY data: %s",
					 PQerrorMessage(streamConn));
			}
			else if (r < 0)
				elog(ERROR, "invalid COPY status %d", r);
			else if (r == 0)
			{
				/* need to wait for new data */
				break;
			}
			else
			{
				int c;
				StringInfoData s;

				initStringInfo(&s);
				s.data = copybuf;
				s.len = r;

				c = pq_getmsgbyte(&s);

				if (c == 'w')
				{
					XLogRecPtr	start_lsn;
					XLogRecPtr	end_lsn;

					start_lsn = pq_getmsgint64(&s);
					end_lsn = pq_getmsgint64(&s);
					pq_getmsgint64(&s); /* sendTime */

					if (last_received < start_lsn)
						last_received = start_lsn;

					if (last_received < end_lsn)
						last_received = end_lsn;

					bdr_process_remote_action(&s);
				}
				else if (c == 'k')
				{
					XLogRecPtr	temp;

					temp = pq_getmsgint64(&s);

					bdr_send_feedback(streamConn, temp,
								 GetCurrentTimestamp(), false, true);
				}
				/* other message types are purposefully ignored */
			}

			MemoryContextResetAndDeleteChildren(MessageContext);
		}

		/* confirm all writes at once */
		/*
		 * FIXME: we should only do that after an xlog flush... Yuck.
		 */
		if (last_received != InvalidXLogRecPtr)
			bdr_send_feedback(streamConn, last_received,
						 GetCurrentTimestamp(), false, false);

		/*
		 * If the user has paused replication with bdr_apply_pause(), we
		 * wait on our procLatch until pg_bdr_apply_resume() unsets the
		 * flag in shmem. We don't pause until the end of the current
		 * transaction, to avoid sleeping with locks held.
		 *
		 * XXX With the 1s timeout below, we don't risk delaying the
		 * resumption too much. But it would be better to use a global
		 * latch that can be set by pg_bdr_apply_resume(), and not have
		 * to wake up so often.
		 */

		while (BdrWorkerCtl->pause_apply && !IsTransactionState())
		{
			ResetLatch(&MyProc->procLatch);
			rc = WaitLatch(&MyProc->procLatch, WL_TIMEOUT, 1000L);
		}
	}

	proc_exit(0);
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

	Assert(process_shared_preload_libraries_in_progress);

	opts = palloc0(sizeof(BdrConnectionConfig));
	opts->is_valid = false;
	*out_config = opts;

	strncpy(NameStr(opts->name), name, NAMEDATALEN);
	NameStr(opts->name)[NAMEDATALEN-1] = '\0';

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
				(errmsg("bdr %s: error in dsn: %s", name, str)));
	}

	cur_option = options;
	while (cur_option->keyword != NULL)
	{
		if (strcmp(cur_option->keyword, "dbname") == 0)
		{
			if (cur_option->val == NULL)
				ereport(ERROR, (errmsg("bdr %s: no dbname set", name)));

			strncpy(NameStr(opts->dbname), cur_option->val,
					NAMEDATALEN);
			NameStr(opts->dbname)[NAMEDATALEN-1] = '\0';
			elog(DEBUG2, "bdr %s: dbname=%s", name, NameStr(opts->dbname));
		}

		if (cur_option->val != NULL)
		{
			elog(DEBUG3, "bdr %s: opt %s, val: %s",
				 name, cur_option->keyword, cur_option->val);
		}
		cur_option++;
	}

	/* cleanup */
	PQconninfoFree(options);

	/*
	 * If this is a DB name we haven't seen yet, add it to our set of known
	 * DBs.
	 */
	for (off = 0; off < *num_used_databases; off++)
	{
		if (strcmp(NameStr(opts->dbname), used_databases[off]) == 0)
			break;
	}

	if (off == *num_used_databases)
	{
		/* Didn't find a match, add new db name */
		used_databases[(*num_used_databases)++] =
			pstrdup(NameStr(opts->dbname));
		elog(DEBUG2, "bdr %s: Saw new database %s, now %i known dbs",
			 name, NameStr(opts->dbname), (int)(*num_used_databases));
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
					(errmsg("Connections %s and %s on database %s both have bdr_init_replica enabled, cannot continue",
							name, database_initcons[off], used_databases[off])));
		else
			database_initcons[off] = name; /* no need to pstrdup, see _PG_init */
	}

	opts->is_valid = true;

	/* optname vars intentionally leaked, see above */
	return true;
}


/*
 * Launch a dynamic bgworker to run bdr_apply_main for each bdr connection on
 * the database identified by dbname.
 *
 * Scans the BdrWorkerCtl shmem segment for workers of type BDR_WORKER_APPLY
 * with a matching database name and launches them.
 */
static List*
bdr_launch_apply_workers(char *dbname)
{
	List             *apply_workers = NIL;
	BackgroundWorker  apply_worker;
	int				  i;

	Assert(IsBackgroundWorker);

	/* Common apply worker values */
	apply_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	apply_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	/* TODO: For EXEC_BACKEND we must use bgw_library_name & bgw_function_name */
	apply_worker.bgw_main = bdr_apply_main;
	apply_worker.bgw_restart_time = 5;
	apply_worker.bgw_notify_pid = 0;

	/* Launch apply workers */
	LWLockAcquire(BdrWorkerCtl->lock, LW_SHARED);
	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrWorker *worker = &BdrWorkerCtl->slots[i];

		/*
		 * Work around issue described in
		 * http://www.postgresql.org/message-id/534E250F.2060705@2ndquadrant.com by
		 * suppressing bgworker relaunch if shm was re-initialized after a
		 * postmaster restart.
		 *
		 * FIXME remove after bgworker behaviour change to auto unregister
		 * bgworkers on restart.
		 */
		if (!BdrWorkerCtl->launch_workers)
			break;

		switch(worker->worker_type)
		{
			case BDR_WORKER_APPLY:
				{
					BdrApplyWorker *con = &worker->worker_data.apply_worker;
					BdrConnectionConfig *cfg =
						bdr_connection_configs[con->connection_config_idx];
					Assert(cfg != NULL);
					if ( strcmp(NameStr(cfg->dbname), dbname) == 0 )
					{
						/* It's an apply worker for our DB; register it */
						BackgroundWorkerHandle *bgw_handle;

						snprintf(apply_worker.bgw_name, BGW_MAXLEN,
								 "bdr apply: %s", NameStr(cfg->name));
						apply_worker.bgw_main_arg = Int32GetDatum(i);

						if (!RegisterDynamicBackgroundWorker(&apply_worker,
															 &bgw_handle))
						{
							ereport(ERROR,
									(errmsg("bdr: Failed to register background worker"
											" %s, see previous log messages",
											NameStr(cfg->name))));
						}
						apply_workers = lcons(bgw_handle, apply_workers);
					}
				}
				break;
			case BDR_WORKER_EMPTY_SLOT:
			case BDR_WORKER_PERDB:
				/* Nothing to do; switch only so we get warnings for insane cases */
				break;
			default:
				/* Bogus value */
				elog(FATAL, "Unhandled BdrWorkerType case %i, memory corruption?",
					 worker->worker_type);
				break;
		}
	}
	LWLockRelease(BdrWorkerCtl->lock);

	return apply_workers;
}

/*
 * Return the total number of BDR workers in the group.
 *
 * Sequence support relies on this.
 *
 * TODO: Use bdr.bdr_nodes to get this information. For now we assume that each
 * node has connections to all other nodes and that all configured connections
 * are correct and valid, so the number of nodes is equal to the number of
 * configured connections.
 */
int
bdr_node_count()
{
	return n_configured_bdr_nodes;
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
static void
bdr_perdb_worker_main(Datum main_arg)
{
	int				  rc;
	List			 *apply_workers;
	ListCell		 *c;
	BdrPerdbWorker   *bdr_perdb_worker;

	Assert(IsBackgroundWorker);

	/* FIXME: won't work with EXEC_BACKEND, change to index into shm array */
	bdr_perdb_worker = (BdrPerdbWorker *) DatumGetPointer(main_arg);

	bdr_worker_init(NameStr(bdr_perdb_worker->dbname));

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	/* Do we need to init the local DB from a remote node? */
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
		BackgroundWorkerHandle *h = (BackgroundWorkerHandle*)lfirst(c);
		pfree(h);
	}

	elog(DEBUG1, "BDR starting sequencer on db \"%s\"",
		 NameStr(bdr_perdb_worker->dbname));

	/* initialize sequencer */
	bdr_sequencer_init(bdr_perdb_worker->seq_slot);

	while (!exit_worker)
	{
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
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   180000L);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* check whether we need to vote */
		bdr_sequencer_vote();

		/* check whether any of our elections needs to be tallied */
		bdr_sequencer_tally();

		/* check all bdr sequences for used up chunks */
		bdr_sequencer_fill_sequences();

		/* check whether we need to start new elections */
		bdr_sequencer_start_elections();
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	proc_exit(0);
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
		/* If it's a restart, don't actually re-register bgworkers */
		BdrWorkerCtl->launch_workers = !bdr_is_restart;
		/* Any subsequent execution must now be a restart */
		bdr_is_restart = true;

		/*
		 * Now that the shm segment is initialized, we can populate it with
		 * BdrWorker entries for the connections we created GUCs for during
		 * _PG_init.
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
	int off;

	/*
	 * Create a BdrApplyWorker for each valid BdrConnectionConfig found during
	 * _PG_init so that the per-db worker will register it for startup after
	 * performing any BDR initialisation work.
	 *
	 * Use of shared memory for this is required for EXEC_BACKEND (windows)
	 * where we can't share postmaster memory, and for when we're launching a
	 * bgworker from another bgworker where the fork() from postmaster doesn't
	 * provide access to the launching bgworker's memory.
	 */
	for (off = 0; off < bdr_max_workers; off++)
	{
		BdrConnectionConfig *cfg = bdr_connection_configs[off];
		BdrWorker	   *shmworker;
		BdrApplyWorker *worker;

		if (cfg == NULL || !cfg->is_valid)
			continue;

		shmworker = (BdrWorker *) bdr_worker_shmem_alloc(BDR_WORKER_APPLY);
		Assert(shmworker->worker_type == BDR_WORKER_APPLY);
		worker = &shmworker->worker_data.apply_worker;
		worker->connection_config_idx = off;
		worker->replay_stop_lsn = InvalidXLogRecPtr;
		worker->forward_changesets = false;
		n_configured_bdr_nodes++;
	}

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
 * To release a block, use bdr_worker_shmem_release(...)
 */
BdrWorker*
bdr_worker_shmem_alloc(BdrWorkerType worker_type)
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
	BackgroundWorker perdb_worker;
	List	   *connames;
	ListCell   *c;
	MemoryContext old_context;
	size_t      off;
	char	   *connections_tmp;

	char	  **used_databases;
	char      **database_initcons;
	Size		num_used_databases = 0;
	int			connection_config_idx;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("bdr can only be loaded via shared_preload_libraries")));

	if (!commit_ts_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bdr requires \"track_commit_timestamp\" to be enabled")));

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
	DefineCustomStringVariable("bdr.synchronous_commit",
							   "bdr specific synchronous commit value",
							   NULL,
							   &bdr_synchronous_commit,
							   NULL, PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

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

	DefineCustomBoolVariable("bdr.skip_ddl_replication",
							 "Internal. Set during local restore during init_replica only",
							 NULL,
							 &bdr_skip_ddl_replication,
							 false,
							 PGC_BACKEND,
							 0,
							 NULL, NULL, NULL);

	bdr_conflict_logging_create_gucs();

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
		bdr_max_workers = list_length(connames) * 2;
		elog(DEBUG1, "bdr: bdr_max_workers unset, configuring for %d workers",
				bdr_max_workers);
	}

	/* Set up a ProcessUtility_hook to stop unsupported commands being run */
	init_bdr_commandfilter();

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
	 * Read all connections and create their BdrApplyWorker structs, validating
	 * parameters and sanity checking as we go. The structs are palloc'd, but
	 * will be copied into shared memory and free'd during shm init.
	 */
	connection_config_idx = 0;
	foreach(c, connames)
	{
		char		   *name;
		BdrApplyWorker *apply_worker;

		apply_worker = (BdrApplyWorker *) palloc0(sizeof(BdrApplyWorker));
		apply_worker->forward_changesets = false;
		apply_worker->replay_stop_lsn = InvalidXLogRecPtr;
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
	 * We now need to register one static bgworker per database.  When started,
	 * this worker will continue setup - doing any required initialization of
	 * the database, then registering dynamic bgworkers for the DB's individual
	 * BDR connections.
	 *
	 * These workers get started *after* the shm init callback is run, we're
	 * just registering them for launch.
	 *
	 * If we ever want to support dynamically adding/removing DBs from BDR at
	 * runtime, this'll need to move into a static bgworker or code called by
	 * the shm startup hook and a guc reload hook.
	 *
	 * TODO: Move this into the shared memory based init.
	 */
	perdb_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	perdb_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	/* TODO: For EXEC_BACKEND we must use bgw_library_name & bgw_function_name */
	perdb_worker.bgw_main = bdr_perdb_worker_main;
	perdb_worker.bgw_restart_time = 5;
	perdb_worker.bgw_notify_pid = 0;

	for (off = 0; off < num_used_databases; off++)
	{
		/* Start a worker for this db */
		BdrPerdbWorker *con = palloc(sizeof(BdrPerdbWorker));

		strncpy(NameStr(con->dbname), used_databases[off], NAMEDATALEN);
		NameStr(con->dbname)[NAMEDATALEN-1] = '\0';

		con->seq_slot = off;

		elog(DEBUG1, "starting bdr database worker for db %s", NameStr(con->dbname));
		snprintf(perdb_worker.bgw_name, BGW_MAXLEN,
				 "bdr: %s", NameStr(con->dbname));
		perdb_worker.bgw_main_arg = PointerGetDatum(con);
		RegisterBackgroundWorker(&perdb_worker);
	}

	EmitWarningsOnPlaceholders("bdr");

	for (off = 0; off < num_used_databases; off++)
		pfree(used_databases[off]);
	pfree(used_databases);

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
	bdr_sequencer_shmem_init(bdr_max_workers, num_used_databases);

	MemoryContextSwitchTo(old_context);
}

static Oid
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
 */
static void
bdr_maintain_schema(void)
{
	Relation	extrel;
	Oid			btree_gist_oid;
	Oid			bdr_oid;
	Oid			schema_oid;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

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
	Assert(schema_oid != InvalidOid);

	QueuedDDLCommandsRelid =
		bdr_lookup_relid("bdr_queued_commands", schema_oid);

	BdrSequenceValuesRelid =
		bdr_lookup_relid("bdr_sequence_values", schema_oid);

	BdrSequenceElectionsRelid =
		bdr_lookup_relid("bdr_sequence_elections", schema_oid);

	BdrVotesRelid = bdr_lookup_relid("bdr_votes", schema_oid);

	BdrNodesRelid = bdr_lookup_relid("bdr_nodes", schema_oid);

	BdrConflictHistoryRelId =
		bdr_lookup_relid("bdr_conflict_history", schema_oid);

	QueuedDropsRelid = bdr_lookup_relid("bdr_queued_drops", schema_oid);

	elog(DEBUG1, "bdr.bdr_queued_commands OID set to %u",
		 QueuedDDLCommandsRelid);
	elog(DEBUG1, "bdr.bdr_queued_drops OID set to %u",
		 QueuedDropsRelid);

	bdr_conflict_handlers_init();

	PopActiveSnapshot();
	CommitTransactionCommand();
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
