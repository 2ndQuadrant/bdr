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

#include "libpq-fe.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port.h"

#include "access/committs.h"
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

/* TODO: move to bdr_apply.c when bdr_apply_main is moved */
extern bool			exit_worker;
extern uint64		origin_sysid;
extern TimeLineID	origin_timeline;
extern Oid			origin_dboid;
/* end externs for bdr apply state */

static int   n_configured_bdr_nodes = 0;
ResourceOwner bdr_saved_resowner;
static bool bdr_is_restart = false;
Oid   BdrNodesRelid;
Oid   BdrConflictHistoryRelId;
Oid   BdrLocksRelid;
Oid   BdrLocksByOwnerRelid;

BdrConnectionConfig  **bdr_connection_configs;
/* All databases for which BDR is configured, valid after _PG_init */
char **bdr_distinct_dbnames;
uint32 bdr_distinct_dbnames_count = 0;

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
Datum bdr_version(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bdr_apply_pause);
PG_FUNCTION_INFO_V1(bdr_apply_resume);
PG_FUNCTION_INFO_V1(bdr_version);


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

	elog(DEBUG2, "sending feedback (force %d, reply requested %d) to %X/%X",
		 force, replyRequested,
		 (uint32) (blockpos >> 32), (uint32) blockpos);

	lastpos = blockpos;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not send feedback packet: %s",
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
			uint64* remote_sysid_i, TimeLineID *remote_tlid_i,
			Oid *remote_dboid_i)
{
	PGconn	   *streamConn;
	PGresult   *res;
	StringInfoData query;
	char	   *remote_sysid;
	char	   *remote_tlid;
	char	   *remote_dbname;
	char	   *remote_dboid;
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
	if (PQntuples(res) != 1 || PQnfields(res) != 5)
	{
		elog(FATAL, "could not identify system: got %d rows and %d fields, expected %d rows and %d fields\n",
			 PQntuples(res), PQnfields(res), 1, 5);
	}

	remote_sysid = PQgetvalue(res, 0, 0);
	remote_tlid = PQgetvalue(res, 0, 1);
	remote_dbname = PQgetvalue(res, 0, 3);
	remote_dboid = PQgetvalue(res, 0, 4);

	if (sscanf(remote_sysid, UINT64_FORMAT, remote_sysid_i) != 1)
		elog(ERROR, "could not parse remote sysid %s", remote_sysid);

	if (sscanf(remote_tlid, "%u", remote_tlid_i) != 1)
		elog(ERROR, "could not parse remote tlid %s", remote_tlid);

	if (sscanf(remote_dboid, "%u", remote_dboid_i) != 1)
		elog(ERROR, "could not parse remote database OID %s", remote_dboid);

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
		elog(DEBUG2, "local node (%s,%u,%u), remote node (%s,%s,%s)",
			 local_sysid, ThisTimeLineID, MyDatabaseId, remote_sysid,
			 remote_tlid, remote_dboid);

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
	bdr_locks_always_allow_writes(true);
	bdr_maintain_schema();
	bdr_locks_always_allow_writes(false);

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
	char	   *copybuf = NULL;
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

	bdr_worker_init(bdr_apply_config->dbname);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr apply top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	elog(DEBUG1, "%s initialized on %s",
		 MyBgworkerEntry->bgw_name, bdr_apply_config->dbname);

	/* Set our local application_name for our SPI connections */
	resetStringInfo(&query);
	appendStringInfo(&query, BDR_LOCALID_FORMAT": %s", BDR_LOCALID_FORMAT_ARGS, "apply");
	if (bdr_apply_worker->forward_changesets)
		appendStringInfoString(&query, " catchup");

	if (bdr_apply_worker->replay_stop_lsn != InvalidXLogRecPtr)
		appendStringInfo(&query, " up to %X/%X",
						 (uint32)(bdr_apply_worker->replay_stop_lsn),
						 (uint32)(bdr_apply_worker->replay_stop_lsn>>32));

	SetConfigOption("application_name", query.data, PGC_USERSET, PGC_S_SESSION);

	/* Form an application_name string to send to the remote end */
	resetStringInfo(&query);
	appendStringInfoString(&query, "receive");

	if (bdr_apply_worker->forward_changesets)
		appendStringInfoString(&query, " catchup");

	if (bdr_apply_worker->replay_stop_lsn != InvalidXLogRecPtr)
		appendStringInfo(&query, " up to %X/%X",
						 (uint32)(bdr_apply_worker->replay_stop_lsn),
						 (uint32)(bdr_apply_worker->replay_stop_lsn>>32));

	/* Make the replication connection to the remote end */
	streamConn = bdr_establish_connection_and_slot(bdr_apply_config,
		query.data, &slot_name, &origin_sysid, &origin_timeline,
		&origin_dboid, &replication_identifier, NULL);


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

	elog(INFO, "starting up replication from %u at %X/%X",
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

	elog(DEBUG3, "Sending replication command: %s", query.data);

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

		MemoryContextSwitchTo(MessageContext);

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

			if (copybuf != NULL)
			{
				PQfreemem(copybuf);
				copybuf = NULL;
			}

			r = PQgetCopyData(streamConn, &copybuf, 1);

			if (r == -1)
			{
				elog(ERROR, "data stream ended");
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

				MemoryContextSwitchTo(MessageContext);

				initStringInfo(&s);
				s.data = copybuf;
				s.len = r;
				s.maxlen = -1;

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
		MemoryContextResetAndDeleteChildren(MessageContext);
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
	char	   *optname_local_dbname = palloc(strlen(name) + 30);

	Assert(process_shared_preload_libraries_in_progress);

	opts = palloc0(sizeof(BdrConnectionConfig));
	opts->is_valid = false;
	*out_config = opts;

	opts->name = pstrdup(name);

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

		switch(worker->worker_type)
		{
			case BDR_WORKER_APPLY:
				{
					BdrApplyWorker *con = &worker->worker_data.apply_worker;
					BdrConnectionConfig *cfg =
						bdr_connection_configs[con->connection_config_idx];
					Assert(cfg != NULL);
					if ( strcmp(cfg->dbname, dbname) == 0 )
					{
						/* It's an apply worker for our DB; register it */
						BackgroundWorkerHandle *bgw_handle;

						if (con->bgw_is_registered)
							/*
							 * This worker was registered on a previous pass;
							 * this is probably a restart of the per-db worker.
							 * Don't register a duplicate.
							 */
							continue;

						snprintf(apply_worker.bgw_name, BGW_MAXLEN,
								 BDR_LOCALID_FORMAT": %s: apply",
								 BDR_LOCALID_FORMAT_ARGS, cfg->name);
						apply_worker.bgw_main_arg = Int32GetDatum(i);

						if (!RegisterDynamicBackgroundWorker(&apply_worker,
															 &bgw_handle))
						{
							ereport(ERROR,
									(errmsg("bdr: Failed to register background worker"
											" %s, see previous log messages",
											cfg->name)));
						}
						/* We've launched this one, don't do it again */
						con->bgw_is_registered = true;
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
	BdrWorker		 *bdr_worker_slot;
	StringInfoData	  si;

	initStringInfo(&si);

	Assert(IsBackgroundWorker);

	bdr_worker_slot = &BdrWorkerCtl->slots[ DatumGetInt32(main_arg) ];
	Assert(bdr_worker_slot->worker_type == BDR_WORKER_PERDB);
	bdr_perdb_worker = &bdr_worker_slot->worker_data.perdb_worker;

	bdr_worker_init(NameStr(bdr_perdb_worker->dbname));

	appendStringInfo(&si, BDR_LOCALID_FORMAT": %s", BDR_LOCALID_FORMAT_ARGS, "perdb worker");
	SetConfigOption("application_name", si.data, PGC_USERSET, PGC_S_SESSION);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	/* need to be able to perform writes ourselves */
	bdr_locks_always_allow_writes(true);
	bdr_locks_startup();

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

	n_configured_bdr_nodes = 0;

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

		if (cfg == NULL || !cfg->is_valid)
			continue;

		shmworker = (BdrWorker *) bdr_worker_shmem_alloc(BDR_WORKER_APPLY, NULL);
		Assert(shmworker->worker_type == BDR_WORKER_APPLY);
		worker = &shmworker->worker_data.apply_worker;
		worker->connection_config_idx = off;
		worker->replay_stop_lsn = InvalidXLogRecPtr;
		worker->forward_changesets = false;
		/*
		 * If this is a postmaster restart, don't register the worker a second
		 * time when the per-db worker starts up.
		 */
		worker->bgw_is_registered = bdr_is_restart;
		n_configured_bdr_nodes++;
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

	/* look for the perdb worker's entries, they have the database name */
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
							 PGC_SUSET,
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
		bdr_max_workers = list_length(connames) * 3;
		elog(DEBUG1, "bdr: bdr_max_workers unset, configuring for %d workers",
				bdr_max_workers);
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
	bdr_sequencer_shmem_init(bdr_max_workers, bdr_distinct_dbnames_count);
	bdr_locks_shmem_init(bdr_distinct_dbnames_count);
	/* Set up a ProcessUtility_hook to stop unsupported commands being run */
	init_bdr_commandfilter();

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

	BdrLocksRelid = bdr_lookup_relid("bdr_global_locks", schema_oid);
	BdrLocksByOwnerRelid = bdr_lookup_relid("bdr_global_locks_byowner", schema_oid);

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

Datum
bdr_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(BDR_VERSION_STR));
}
