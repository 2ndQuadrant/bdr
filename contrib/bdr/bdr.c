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

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "pgstat.h"

#include "access/committs.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_index.h"
#include "catalog/catversion.h"
#include "commands/extension.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "replication/replication_identifier.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* sequencer */
#include "commands/extension.h"

/* apply */
#include "libpq-fe.h"

/* init_replica */
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/errno.h>
#include "postmaster/postmaster.h"

#define MAXCONNINFO		1024

static bool got_sigterm = false;
ResourceOwner bdr_saved_resowner;
static char *connections = NULL;
static char *bdr_synchronous_commit = NULL;

BDRWorkerCon *bdr_apply_con = NULL;
BDRStaticCon *bdr_static_con = NULL;

static void init_replica(BDRWorkerCon *wcon, PGconn *conn, PGresult *res);

PG_MODULE_MAGIC;

void		_PG_init(void);

static void bdr_maintain_schema(void);

/*
 * Converts an int64 to network byte order.
 */
static void
sendint64(int64 i, char *buf)
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
sendFeedback(PGconn *conn, XLogRecPtr blockpos, int64 now, bool replyRequested,
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
	sendint64(blockpos, &replybuf[len]);		/* write */
	len += 8;
	sendint64(blockpos, &replybuf[len]);		/* flush */
	len += 8;
	sendint64(blockpos, &replybuf[len]);		/* apply */
	len += 8;
	sendint64(now, &replybuf[len]);				/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0;		/* replyRequested */
	len += 1;

	elog(LOG, "sending feedback (force %d, reply requested %d) to %X/%X",
		 force, replyRequested,
		 (uint32) (blockpos >> 32), (uint32) blockpos);

	lastpos = blockpos;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		elog(ERROR, "could not send feedback packet: %s",
			 PQerrorMessage(conn));
		return false;
	}

	return true;
}

static void
bdr_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
bdr_sighup(SIGNAL_ARGS)
{
	elog(LOG, "got sighup!");
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

static void
process_remote_action(StringInfo s)
{
	char		action;

	action = pq_getmsgbyte(s);

	switch (action)
	{
			/* BEGIN */
		case 'B':
			process_remote_begin(s);
			break;
			/* COMMIT */
		case 'C':
			process_remote_commit(s);
			break;
			/* INSERT */
		case 'I':
			process_remote_insert(s);
			break;
			/* UPDATE */
		case 'U':
			process_remote_update(s);
			break;
			/* DELETE */
		case 'D':
			process_remote_delete(s);
			break;
		default:
			elog(ERROR, "unknown action of type %c", action);
	}
}

static void
bdr_apply_main(Datum main_arg)
{
	PGconn	   *streamConn;
	PGresult   *res;
	int			fd;
	char	   *remote_sysid;
	uint64		remote_sysid_i;
	char	   *remote_tlid;
	TimeLineID	remote_tlid_i;

#ifdef NOT_USED
	char	   *remote_dbname;
#endif
	char	   *remote_dboid;
	Oid			remote_dboid_i;
	char		local_sysid[32];
	char		remote_ident[256];
	StringInfoData query;
	char		conninfo_repl[MAXCONNINFO + 75];
	XLogRecPtr	last_received = InvalidXLogRecPtr;
	char	   *sqlstate;
	NameData	replication_name;
	RepNodeId	replication_identifier;
	XLogRecPtr	start_from;
	NameData	slot_name;

	initStringInfo(&query);

	bdr_apply_con = (BDRWorkerCon *) DatumGetPointer(main_arg);

	NameStr(replication_name)[0] = '\0';

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, bdr_sighup);
	pqsignal(SIGTERM, bdr_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(bdr_apply_con->dbname, NULL);

	/* make sure BDR extension exists */
	bdr_maintain_schema();

	/* always work in our own schema */
	SetConfigOption("search_path", "bdr, pg_catalog",
					PGC_BACKEND, PGC_S_OVERRIDE);

	/* setup synchronous commit according to the user's wishes */
	if (bdr_synchronous_commit != NULL)
		SetConfigOption("synchronous_commit", bdr_synchronous_commit,
						PGC_BACKEND, PGC_S_OVERRIDE);	/* other context? */

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr apply top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	snprintf(conninfo_repl, sizeof(conninfo_repl),
			 "%s replication=database fallback_application_name=bdr",
			 bdr_apply_con->dsn);

	elog(LOG, "%s initialized on %s, remote %s",
		 MyBgworkerEntry->bgw_name, bdr_apply_con->dbname, conninfo_repl);

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

	if (sscanf(remote_sysid, UINT64_FORMAT, &remote_sysid_i) != 1)
		elog(ERROR, "could not parse remote sysid %s", remote_sysid);

	if (sscanf(remote_tlid, "%u", &remote_tlid_i) != 1)
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
		elog(LOG, "local sysid %s, remote: %s",
			 local_sysid, remote_sysid);

	/*
	 * build slot name.
	 *
	 * FIXME: This might truncate the identifier if replication_name is
	 * somewhat longer...
	 */
	snprintf(NameStr(slot_name), NAMEDATALEN, "bdr_%u_%s_%u_%u__%s",
			 remote_dboid_i, local_sysid, ThisTimeLineID,
			 MyDatabaseId, NameStr(replication_name));
	NameStr(slot_name)[NAMEDATALEN - 1] = '\0';

	/*
	 * Build replication identifier.
	 */
	snprintf(remote_ident, sizeof(remote_ident), "bdr_"UINT64_FORMAT"_%u_%u_%u_%s",
			 remote_sysid_i, remote_tlid_i, remote_dboid_i, MyDatabaseId, NameStr(replication_name));

	StartTransactionCommand();

	replication_identifier = GetReplicationIdentifier(remote_ident, true);

	CommitTransactionCommand();

	/* no parts of IDENTIFY_SYSTEM's response needed anymore */
	PQclear(res);

	if (OidIsValid(replication_identifier))
		elog(LOG, "found valid replication identifier %u", replication_identifier);
	/* create local replication identifier and a remote slot */
	else
	{
		elog(LOG, "lookup failed, create new identifier");
		/* doing this really safely would require 2pc... */
		StartTransactionCommand();

		/* we want the new identifier on stable storage immediately */
		ForceSyncCommit();

		/* acquire remote decoding slot */
		resetStringInfo(&query);
		appendStringInfo(&query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
						 NameStr(slot_name), "bdr_output");
		res = PQexec(streamConn, query.data);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(FATAL, "could not send replication command \"%s\": status %s: %s\n",
				 query.data, PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}

		/* acquire new local identifier, but don't commit */
		replication_identifier = CreateReplicationIdentifier(remote_ident);

		/* now commit local identifier */
		CommitTransactionCommand();
		CurrentResourceOwner = bdr_saved_resowner;
		elog(LOG, "created replication identifier %u", replication_identifier);

		/* copy the database, if needed */
		if (bdr_apply_con->init_replica)
			init_replica(bdr_apply_con, streamConn, res);
		
		PQclear(res);
	}

	bdr_apply_con->origin_id = replication_identifier;
	bdr_apply_con->sysid = remote_sysid_i;
	bdr_apply_con->timeline = remote_tlid_i;

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

	elog(LOG, "starting up replication at %u from %X/%X",
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

	while (!got_sigterm)
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
			if (got_sigterm)
				break;

			r = PQgetCopyData(streamConn, &copybuf, 1);

			if (r == -1)
			{
				elog(LOG, "data stream ended");
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

					process_remote_action(&s);
				}
				else if (c == 'k')
				{
					XLogRecPtr	temp;

					temp = pq_getmsgint64(&s);

					sendFeedback(streamConn, temp,
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
			sendFeedback(streamConn, last_received,
						 GetCurrentTimestamp(), false, false);
	}

	proc_exit(0);
}

static void
bdr_sequencer_main(Datum main_arg)
{
	int rc;

	bdr_static_con = (BDRStaticCon *) DatumGetPointer(main_arg);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, bdr_sighup);
	pqsignal(SIGTERM, bdr_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(bdr_static_con->dbname, NULL);

	/* make sure BDR extension exists */
	bdr_maintain_schema();

	/* always work in our own schema */
	SetConfigOption("search_path", "bdr, pg_catalog",
					PGC_BACKEND, PGC_S_OVERRIDE);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	elog(LOG, "BDR starting sequencer on db \"%s\"", bdr_static_con->dbname);

	/* initialize sequencer */
	bdr_sequencer_init();

	while (!got_sigterm)
	{
		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10000L);

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

static BDRWorkerCon*
create_worker_con(char *name)
{
	char	   *errmsg = NULL;
	PQconninfoOption *options;
	PQconninfoOption *cur_option;

	/* don't free, referenced by the guc machinery! */
	char	   *optname_dsn = palloc(strlen(name) + 30);
	char	   *optname_delay = palloc(strlen(name) + 30);
	char	   *optname_replica = palloc(strlen(name) + 30);
	char	   *optname_bindir = palloc(strlen(name) + 30);
	char	   *optname_tmpdir = palloc(strlen(name) + 30);
	char	   *optname_local_dsn = palloc(strlen(name) + 30);
	char	   *optname_script_path = palloc(strlen(name) + 30);
	BDRWorkerCon *con;

	con = palloc(sizeof(BDRWorkerCon));
	con->dsn = NULL;
	con->name = pstrdup(name);
	con->apply_delay = 0;
	con->init_replica = false;
	con->replica_bin_dir = NULL;
	con->replica_tmp_dir = NULL;
	con->replica_local_dsn = NULL;
	con->replica_script_path = NULL;

	sprintf(optname_dsn, "bdr.%s_dsn", name);
	DefineCustomStringVariable(optname_dsn,
							   optname_dsn,
							   NULL,
							   &con->dsn,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	sprintf(optname_delay, "bdr.%s_apply_delay", name);
	DefineCustomIntVariable(optname_delay,
							optname_delay,
							NULL,
							&con->apply_delay,
							0, 0, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	sprintf(optname_replica, "bdr.%s_init_replica", name);
	DefineCustomBoolVariable(optname_replica,
							 optname_replica,
							 NULL,
							 &con->init_replica,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	sprintf(optname_bindir, "bdr.%s_replica_bin_dir", name);
	DefineCustomStringVariable(optname_bindir,
							   optname_bindir,
							   NULL,
							   &con->replica_bin_dir,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	sprintf(optname_tmpdir, "bdr.%s_replica_tmp_dir", name);
	DefineCustomStringVariable(optname_tmpdir,
							   optname_tmpdir,
							   NULL,
							   &con->replica_tmp_dir,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	sprintf(optname_local_dsn, "bdr.%s_replica_local_dsn", name);
	DefineCustomStringVariable(optname_local_dsn,
							   optname_local_dsn,
							   NULL,
							   &con->replica_local_dsn,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	sprintf(optname_script_path, "bdr.%s_replica_script_path", name);
	DefineCustomStringVariable(optname_script_path,
							   optname_script_path,
							   NULL,
							   &con->replica_script_path,
							   NULL, PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	if (!con->dsn)
	{
		elog(WARNING, "no connection information for %s", name);
		return NULL;
	}

	elog(LOG, "bgworkers, connection: %s", con->dsn);

	options = PQconninfoParse(con->dsn, &errmsg);
	if (errmsg != NULL)
	{
		char	   *str = pstrdup(errmsg);

		PQfreemem(errmsg);
		elog(ERROR, "msg: %s", str);
	}

	cur_option = options;
	while (cur_option->keyword != NULL)
	{
		if (strcmp(cur_option->keyword, "dbname") == 0)
		{
			if (cur_option->val == NULL)
				elog(ERROR, "no dbname set");

			con->dbname = pstrdup(cur_option->val);
		}

		if (cur_option->val != NULL)
		{
			elog(LOG, "option: %s, val: %s",
				 cur_option->keyword, cur_option->val);
		}
		cur_option++;
	}

	/* cleanup */
	PQconninfoFree(options);

	return con;
}

/*
 * Each database with BDR enabled on it has a static background
 * worker, registered at shared_preload_libraries time during
 * postmaster start. This is the entry point for these bgworkers.
 *
 * This worker handles BDR startup on the database and launches
 * apply workers for each BDR connection.
 *
 * Since the worker is fork()ed from the postmaster, all globals
 * initialised in _PG_init remain valid.
 */
static void
bdr_static_worker(Datum main_arg)
{
	BackgroundWorker  apply_worker;
	List             *apply_workers = NIL;
	ListCell		 *c;

	bdr_static_con = (BDRStaticCon *) DatumGetPointer(main_arg);

	elog(LOG, "Starting bdr worker for %s", bdr_static_con->dbname);

	/* Common apply worker values */
	apply_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	apply_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	apply_worker.bgw_main = bdr_apply_main;
	apply_worker.bgw_restart_time = 5;
	apply_worker.bgw_notify_pid = 0;

	foreach(c, bdr_static_con->conns)
	{
		BDRWorkerCon 		   *con;
		BackgroundWorkerHandle *bgw_handle;

		con = (BDRWorkerCon*) lfirst(c);

		if ( strcmp(con->dbname, bdr_static_con->dbname) != 0 )
			/* Connection for a different DB than ours, skip it */
			continue;

		snprintf(apply_worker.bgw_name, BGW_MAXLEN,
				 "bdr apply: %s", con->name);
		apply_worker.bgw_main_arg = PointerGetDatum(con);

		RegisterDynamicBackgroundWorker(&apply_worker, &bgw_handle);
		apply_workers = lcons(bgw_handle, apply_workers);
	}

	/* Once we're done with init, launch the sequencer
	 * (should integrate it) */
	bdr_sequencer_main(main_arg);
}

/*
 * Entrypoint of this module - called at shared_preload_libraries time
 * in the context of the postmaster.
 *
 * Can't use SPI, and should do as little as sensibly possible. Must
 * initialize any PGC_POSTMASTER custom GUCs, register static bgworkers,
 * as that can't be done later.
 */
void
_PG_init(void)
{
	BackgroundWorker static_worker;
	List	   *connames;
	List       *conns = NIL;
	ListCell   *c;
	MemoryContext old_context;
	Size		nregistered;
	size_t      off;

	char	  **used_databases;
	char      **database_initcons;
	Size		num_used_databases = 0;

	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "bdr can only be loaded via shared_preload_libraries");

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

	/* if nothing is configured, we're done */
	if (connections == NULL)
		goto out;

	/*
	 * otherwise, set up a ProcessUtility_hook to stop unsupported commands
	 * being run
	 */
	init_bdr_commandfilter();

	/*
	 * Get the list of BDR connection names to iterate over
	 */
	if (!SplitIdentifierString(connections, ',', &connames))
	{
		/* syntax error in list */
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax for \"bdr.connections\"")));
	}

	/* Names of all databases we're going to be doing BDR for */
	used_databases = palloc0(sizeof(char *) * list_length(connames));
	/* Name of the worker with init_replica set for each db for the
	 * corresponding index in used_databases */
	database_initcons = palloc0(sizeof(char *) * list_length(connames));

	/*
	 * Read all connections and create their BDRWorkerCon structs,
	 * validating parameters and sanity checking as we go.
	 */
	foreach(c, connames)
	{
		size_t		  off;
		BDRWorkerCon *con;

		char *name = (char *) lfirst(c);
		con = create_worker_con(name);

		if (!con)
			continue;

		/* If this is a DB name we haven't seen yet, add it to our set of known DBs */
		for (off = 0; off < num_used_databases; off++)
		{
			if (strcmp(con->dbname , used_databases[off]) == 0)
				break;
		}

		if (off == num_used_databases)
		{
			/* Didn't find a match, add new db name */
			used_databases[num_used_databases++] = pstrdup(con->dbname);
		}

		/*
		 * Make sure that at most one of the worker configs for each DB can be
		 * configured to run initialization.
		 */
		if (con->init_replica)
		{
			if (database_initcons[off] != NULL)
				elog(ERROR, "Connections %s and %s on database %s both have init_replica enabled, cannot continue",
					con->name, database_initcons[off], used_databases[off]);
			else 
				database_initcons[off] = con->name;
		}

		conns = lcons(con, conns);
	}
	/* We've ensured there are no duplicate init connections */
	pfree(database_initcons);

	/*
	 * We now need to register one static bgworker per database.
	 * When started, this worker will continue setup - doing any
	 * required initialization of the database, then registering
	 * dynamic bgworkers for the DB's individual BDR connections.
	 */
	static_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	static_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	static_worker.bgw_main = bdr_static_worker;
	static_worker.bgw_restart_time = 5;
	static_worker.bgw_notify_pid = 0;

	for (off = 0; off < num_used_databases; off++)
	{
		/* Start a worker for this db */
		BDRStaticCon *con = palloc(sizeof(BDRStaticCon));
		con->dbname = used_databases[off];
		/* Pass all the connections, even those for other dbs;
		 * let the backend filter them out. */
		con->conns = conns;
		con->slot = off;

		elog(LOG, "starting bdr worker for db %s", con->dbname);
		snprintf(static_worker.bgw_name, BGW_MAXLEN,
				 "bdr: %s", con->dbname);
		static_worker.bgw_main_arg = PointerGetDatum(con);
		RegisterBackgroundWorker(&static_worker);
	}

	EmitWarningsOnPlaceholders("bdr");
out:

	/*
	 * initialize other modules that need shared memory
	 *
	 * Do so even if we haven't any remote nodes setup, the shared memory
	 * might still be needed for some sql callable functions or such.
	 */

	/* register a slot for every remote node */
	nregistered = list_length(conns);
	bdr_count_shmem_init(nregistered);
	bdr_sequencer_shmem_init(nregistered, num_used_databases);

	MemoryContextSwitchTo(old_context);
}

static Oid
lookup_relid(const char *relname, Oid schema_oid)
{
	Oid			relid;

	relid = get_relname_relid(relname, schema_oid);

	if (!relid)
		elog(ERROR, "cache lookup failed for relation public.%s", relname);

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
	Relation extrel;
	Oid		extoid;
	Oid			schema_oid;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* make sure we're operating without other bdr workers interfering */
	extrel = heap_open(ExtensionRelationId, ShareUpdateExclusiveLock);

	extoid = get_extension_oid("bdr", true);

	/* create required extension if they don't exists yet */
	if (extoid == InvalidOid)
	{
		CreateExtensionStmt create_stmt;

		create_stmt.if_not_exists = false;
		create_stmt.options = NIL;
		create_stmt.extname = (char *)"btree_gist";

		CreateExtension(&create_stmt);

		create_stmt.extname = (char *)"bdr";
		CreateExtension(&create_stmt);
	}
	else
	{
		AlterExtensionStmt alter_stmt;

		alter_stmt.options = NIL;
		alter_stmt.extname = (char *)"btree_gist";
		ExecAlterExtensionStmt(&alter_stmt);

		alter_stmt.extname = (char *)"bdr";
		ExecAlterExtensionStmt(&alter_stmt);
	}

	heap_close(extrel, NoLock);

	/* setup initial queued_cmds OID */
	schema_oid = get_namespace_oid("bdr", false);
	if (schema_oid != InvalidOid)
	{
		QueuedDDLCommandsRelid = lookup_relid("bdr_queued_commands",
											  schema_oid);

		BdrSequenceValuesRelid = lookup_relid("bdr_sequence_values",
											  schema_oid);

		BdrSequenceElectionsRelid = lookup_relid("bdr_sequence_elections",
												 schema_oid);

		BdrVotesRelid = lookup_relid("bdr_votes", schema_oid);
	}
	else
		elog(ERROR, "cache lookup failed for schema bdr");

	elog(LOG, "bdr.bdr_queued_commands OID set to %u", QueuedDDLCommandsRelid);

	PopActiveSnapshot();
	CommitTransactionCommand();
}

/*
 * Use a script to copy the contents of a remote node using pg_dump
 * and apply it to the local node. Runs during slot creation to bring
 * up a new logical replica from an existing node.
 */
static void
init_replica(BDRWorkerCon *wcon, PGconn *conn, PGresult *res)
{
	pid_t pid;
	char *snapshot;
	char *directory;

	if (!wcon->replica_bin_dir)
		elog(FATAL, "bdr init_replica: no replica_bin_dir specified");

	if (!wcon->replica_tmp_dir)
		elog(FATAL, "bdr init_replica: no replica_tmp_dir specified");

	if (!wcon->replica_local_dsn)
		elog(FATAL, "bdr init_replica: no replica_local_dsn specified");

	if (!wcon->replica_script_path)
		elog(FATAL, "bdr init_replica: no replica_script_path specified");

	snapshot = PQgetvalue(res, 0, 2);
	directory = palloc(strlen(wcon->replica_tmp_dir)+32);
	sprintf(directory, "%s.%s.%d", wcon->replica_tmp_dir,
			snapshot, getpid());

	pid = fork();
	if (pid < 0)
		elog(FATAL, "can't fork to create initial replica");
	else if (pid == 0)
	{
		int n = 0;

		char *const envp[] = { NULL };
		char *const argv[] = {
			wcon->replica_script_path,
			"--snapshot", snapshot,
			"--source", wcon->dsn,
			"--target", wcon->replica_local_dsn,
			"--bindir", wcon->replica_bin_dir,
			"--tmp-directory", directory,
			NULL
		};

		elog(LOG, "Creating replica with: %s --snapshot %s --source \"%s\" --target \"%s\" --bindir \"%s\" --tmp-directory \"%s\"",
			 wcon->replica_script_path, snapshot, wcon->dsn, wcon->replica_local_dsn,
			 wcon->replica_bin_dir, directory);

		n = execve(wcon->replica_script_path, argv, envp);
		if (n < 0)
			exit(n);
	}
	else
	{
		pid_t res;
		int exitstatus = 0;

		elog(DEBUG1, "Waiting for pg_bdr_replica pid %d", pid);

		do
		{
			res = waitpid(pid, &exitstatus, WNOHANG);
			if (res < 0)
			{
				if (errno == EINTR || errno == EAGAIN)
					continue;
				elog(FATAL, "error calling waitpid");
			}
			else if (res == pid)
				break;

			pg_usleep(10 * 1000);
			CHECK_FOR_INTERRUPTS();
		}
		while (1);

		elog(DEBUG1, "pg_bdr_replica exited with status %d", exitstatus);

		if (exitstatus != 0)
			elog(FATAL, "pg_bdr_replica returned non-zero");
	}
}
