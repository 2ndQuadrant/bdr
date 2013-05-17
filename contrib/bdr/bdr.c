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
#include "access/xact.h"
#include "catalog/pg_index.h"
#include "lib/stringinfo.h"
#include "replication/replication_identifier.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* sequencer */
#include "commands/extension.h"

/* apply */
#include "libpq-fe.h"

#define MAXCONNINFO		1024

static bool got_sigterm = false;
ResourceOwner bdr_saved_resowner;
static char *connections = NULL;
static char *bdr_synchronous_commit = NULL;

BDRWorkerCon *bdr_apply_con = NULL;
BDRSequencerCon *bdr_sequencer_con = NULL;

PG_MODULE_MAGIC;

void		_PG_init(void);

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
 * Converts an int64 from network byte order to native format.
 *
 * FIXME: replace with pq_getmsgint64
 */
static int64
recvint64(char *buf)
{
	int64		result;
	uint32		h32;
	uint32		l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
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
	sendint64(now, &replybuf[len]);		/* sendTime */
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
process_remote_action(char *data, size_t r)
{
	char		action;

	action = data[0];
	data += 1;
	r--;

	switch (action)
	{
			/* BEGIN */
		case 'B':
			process_remote_begin(data, r);
			break;
			/* COMMIT */
		case 'C':
			process_remote_commit(data, r);
			break;
			/* INSERT */
		case 'I':
			process_remote_insert(data, r);
			break;
			/* UPDATE */
		case 'U':
			process_remote_update(data, r);
			break;
			/* DELETE */
		case 'D':
			process_remote_delete(data, r);
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
	char		query[256];
	char		conninfo_repl[MAXCONNINFO + 75];
	XLogRecPtr	last_received = InvalidXLogRecPtr;
	char	   *sqlstate;
	NameData	replication_name;
	RepNodeId	replication_identifier;
	XLogRecPtr	start_from;
	NameData	slot_name;

	bdr_apply_con = (BDRWorkerCon *) DatumGetPointer(main_arg);

	NameStr(replication_name)[0] = '\0';

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, bdr_sighup);
	pqsignal(SIGTERM, bdr_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(bdr_apply_con->dbname, NULL);

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
		snprintf(query, sizeof(query), "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
				 NameStr(slot_name), "bdr_output");
		res = PQexec(streamConn, query);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(FATAL, "could not send replication command \"%s\": status %s: %s\n",
				 query, PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}
		PQclear(res);

		/* acquire new local identifier, but don't commit */
		replication_identifier = CreateReplicationIdentifier(remote_ident);

		/* now commit local identifier */
		CommitTransactionCommand();
		CurrentResourceOwner = bdr_saved_resowner;
		elog(LOG, "created replication identifier %u", replication_identifier);

		/*
		 * FIXME: set current replication progress from upstream IFF it has not
		 * been set yet. Another worker might have cloned from upstream and set
		 * the progress for all nodes.
		 */
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

	snprintf(query, sizeof(query), "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
	   NameStr(slot_name), (uint32) (start_from >> 32), (uint32) start_from);
	res = PQexec(streamConn, query);

	sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		elog(FATAL, "could not send replication command \"%s\": %s\n, sqlstate: %s",
			 query, PQresultErrorMessage(res), sqlstate);
	}
	PQclear(res);

	fd = PQsocket(streamConn);

	replication_origin_id = replication_identifier;

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
				if (copybuf[0] == 'w')
				{
					int			hdr_len = 0;
					char	   *data;
					XLogRecPtr	start_lsn;
					XLogRecPtr	end_lsn;

					hdr_len = 1;	/* msgtype 'w' */

					start_lsn = recvint64(&copybuf[hdr_len]);

					hdr_len += 8;		/* dataStart */

					end_lsn = recvint64(&copybuf[hdr_len]);

					hdr_len += 8;		/* walEnd */
					hdr_len += 8;		/* sendTime */

					if (last_received < start_lsn)
						last_received = start_lsn;

					if (last_received < end_lsn)
						last_received = end_lsn;

					data = copybuf + hdr_len;

					process_remote_action(data, r);
				}
				else if (copybuf[0] == 'k')
				{
					XLogRecPtr	temp;

					temp = recvint64(&copybuf[1]);

					sendFeedback(streamConn, temp,
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
			sendFeedback(streamConn, last_received,
						 GetCurrentTimestamp(), false, false);
	}

	proc_exit(0);
}

static void
bdr_sequencer_main(Datum main_arg)
{
	int rc;

	bdr_sequencer_con = (BDRSequencerCon *) DatumGetPointer(main_arg);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, bdr_sighup);
	pqsignal(SIGTERM, bdr_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(bdr_sequencer_con->dbname, NULL);

	/* always work in our own schema */
	SetConfigOption("search_path", "bdr, pg_catalog",
					PGC_BACKEND, PGC_S_OVERRIDE);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	elog(WARNING, "starting sequencer on db \"%s\"", bdr_sequencer_con->dbname);

	/* make sure BDR extension exists */
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

/*
 * Entrypoint of this module.
 */
void
_PG_init(void)
{
	BackgroundWorker apply_worker;
	BackgroundWorker sequencer_worker;
	List	   *cons;
	ListCell   *c;
	MemoryContext old_context;
	Size		nregistered = 0;

	char	  **used_databases;
	Size		num_used_databases = 0;

	size_t		off;
	bool		found;

	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "bdr can only be loaded via shared_preload_libraries");

	if (!commit_ts_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bdr requires \"track_commit_timestamp\" to be enabled")));

	/* guc's et al need to survive this */
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

	if (!SplitIdentifierString(connections, ',', &cons))
	{
		/* syntax error in list */
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax for \"bdr.connections\"")));
	}

	used_databases = malloc(sizeof(char *) * list_length(cons));

	/* Common apply worker values */
	apply_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	apply_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	apply_worker.bgw_main = bdr_apply_main;
	apply_worker.bgw_restart_time = 5;

	/* Common sequence worker values */
	sequencer_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	sequencer_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	sequencer_worker.bgw_main = bdr_sequencer_main;
	sequencer_worker.bgw_restart_time = 5;

	foreach(c, cons)
	{
		const char *name = (char *) lfirst(c);
		char	   *errmsg = NULL;
		PQconninfoOption *options;
		PQconninfoOption *cur_option;

		/* don't free, referenced by the guc machinery! */
		char	   *optname_dsn = palloc(strlen(name) + 30);
		char	   *optname_delay = palloc(strlen(name) + 30);
		BDRWorkerCon *con;

		found = false;

		con = palloc(sizeof(BDRWorkerCon));
		con->dsn = (char *) lfirst(c);
		con->name = pstrdup(name);
		con->apply_delay = 0;

		sprintf(optname_dsn, "bdr.%s.dsn", name);
		DefineCustomStringVariable(optname_dsn,
								   optname_dsn,
								   NULL,
								   &con->dsn,
								   NULL, PGC_POSTMASTER,
								   GUC_NOT_IN_SAMPLE,
								   NULL, NULL, NULL);

		sprintf(optname_delay, "bdr.%s.apply_delay", name);
		DefineCustomIntVariable(optname_delay,
								optname_delay,
								NULL,
								&con->apply_delay,
								0, 0, INT_MAX,
								PGC_SIGHUP,
								GUC_UNIT_MS,
								NULL, NULL, NULL);

		if (!con->dsn)
		{
			elog(WARNING, "no connection information for %s", name);
			continue;
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


		snprintf(apply_worker.bgw_name, BGW_MAXLEN,
				 "bdr apply: %s", name);
		apply_worker.bgw_main_arg = PointerGetDatum(con);

		RegisterBackgroundWorker(&apply_worker);
		nregistered++;

		/* keep track of the databases used */
		/* check whether we already have a connection in this db */
		for (off = 0; off < num_used_databases; off++)
		{
			if (strcmp(con->dbname , used_databases[off]) == 0)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			used_databases[num_used_databases++] = pstrdup(con->dbname);
		}

		/* cleanup */
		PQconninfoFree(options);
	}

	Assert(num_used_databases <= nregistered);

	/*
	 * start sequence coordination process if necessary. One process per
	 * database, *not* one per configured connection.
	 */
	for (off = 0; off < num_used_databases; off++)
	{
		const char *name = used_databases[off];
		BDRSequencerCon *con;

		con = palloc(sizeof(BDRSequencerCon));
		con->dbname = pstrdup(name);
		con->num_nodes = nregistered;
		con->slot = off;

		elog(LOG, "starting seq on %s", name);

		snprintf(sequencer_worker.bgw_name, BGW_MAXLEN,
				 "bdr sequencer: %s", name);
		sequencer_worker.bgw_main_arg = PointerGetDatum(con);

		RegisterBackgroundWorker(&sequencer_worker);

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
	bdr_count_shmem_init(nregistered);
	bdr_sequencer_shmem_init(nregistered, num_used_databases);

	MemoryContextSwitchTo(old_context);
}
