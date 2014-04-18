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

#include "port.h"
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

#define MAXCONNINFO		1024

static bool got_sigterm = false;
static int   n_configured_bdr_nodes = 0;
ResourceOwner bdr_saved_resowner;
static bool bdr_is_restart = false;

/* GUC storage */
static char *connections = NULL;
static char *bdr_synchronous_commit = NULL;
static int bdr_max_workers;

/* TODO: Remove when bdr_apply_main moved into bdr_apply.c */
extern BdrApplyWorker *bdr_apply_worker;

/*
 * Header for the shared memory segment ref'd by the BdrWorkerCtl ptr,
 * containing bdr_max_workers entries of BdrWorkerCon .
 */
typedef struct BdrWorkerControl
{
	/* Must hold this lock when writing to BdrWorkerControl members */
	LWLockId     lock;
	/* Required only for bgworker restart issues: */
	bool		 launch_workers;
	/* Array members, of size bdr_max_workers */
	BdrWorker    slots[FLEXIBLE_ARRAY_MEMBER];
} BdrWorkerControl;

/* shmem init hook to chain to on startup, if any */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
/* shortcut for finding the the worker shmem block */
static BdrWorkerControl *BdrWorkerCtl = NULL;

/*
 * Used only in postmaster to pass data from _PG_init during
 * shared_preload_libraries into the shared memory startup hook.
 */
typedef struct BdrStartupContext
{
	/* List of palloc'd BdrApplyWorker instances to copy into shmem */
	List    *workers;
} BdrStartupContext;

static BdrStartupContext *bdr_startup_context;

/*
 * We need somewhere to store config options for each bdr apply worker when
 * we're creating the GUCs for each worker in the postmaster during startup.
 *
 * This isn't directly accessible to workers as we don't keep a pointer to it
 * anywhere, and in EXEC_BACKEND cases it'd be useless because it wouldn't be
 * preserved across fork() anyway. Workers have to use GetConfigOption to
 * access these values.
 */
typedef struct BdrApplyWorkerConfigOptions
{
	char *dsn;
	int   apply_delay;
} BdrApplyWorkerConfigOptions;

PG_MODULE_MAGIC;

void		_PG_init(void);
static void bdr_maintain_schema(void);
static void bdr_worker_shmem_startup(void);
static void bdr_worker_shmem_create_workers(void);
static BdrWorker* bdr_worker_shmem_alloc(BdrWorkerType worker_type);

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
bdr_process_remote_action(StringInfo s)
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
static PGconn*
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
		elog(LOG, "local sysid %s, remote: %s",
			 local_sysid, remote_sysid);

	/*
	 * build slot name.
	 *
	 * FIXME: This might truncate the identifier if replication_name is
	 * somewhat longer...
	 */
	snprintf(NameStr(*slot_name), NAMEDATALEN, "bdr_%u_%s_%u_%u__%s",
			 remote_dboid_i, local_sysid, ThisTimeLineID,
			 MyDatabaseId, NameStr(replication_name));
	NameStr(*slot_name)[NAMEDATALEN - 1] = '\0';

	/*
	 * Build replication identifier.
	 */
	snprintf(remote_ident, remote_ident_length,
			 "bdr_"UINT64_FORMAT"_%u_%u_%u_%s",
			 *remote_sysid_i, *remote_tlid_i, remote_dboid_i, MyDatabaseId,
			 NameStr(replication_name));

	/* no parts of IDENTIFY_SYSTEM's response needed anymore */
	PQclear(res);

	return streamConn;
}

/*
 * Create a slot on a remote node, and the corresponding local replication
 * identifier.
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
		elog(FATAL, "could not send replication command \"%s\": status %s: %s\n",
			 query.data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	/* acquire new local identifier, but don't commit */
	*replication_identifier = CreateReplicationIdentifier(remote_ident);

	/* now commit local identifier */
	CommitTransactionCommand();
	CurrentResourceOwner = bdr_saved_resowner;
	elog(LOG, "created replication identifier %u", *replication_identifier);

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
 * GetConfigOption wrapper that gets an option name qualified by the worker's
 * name.
 *
 * The returned string is *not* modifiable and will only be valid until the
 * next GUC-related call.
 */
const char *
bdr_get_worker_option(const char * worker_name, const char * option_name,
					  bool missing_ok)
{
	char	   *gucname;
	size_t      namelen;
	const char *optval;

	/* Option names should omit leading underscore */
	Assert(option_name[0] != '_');

	namelen = sizeof("bdr.") + strlen(worker_name) + strlen(option_name) + 3;
	gucname = palloc(namelen);
	snprintf(gucname, namelen, "bdr.%s_%s", worker_name, option_name);

	optval = GetConfigOption(gucname, missing_ok, false);

	pfree(gucname);

	return optval;
}

/*
 * Entry point and main loop for a BDR apply worker.
 *
 * Responsible for establishing a replication connection, creating slots,
 * replaying.
 *
 * TODO: move to bdr_apply.c
 */
static void
bdr_apply_main(Datum main_arg)
{
	PGconn	   *streamConn;
	PGresult   *res;
	int			fd;
	char		remote_ident[256];
	StringInfoData query;
	char		conninfo_repl[MAXCONNINFO + 75];
	XLogRecPtr	last_received = InvalidXLogRecPtr;
	char	   *sqlstate;
	RepNodeId	replication_identifier;
	XLogRecPtr	start_from;
	NameData	slot_name;
	const char *dsn;
	BdrWorker  *bdr_worker_slot;

	Assert(IsBackgroundWorker);

	initStringInfo(&query);

	bdr_worker_slot = &BdrWorkerCtl->slots[ DatumGetInt32(main_arg) ];
	Assert(bdr_worker_slot->worker_type == BDR_WORKER_APPLY);
	bdr_apply_worker = &bdr_worker_slot->worker_data.apply_worker;

	bdr_worker_init(NameStr(bdr_apply_worker->dbname));

	CurrentResourceOwner =
		ResourceOwnerCreate(NULL, "bdr apply top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	dsn = bdr_get_worker_option(NameStr(bdr_apply_worker->name),
								"dsn", false);
	snprintf(conninfo_repl, sizeof(conninfo_repl),
			 "%s replication=database fallback_application_name=bdr",
			 dsn);

	elog(LOG, "%s initialized on %s, remote %s",
		 MyBgworkerEntry->bgw_name, NameStr(bdr_apply_worker->dbname),
		 conninfo_repl);

	/* Establish BDR conn and IDENTIFY_SYSTEM */
	streamConn = bdr_connect(
		conninfo_repl,
		remote_ident, sizeof(remote_ident),
		&slot_name,
		&bdr_apply_worker->sysid,
		&bdr_apply_worker->timeline
		);

	StartTransactionCommand();
	replication_identifier = GetReplicationIdentifier(remote_ident, true);
	CommitTransactionCommand();

	if (OidIsValid(replication_identifier))
		elog(LOG, "found valid replication identifier %u",
			 replication_identifier);
	else
	{
		char *snapshot;

		elog(LOG, "Creating new slot %s", NameStr(slot_name));

		/* create local replication identifier and a remote slot */
		bdr_create_slot(streamConn, &slot_name, remote_ident,
						&replication_identifier, &snapshot);

		/* TODO: Initialize database from remote */
	}

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
 *	out_worker
 *	Initialize this BdrApplyWorker with the name and dbname found.
 *
 * TODO At some point we'll have to make the GUC setup dynamic so we can
 * handle workers being added/removed with a config reload SIGHUP.
 */
static bool
bdr_create_con_gucs(char  *name,
					char **used_databases,
					Size  *num_used_databases,
					BdrApplyWorker *out_worker)
{
	int			off;
	char	   *errmsg = NULL;
	PQconninfoOption *options;
	PQconninfoOption *cur_option;
	BdrApplyWorkerConfigOptions *opts;

	/* don't free, referenced by the guc machinery! */
	char	   *optname_dsn = palloc(strlen(name) + 30);
	char	   *optname_delay = palloc(strlen(name) + 30);
	opts = palloc(sizeof(BdrApplyWorkerConfigOptions));

	Assert(process_shared_preload_libraries_in_progress);

	strncpy(NameStr(out_worker->name), name, NAMEDATALEN);
	NameStr(out_worker->name)[NAMEDATALEN-1] = '\0';

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
							0, 0, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	if (!opts->dsn)
	{
		elog(WARNING, "no connection information for %s", name);
		return false;
	}

	elog(LOG, "bgworkers, connection: %s", opts->dsn);

	options = PQconninfoParse(opts->dsn, &errmsg);
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

			strncpy(NameStr(out_worker->dbname), cur_option->val,
					NAMEDATALEN);
			NameStr(out_worker->dbname)[NAMEDATALEN-1] = '\0';
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

	/*
	 * If this is a DB name we haven't seen yet, add it to our set of known
	 * DBs.
	 */
	for (off = 0; off < *num_used_databases; off++)
	{
		if (strcmp(NameStr(out_worker->dbname), used_databases[off]) == 0)
			break;
	}

	if (off == *num_used_databases)
	{
		/* Didn't find a match, add new db name */
		used_databases[(*num_used_databases)++] =
			pstrdup(NameStr(out_worker->dbname));
	}

	/* optname vars and opts intentionally leaked, see above */
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
					if ( strcmp(NameStr(con->dbname), dbname) == 0 )
					{
						/* It's an apply worker for our DB; launch it */
						BackgroundWorkerHandle *bgw_handle;

						snprintf(apply_worker.bgw_name, BGW_MAXLEN,
								 "bdr apply: %s", NameStr(con->name));
						apply_worker.bgw_main_arg = Int32GetDatum(i);

						if (!RegisterDynamicBackgroundWorker(&apply_worker,
															 &bgw_handle))
						{
							/* FIXME better error */
							elog(ERROR, "Failed to register background worker");
						}
						elog(LOG, "Registered worker");
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
 * _PG_init remain valid (TODO: change this for EXEC_BACKEND support).
 *
 * This worker can use the SPI and shared memory.
 */
static void
bdr_perdb_worker_main(Datum main_arg)
{
	int				rc;
	List		   *apply_workers;
	ListCell	   *c;
	BdrPerdbWorker *bdr_perdb_worker;
	char		   *dbname;

	Assert(IsBackgroundWorker);

	/* FIXME: won't work with EXEC_BACKEND, change to index into shm array */
	bdr_perdb_worker = (BdrPerdbWorker *) DatumGetPointer(main_arg);
	dbname = NameStr(bdr_perdb_worker->dbname);

	bdr_worker_init(dbname);

	CurrentResourceOwner =
		ResourceOwnerCreate(NULL, "bdr seq top-level resource owner");
	bdr_saved_resowner = CurrentResourceOwner;

	/* TODO: Hande need to initialize database from remote at this point */

	elog(LOG, "Starting bdr apply workers for db %s",
		 NameStr(bdr_perdb_worker->dbname));

	/* Launch the apply workers */
	apply_workers = bdr_launch_apply_workers(dbname);

	/*
	 * For now, just free the bgworker handles. Later we'll probably want them
	 * for adding/removing/reconfiguring bgworkers.
	 */
	foreach(c, apply_workers)
	{
		BackgroundWorkerHandle *h = (BackgroundWorkerHandle*)lfirst(c);
		pfree(h);
	}

	elog(LOG, "BDR starting sequencer on db \"%s\"",
		 NameStr(bdr_perdb_worker->dbname));

	/* initialize sequencer */
	bdr_sequencer_init(bdr_perdb_worker->seq_slot);

	while (!got_sigterm)
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
/*
 * TODO: Do the required GUC parsing, etc in bdr_worker_shmem_create_workers
 * instead of in _PG_init.
 */
static void
bdr_worker_shmem_create_workers(void)
{
	ListCell *c;

	/*
	 * Copy the BdrApplyWorker configs created in _PG_init into shared memory,
	 * then free the palloc'd original.
	 *
	 * This is necessary on EXEC_BACKEND (Windows) where postmaster memory
	 * isn't accessible by other backends, and is also required when launching
	 * one bgworker from another.
	 */
	foreach(c, bdr_startup_context->workers)
	{
		BdrApplyWorker *worker = (BdrApplyWorker *) lfirst(c);
		BdrWorker	   *shmworker;

		shmworker = (BdrWorker *) bdr_worker_shmem_alloc(BDR_WORKER_APPLY);
		Assert(shmworker->worker_type == BDR_WORKER_APPLY);
		memcpy(&shmworker->worker_data, worker, sizeof(BdrApplyWorker));
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
static BdrWorker*
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
 */
static void
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

	/*
	 * Limit on worker count - number of slots to allocate in fixed shared
	 * memory array.
	 */
	DefineCustomIntVariable("bdr.max_workers",
							"max number of bdr connections + distinct databases",
							NULL,
							&bdr_max_workers,
							0, 0, 100,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);
	/*
	 * TODO: If bdr_max_workers is unset/zero, autoset from number of
	 * bdr.connections
	 */

	/* if nothing is configured, we're done */
	if (connections == NULL)
		goto out;

	/* Set up a ProcessUtility_hook to stop unsupported commands being run */
	init_bdr_commandfilter();

	/*
	 * Allocate a shared memory segment to store the bgworker connection
	 * information we must pass to each worker we launch.
	 *
	 * This registers a hook on shm initialization, bdr_worker_shmem_startup(),
	 * which populates the shm segment with configured apply workers using data
	 * in bdr_startup_context.
	 */
	bdr_worker_alloc_shmem_segment();

	/* Prepare storage to pass data into our shared memory startup hook */
	bdr_startup_context = (BdrStartupContext *) palloc(sizeof(BdrStartupContext));

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

	/* Names of all databases we're going to be doing BDR for */
	used_databases = palloc0(sizeof(char *) * list_length(connames));

	/*
	 * Read all connections and create their BdrApplyWorker structs, validating
	 * parameters and sanity checking as we go. The structs are palloc'd, but
	 * will be copied into shared memory and free'd during shm init.
	 */
	foreach(c, connames)
	{
		BdrApplyWorker *apply_worker;
		char *name;

		apply_worker = (BdrApplyWorker *) palloc(sizeof(BdrApplyWorker));
		name = (char *) lfirst(c);

		if (!bdr_create_con_gucs(name, used_databases, &num_used_databases,
								 apply_worker))
			continue;

		apply_worker->origin_id = InvalidRepNodeId;
		bdr_startup_context->workers = lcons(apply_worker,
											 bdr_startup_context->workers);
	}

	/*
	 * Free the connames list cells. The strings are just pointers into
	 * 'connections' and must not be freed'd.
	 */
	list_free(connames);
	connames = NIL;

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

		elog(LOG, "starting bdr database worker for db %s", NameStr(con->dbname));
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
		QueuedDDLCommandsRelid = bdr_lookup_relid("bdr_queued_commands",
											  schema_oid);

		BdrSequenceValuesRelid = bdr_lookup_relid("bdr_sequence_values",
											  schema_oid);

		BdrSequenceElectionsRelid = bdr_lookup_relid("bdr_sequence_elections",
												 schema_oid);

		BdrVotesRelid = bdr_lookup_relid("bdr_votes", schema_oid);

		QueuedDropsRelid = bdr_lookup_relid("bdr_queued_drops", schema_oid);
	}
	else
		elog(ERROR, "cache lookup failed for schema bdr");

	elog(LOG, "bdr.bdr_queued_commands OID set to %u", QueuedDDLCommandsRelid);
	elog(LOG, "bdr.bdr_queued_drops OID set to %u", QueuedDropsRelid);

	PopActiveSnapshot();
	CommitTransactionCommand();
}
