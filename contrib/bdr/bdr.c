/* -------------------------------------------------------------------------
 *
 * bdi.c
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

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "access/sysattr.h"
#include "catalog/namespace.h"
#include "catalog/pg_index.h"
#include "executor/spi.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "parser/parse_relation.h"
#include "replication/logical.h"
#include "replication/replication_identifier.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/relcache.h"
#include "utils/rel.h"

#include "libpq-fe.h"

/* debugging only */
#include "catalog/pg_type.h"
#include "access/htup_details.h"


#define MAXCONNINFO		1024
static bool	got_sigterm = false;
static ResourceOwner saved_resowner;

static char* connections = NULL;
static char* bdr_synchronous_commit = NULL;

typedef struct BDRCon
{
	char *dbname;
	char *con;
} BDRCon;

PG_MODULE_MAGIC;

void	_PG_init(void);

static void
UserTableUpdateIndexes(Relation heapRel, HeapTuple heapTuple);
static HeapTuple
ExtractKeyTuple(Relation relation, HeapTuple tp);
static void build_scan_key(ScanKey skey, Relation rel, Relation idxrel, HeapTuple key);
static bool find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel, ItemPointer tid);

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
 * Local version of GetCurrentTimestamp(), since we are not linked with
 * backend code. The protocol always uses integer timestamps, regardless of
 * server setting.
 */
static int64
localGetCurrentTimestamp(void)
{
	int64 result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (int64) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}


/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
	int	natt;
	Oid oid;

	/* print oid of tuple, it's not included in the TupleDesc */
	if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid)
	{
		appendStringInfo(s, " oid[oid]:%u", oid);
	}

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid; /* type of current attribute */
		HeapTuple	type_tuple; /* information about a type */
		Form_pg_type type_form;
		Oid			typoutput; /* output function */
		bool		typisvarlena;
		Datum		origval; /* possibly toasted Datum */
		Datum		val; /* definitely detoasted Datum */
		char        *outputstr = NULL;
		bool        isnull; /* column is null? */

		attr = tupdesc->attrs[natt];
		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* gather type name */
		type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);
		type_form = (Form_pg_type) GETSTRUCT(type_tuple);

		/* print attribute name */
		appendStringInfoChar(s, ' ');
		appendStringInfoString(s, NameStr(attr->attname));

		/* print attribute type */
		appendStringInfoChar(s, '[');
		appendStringInfoString(s, NameStr(type_form->typname));
		appendStringInfoChar(s, ']');

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		ReleaseSysCache(type_tuple);

		/* get Datum from tuple */
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull)
			outputstr = "(null)";
		else if (typisvarlena && VARATT_IS_EXTERNAL_TOAST(origval))
			outputstr = "(unchanged-toast-datum)";
		else if (typisvarlena)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;

		/* print data */
		if (outputstr == NULL)
			outputstr = OidOutputFunctionCall(typoutput, val);

		appendStringInfoChar(s, ':');
		appendStringInfoString(s, outputstr);
	}
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, XLogRecPtr blockpos, int64 now, bool replyRequested)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;
	static XLogRecPtr lastpos = InvalidXLogRecPtr;

	if (blockpos == lastpos)
		return true;

	replybuf[len] = 'r';
	len += 1;
	sendint64(blockpos, &replybuf[len]);			/* write */
	len += 8;
	sendint64(blockpos, &replybuf[len]);	/* flush */
	len += 8;
	sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	sendint64(now, &replybuf[len]);					/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0;			/* replyRequested */
	len += 1;

	elog(LOG, "sending feedback to %X/%X",
		 (uint32)(blockpos >> 32), (uint32)blockpos);

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

static char *
read_tuple(char *data, size_t len, HeapTuple tuple, Oid *reloid);

static void
process_change(char *data, size_t r)
{
	char action;
	StringInfoData s;

	action = data[0];
	data += 1;
	if (action == 'B')
	{
		elog(LOG, "BEGIN");
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
	}
	else if (action == 'C')
	{
		XLogRecPtr *ptr = (XLogRecPtr *)data;
		data += sizeof(XLogRecPtr);
		elog(LOG, "COMMIT %X/%X",
			 (uint32)(*ptr>>32), (uint32)*ptr);
		replication_origin_lsn = *ptr;

		PopActiveSnapshot();
		CommitTransactionCommand();

		AdvanceCachedReplicationIdentifier(*ptr, XactLastCommitEnd);

		CurrentResourceOwner = saved_resowner;
	}
	else if (action == 'I')
	{
		HeapTupleData tup;
		Oid reloid;
		Relation rel;
		char action;

		action = data[0];
		data++;

		if (action != 'N')
			elog(ERROR, "expected new tuple but got %c",
				 action);

		data = read_tuple(data, r, &tup, &reloid);

		rel = heap_open(reloid, RowExclusiveLock);
		if (rel->rd_rel->relkind == 'r')
		{
			simple_heap_insert(rel, &tup);
			UserTableUpdateIndexes(rel, &tup);
		}
		else
		{
			elog(LOG, "skipping replicating to relkind '%c' rel \"%s\"",
				 rel->rd_rel->relkind, RelationGetRelationName(rel));
		}

		/* debug output */
		initStringInfo(&s);
		tuple_to_stringinfo(&s, RelationGetDescr(rel), &tup);
		elog(LOG, "INSERT: %s", s.data);
		resetStringInfo(&s);

		heap_close(rel, NoLock);
	}
	else if (action == 'U')
	{
		HeapTupleData old_key;
		HeapTupleData new_tuple;
		Oid reloid;
		Oid idxoid = InvalidOid;
		HeapTuple old_key_generated = NULL;
		ItemPointerData oldtid;
		Relation rel;
		Relation idxrel;
		bool found_old;

		char action;

		ScanKeyData skey[INDEX_MAX_KEYS];

		action = data[0];
		data++;

		/* old key */
		if (action == 'K')
		{
			data = read_tuple(data, r, &old_key, &idxoid);
			action = data[0];
			data++;
		}
		else if (action != 'N')
			elog(ERROR, "expected action N or K got %c",
				 action);

		/* new tuple */
		if (action != 'N')
			elog(ERROR, "expected action N got %c",
				 action);

		data = read_tuple(data, r, &new_tuple, &reloid);

		rel = heap_open(reloid, RowExclusiveLock);

		/* if there's no separate primary key, extrakt pkey from tuple */
		if (idxoid == InvalidOid)
		{
			old_key_generated = ExtractKeyTuple(rel, &new_tuple);
			old_key.t_data = old_key_generated->t_data;
			old_key.t_len = old_key_generated->t_len;
			if (rel->rd_indexvalid == 0)
				RelationGetIndexList(rel);
			idxoid = rel->rd_primary;
		}
		else
		{
#ifdef USE_ASSERT_CHECKING
			RelationGetIndexList(rel);
			Assert(idxoid == rel->rd_primary);
#endif
		}

		idxrel = index_open(idxoid, RowExclusiveLock);
		build_scan_key(skey, rel, idxrel, &old_key);
		found_old = find_pkey_tuple(skey, rel, idxrel, &oldtid);

		if (found_old)
		{
			elog(LOG, "doing update");
			if (rel->rd_rel->relkind == 'r')
			{
				simple_heap_update(rel, &oldtid, &new_tuple);
			/* FIXME: HOT support */
				UserTableUpdateIndexes(rel, &new_tuple);
			}
			else
			{
				elog(LOG, "skipping replicating to relkind '%c' rel \"%s\"",
					 rel->rd_rel->relkind, RelationGetRelationName(rel));
			}
		}
		else
		{
			elog(WARNING, "UPDATE: could not find old tuple");
		}


		initStringInfo(&s);
		tuple_to_stringinfo(&s, RelationGetDescr(idxrel), &old_key);
		elog(LOG, "UPDATE old-key: %s", s.data);
		resetStringInfo(&s);

		tuple_to_stringinfo(&s, RelationGetDescr(rel), &new_tuple);
		elog(LOG, "UPDATE new-tup: %s", s.data);
		resetStringInfo(&s);

		index_close(idxrel, NoLock);

		if (old_key_generated)
			heap_freetuple(old_key_generated);
		heap_close(rel, NoLock);
	}
	else if (action == 'D')
	{
		Oid idxoid;
		HeapTupleData old_key;
		Relation rel;
		Relation idxrel;
		ScanKeyData skey[INDEX_MAX_KEYS];
		bool found_old;
		ItemPointerData oldtid;

		action = data[0];
		data++;

		if (action == 'E')
		{
			elog(WARNING, "got delete without pkey");
			return;
		}
		else if (action != 'K')
			elog(ERROR, "expected action K got %c", action);

		data = read_tuple(data, r, &old_key, &idxoid);

		/* FIXME: may not open relations in that order */
		idxrel = index_open(idxoid, RowExclusiveLock);

		rel = heap_open(idxrel->rd_index->indrelid, RowExclusiveLock);
		build_scan_key(skey, rel, idxrel, &old_key);
		found_old = find_pkey_tuple(skey, rel, idxrel, &oldtid);

		if (found_old)
		{
			elog(LOG, "doing delete");
			if (rel->rd_rel->relkind == 'r')
				simple_heap_delete(rel, &oldtid);
			else
				elog(LOG, "skipping replicating to relkind '%c' rel \"%s\"",
					 rel->rd_rel->relkind, RelationGetRelationName(rel));
		}
		else
		{
			elog(WARNING, "DELETE: could not find old tuple");
		}

		initStringInfo(&s);
		tuple_to_stringinfo(&s, RelationGetDescr(idxrel), &old_key);
		elog(LOG, "DELETE old-key: %s", s.data);
		resetStringInfo(&s);

		index_close(idxrel, NoLock);
		heap_close(rel, NoLock);
	}
	else
	{
		elog(ERROR, "unknown action of type %c", action);
	}

}

static void
bdr_main(void *main_arg)
{
	PGconn *streamConn;
	PGresult   *res;
	BDRCon	   *con = (BDRCon *) main_arg;
	int fd;
	char	   *remote_sysid;
	uint64		remote_sysid_i;
	char	   *remote_tlid;
	TimeLineID	remote_tlid_i;
	char	   *remote_dbname;
	char	   *remote_dboid;
	Oid			remote_dboid_i;
	char		local_sysid[32];
	char		query[256];
	char		conninfo_repl[MAXCONNINFO + 75];
	XLogRecPtr	last_received = InvalidXLogRecPtr;
	char		*sqlstate;
	NameData	replication_name;
	Oid			replication_identifier;
	XLogRecPtr  start_from;
	NameData	slot_name;

	NameStr(replication_name)[0] = '\0';

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* setup synchronous commit according to the user's wishes*/
	if (bdr_synchronous_commit != NULL)
		SetConfigOption("synchronous_commit", bdr_synchronous_commit,
						PGC_BACKEND, PGC_S_OVERRIDE); /* other context? */

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(con->dbname, NULL);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr top-level resource owner");
	saved_resowner = CurrentResourceOwner;

	snprintf(conninfo_repl, sizeof(conninfo_repl),
			 "replication=true fallback_application_name=bdr %s",
			 con->con);

	elog(LOG, "%s initialized on %s, remote %s",
		 MyBgworkerEntry->bgw_name, con->dbname, conninfo_repl);

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
	remote_dbname = PQgetvalue(res, 0, 3);
	remote_dboid = PQgetvalue(res, 0, 4);

	if (sscanf(remote_sysid, UINT64_FORMAT, &remote_sysid_i) != 1)
		elog(ERROR, "could not parse remote sysid %s", remote_sysid);

	if (sscanf(remote_tlid, "%u", &remote_tlid_i) != 1)
		elog(ERROR, "could not parse remote tlid %s", remote_tlid);

	if (sscanf(remote_dboid, "%u", &remote_dboid_i) != 1)
		elog(ERROR, "could not parse remote tlid %s", remote_tlid);

	snprintf(local_sysid, sizeof(local_sysid), UINT64_FORMAT,
			 GetSystemIdentifier());

	if (strcmp(remote_sysid, local_sysid) == 0)
	{
		ereport(FATAL,
				(errmsg("database system identifier have to differ between the nodes"),
				 errdetail("The remotes's identifier is %s, the local identifier is %s.",
						   remote_sysid, local_sysid)));
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
	snprintf(NameStr(slot_name), NAMEDATALEN, "bdr: %u:%s-%u-%u:%s",
	         remote_dboid_i, local_sysid, ThisTimeLineID,
	         MyDatabaseId, NameStr(replication_name));
	NameStr(slot_name)[NAMEDATALEN-1] = '\0';

	replication_identifier =
		GetReplicationIdentifier(remote_sysid_i, remote_tlid_i, remote_dboid_i,
								 &replication_name, MyDatabaseId);

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

		/* we wan't the new identifier on stable storage immediately */
		ForceSyncCommit();

		/* acquire new local identifier, but don't commit */
		replication_identifier =
			CreateReplicationIdentifier(remote_sysid_i,remote_tlid_i, remote_dboid_i,
										&replication_name, MyDatabaseId);

		/* acquire remote decoding slot */
		snprintf(query, sizeof(query), "INIT_LOGICAL_REPLICATION \"%s\" %s",
		         NameStr(slot_name), "bdr_output");
		res = PQexec(streamConn, query);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(FATAL, "could not send replication command \"%s\": %s: %d\n",
			     query, PQresultErrorMessage(res), PQresultStatus(res));
		}
		PQclear(res);

		/* now commit local identifier */
		CommitTransactionCommand();
		CurrentResourceOwner = saved_resowner;
		elog(LOG, "created replication identifier %u", replication_identifier);
	}

	/*
	 * tell replication_identifier.c about our identifier so it can cache the
	 * search in shared memory.
	 */
	SetupCachedReplicationIdentifier(replication_identifier);

	/*
	 * Chck whether we already replayed something so we don't replay it
	 * multiple times.
	 */
	start_from = RemoteCommitFromCachedReplicationIdentifier();

	elog(LOG, "starting up replication at %u from %X/%X",
		 replication_identifier,
		 (uint32) (start_from >> 32), (uint32) start_from);

	snprintf(query, sizeof(query), "START_LOGICAL_REPLICATION \"%s\" %X/%X",
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

	guc_replication_origin_id = replication_identifier;

	while (!got_sigterm)
	{
		/*int		ret;*/
		int		rc;
		int		r;
		char	*copybuf = NULL;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatchOrSocket(&MyProc->procLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   fd, 1000L);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_SOCKET_READABLE)
			PQconsumeInput(streamConn);

		for (;;)
		{
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
			/* need to wait for new data */
			else if (r == 0)
				break;
			else
			{
				elog(LOG, "got %d bytes of data, type %c", r, copybuf[0]);
				if (copybuf[0] == 'w')
				{
					int hdr_len = 0;
					char *data;
					XLogRecPtr temp;

					hdr_len = 1;	/* msgtype 'w' */
					hdr_len += 8;	/* dataStart */
					hdr_len += 8;	/* walEnd */
					hdr_len += 8;	/* sendTime */

					temp = recvint64(&copybuf[1]);

					if (last_received < temp)
						last_received = temp;

					data = copybuf + hdr_len;

					process_change(data, r);
				}
			}
		}

		/* confirm all writes at once */
		if (last_received != InvalidXLogRecPtr)
			sendFeedback(streamConn, last_received,
						 localGetCurrentTimestamp(), false);
	}

	proc_exit(0);
}

/*
 * Entrypoint of this module.
 *
 * We register two worker processes here, to demonstrate how that can be done.
 */
void
_PG_init(void)
{
	BackgroundWorker	worker;
	BDRCon             *con;
	List *cons;
	ListCell   *c;
	MemoryContext old_context;

	/* guc's et al need to survive this */
	old_context = MemoryContextSwitchTo(TopMemoryContext);

	DefineCustomStringVariable("bdr.connections",
							   "List of connections",
							   NULL,
							   &connections,
							   NULL, PGC_POSTMASTER,
							   GUC_LIST_INPUT|GUC_LIST_QUOTE,
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

	/* register the worker processes.  These values are common for all of them */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = bdr_main;
	worker.bgw_sighup = bdr_sighup;
	worker.bgw_sigterm = bdr_sigterm;

	foreach(c, cons)
	{
		const char *name = (char *) lfirst(c);
		char *errmsg = NULL;
		PQconninfoOption *options;
		PQconninfoOption *cur_option;
		/* don't free! */
		char *optname_dsn = palloc(strlen(name) + 30);

		/* Values for the second worker */
		con = palloc(sizeof(BDRCon));
		con->dbname = NULL;
		con->con = (char *) lfirst(c);

		sprintf(optname_dsn, "bdr.%s.dsn", name);
		DefineCustomStringVariable(optname_dsn,
								   optname_dsn,
								   NULL,
								   &con->con,
								   NULL, PGC_POSTMASTER,
								   GUC_NOT_IN_SAMPLE,
								   NULL, NULL, NULL);

		if (!con->con)
		{
			elog(WARNING, "no connection information for %s", name);
			continue;
		}

		elog(LOG, "bgworkers, connection: %s", con->con);

		options = PQconninfoParse(con->con, &errmsg);
		if (errmsg != NULL)
		{
			char *str = pstrdup(errmsg);
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

		PQconninfoFree(options);

		worker.bgw_name = pstrdup(name);
		worker.bgw_restart_time = 5;
		worker.bgw_main_arg = (void *) con;
		RegisterBackgroundWorker(&worker);
	}
out:
	MemoryContextSwitchTo(old_context);

}


/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
typedef struct ResultRelInfo *UserTableIndexState;

static void
UserTableUpdateIndexes(Relation heapRel, HeapTuple heapTuple)
{
   /* this is largely copied together from copy.c's CopyFrom */
   EState *estate = CreateExecutorState();
   ResultRelInfo *resultRelInfo;
   List *recheckIndexes = NIL;
   TupleDesc tupleDesc = RelationGetDescr(heapRel);

   resultRelInfo = makeNode(ResultRelInfo);
   resultRelInfo->ri_RangeTableIndex = 1;      /* dummy */
   resultRelInfo->ri_RelationDesc = heapRel;
   resultRelInfo->ri_TrigInstrument = NULL;

   ExecOpenIndices(resultRelInfo);

   estate->es_result_relations = resultRelInfo;
   estate->es_num_result_relations = 1;
   estate->es_result_relation_info = resultRelInfo;

   if (resultRelInfo->ri_NumIndices > 0)
   {
       TupleTableSlot *slot = ExecInitExtraTupleSlot(estate);
       ExecSetSlotDescriptor(slot, tupleDesc);
       ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

       recheckIndexes = ExecInsertIndexTuples(slot, &heapTuple->t_self,
                                              estate);
   }

   ExecResetTupleTable(estate->es_tupleTable, false);

   ExecCloseIndices(resultRelInfo);

   FreeExecutorState(estate);
   /* FIXME: recheck the indexes */
   list_free(recheckIndexes);
}

static char *
read_tuple(char *data, size_t len, HeapTuple tuple, Oid *reloid)
{
	int64 tablenamelen;
	char *tablename;
	Oid tableoid;
	int64 nspnamelen;
	char *nspname;
	int64 tuplelen;
	Oid nspoid;
	char t;

	*reloid = InvalidOid;

	/* FIXME: unaligned data accesses */
	t = data[0];
	data += 1;
	if (t != 'T')
		elog(ERROR, "expected TUPLE, got %c", t);

	nspnamelen = recvint64(&data[0]);
	data += 8;
	nspname = data;
	data += nspnamelen;

	tablenamelen = recvint64(&data[0]);
	data += 8;
	tablename = data;
	data += tablenamelen;

	tuplelen = recvint64(&data[0]);
	data += 8;

	tuple->t_data = (HeapTupleHeader)data;
	tuple->t_len = tuplelen;
	data += tuplelen;

	elog(LOG, "TUPLE IN %*s.%*s of len %u",
		 (int)nspnamelen, nspname,
		 (int)tablenamelen, tablename, (uint32)tuplelen);

	nspoid = get_namespace_oid(nspname, false);

	tableoid = get_relname_relid(tablename, nspoid);
	if (tableoid == InvalidOid)
		elog(ERROR, "could not resolve tablename %s", tablename);

	*reloid = tableoid;

	return data;
}

static HeapTuple
ExtractKeyTuple(Relation relation, HeapTuple tp)
{
	HeapTuple idx_tuple = NULL;
	TupleDesc desc = RelationGetDescr(relation);
	Relation idx_rel;
	TupleDesc idx_desc;
	Datum idx_vals[INDEX_MAX_KEYS];
	bool idx_isnull[INDEX_MAX_KEYS];
	int natt;

	/* needs to already have been fetched? */
	if (relation->rd_indexvalid == 0)
		RelationGetIndexList(relation);

	if (!OidIsValid(relation->rd_primary))
	{
		elog(DEBUG1, "Could not find primary key for table with oid %u",
			 RelationGetRelid(relation));
	}
	else
	{
		idx_rel = RelationIdGetRelation(relation->rd_primary);
		idx_desc = RelationGetDescr(idx_rel);

		for (natt = 0; natt < idx_desc->natts; natt++)
		{
			int attno = idx_rel->rd_index->indkey.values[natt];
			if (attno == ObjectIdAttributeNumber)
			{
				idx_vals[natt] = HeapTupleGetOid(tp);
				idx_isnull[natt] = false;
			}
			else
			{
				idx_vals[natt] =
					fastgetattr(tp, attno, desc, &idx_isnull[natt]);
			}
			Assert(!idx_isnull[natt]);
		}
		idx_tuple = heap_form_tuple(idx_desc, idx_vals, idx_isnull);
		RelationClose(idx_rel);
	}
	return idx_tuple;
}

static void
build_scan_key(ScanKey skey, Relation rel, Relation idxrel, HeapTuple key)
{
	int attoff;
	Datum indclassDatum;
	bool isnull;
	oidvector *opclass;

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	Assert(!isnull);

	for (attoff = 0; attoff < RelationGetNumberOfAttributes(idxrel); attoff++)
	{
		Oid operator;
		Oid opfamily;
		RegProcedure regop;
		int pkattno = attoff + 1;

		opfamily = get_opclass_family(opclass->values[attoff]);

		operator = get_opfamily_member(opfamily, attnumTypeId(idxrel, pkattno),
									   attnumTypeId(idxrel, pkattno), BTEqualStrategyNumber);

		regop = get_opcode(operator);

		/*
		 * FIXME: deform index tuple instead of fastgetatt'ing
		 * everything
		 */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					fastgetattr(key, pkattno,
								RelationGetDescr(idxrel), &isnull));
		Assert(!isnull);
	}
}

static bool
find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel, ItemPointer tid)
{
	HeapTuple tuple;
	bool found = false;
	IndexScanDesc scan;

	scan = index_beginscan(rel, idxrel,
						   GetTransactionSnapshot(),
						   RelationGetNumberOfAttributes(idxrel),
						   0);
	index_rescan(scan, skey, RelationGetNumberOfAttributes(idxrel), NULL, 0);

	while ((tuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		if (found)
		{
			elog(ERROR, "WTF, more than one tuple found via pk???");
		}
		found = true;
		ItemPointerCopy(&tuple->t_self, tid);
	}
	index_endscan(scan);

	return found;
}
