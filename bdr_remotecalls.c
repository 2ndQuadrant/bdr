/* -------------------------------------------------------------------------
 *
 * bdr_remotecalls.c
 *     Make libpq requests to a remote BDR instance
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_remotecalls.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"
#include "bdr_internal.h"

#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "libpq/pqformat.h"

#include "access/heapam.h"
#include "access/xact.h"

#include "catalog/pg_type.h"

#include "executor/spi.h"

#include "bdr_replication_identifier.h"
#include "replication/walreceiver.h"

#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"

PGDLLEXPORT Datum bdr_get_remote_nodeinfo(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum bdr_test_replication_connection(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum bdr_test_remote_connectback(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum bdr_copytable_test(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum bdr_drop_remote_slot(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bdr_get_remote_nodeinfo);
PG_FUNCTION_INFO_V1(bdr_test_replication_connection);
PG_FUNCTION_INFO_V1(bdr_test_remote_connectback);
PG_FUNCTION_INFO_V1(bdr_copytable_test);
PG_FUNCTION_INFO_V1(bdr_drop_remote_slot);

/*
 * Make standard postgres connection, ERROR on failure.
 */
PGconn*
bdr_connect_nonrepl(const char *connstring, const char *appnamesuffix)
{
	PGconn			*nonrepl_conn;
	StringInfoData	dsn;

	initStringInfo(&dsn);
	appendStringInfo(&dsn,
					"%s fallback_application_name='"BDR_LOCALID_FORMAT":%s'",
					connstring, BDR_LOCALID_FORMAT_ARGS, appnamesuffix);

	/*
	 * Test to see if there's an entry in the remote's bdr.bdr_nodes for our
	 * system identifier. If there is, that'll tell us what stage of startup
	 * we are up to and let us resume an incomplete start.
	 */
	nonrepl_conn = PQconnectdb(dsn.data);
	if (PQstatus(nonrepl_conn) != CONNECTION_OK)
	{
		ereport(FATAL,
				(errmsg("could not connect to the server in non-replication mode: %s",
						PQerrorMessage(nonrepl_conn)),
				 errdetail("dsn was: %s", dsn.data)));
	}

	return nonrepl_conn;
}

/*
 * Close a connection if it exists. The connection passed
 * is a pointer to a *PGconn; if the target is NULL, it's
 * presumed not inited or already closed and is ignored.
 */
void
bdr_cleanup_conn_close(int code, Datum connptr)
{
	PGconn **conn_p;
	PGconn *conn;

	conn_p = (PGconn**) DatumGetPointer(connptr);
	Assert(conn_p != NULL);
	conn = *conn_p;

	if (conn == NULL)
		return;
	if (PQstatus(conn) != CONNECTION_OK)
		return;
	PQfinish(conn);
}

/*
 * Frees contents of a remote_node_info (but not the struct its self)
 */
void
free_remote_node_info(remote_node_info *ri)
{
	if (ri->sysid_str != NULL)
		pfree(ri->sysid_str);
	if (ri->variant != NULL)
		pfree(ri->variant);
	if (ri->version != NULL)
		pfree(ri->version);
}

/*
 * Given two connections, execute a COPY ... TO stdout on one connection
 * and feed the results to a COPY ... FROM stdin on the other connection
 * for the purpose of copying a set of rows between two nodes.
 *
 * It copies bdr_connections entries from the remote table to the
 * local table of the same name, optionally with a filtering query.
 *
 * "from" here is from the client perspective, i.e. to copy from
 * the server we "COPY ... TO stdout", and to copy to the server we
 * "COPY ... FROM stdin".
 *
 * On failure an ERROR will be raised.
 *
 * Note that query parameters are not supported for COPY, so values must be
 * carefully interpolated into the SQL if you're using a query, not just a
 * table name. Be careful of SQL injection opportunities.
 */
void
bdr_copytable(PGconn *copyfrom_conn, PGconn *copyto_conn,
		const char * copyfrom_query, const char *copyto_query)
{
	PGresult *copyfrom_result;
	PGresult *copyto_result;
	int	copyinresult, copyoutresult;
	char * copybuf;

	copyfrom_result = PQexec(copyfrom_conn, copyfrom_query);
	if (PQresultStatus(copyfrom_result) != PGRES_COPY_OUT)
	{
		ereport(ERROR,
				(errmsg("execution of COPY ... TO stdout failed"),
				 errdetail("Query '%s': %s", copyfrom_query,
					 PQerrorMessage(copyfrom_conn))));
	}

	copyto_result = PQexec(copyto_conn, copyto_query);
	if (PQresultStatus(copyto_result) != PGRES_COPY_IN)
	{
		ereport(ERROR,
				(errmsg("execution of COPY ... FROM stdout failed"),
				 errdetail("Query '%s': %s", copyto_query,
					 PQerrorMessage(copyto_conn))));
	}

	while ((copyoutresult = PQgetCopyData(copyfrom_conn, &copybuf, false)) > 0)
	{
		if ((copyinresult = PQputCopyData(copyto_conn, copybuf, copyoutresult)) != 1)
		{
			ereport(ERROR,
					(errmsg("writing to destination table failed"),
					 errdetail("destination connection reported: %s",
						 PQerrorMessage(copyto_conn))));
		}
		PQfreemem(copybuf);
	}

	if (copyoutresult != -1)
	{
		ereport(ERROR,
				(errmsg("reading from origin table/query failed"),
				 errdetail("source connection returned %d: %s",
					copyoutresult, PQerrorMessage(copyfrom_conn))));
	}

	// Send local finish
	if (PQputCopyEnd(copyto_conn, NULL) != 1)
	{
		ereport(ERROR,
				(errmsg("sending copy-completion to destination connection failed"),
				 errdetail("destination connection reported: %s",
					 PQerrorMessage(copyto_conn))));
	}
}

/*
 * Test function for bdr_copytable.
 */
Datum
bdr_copytable_test(PG_FUNCTION_ARGS)
{
	const char * fromdsn = PG_GETARG_CSTRING(0);
	const char * todsn = PG_GETARG_CSTRING(1);
	const char * fromquery = PG_GETARG_CSTRING(2);
	const char * toquery = PG_GETARG_CSTRING(3);

	PGconn *fromconn, *toconn;

	fromconn = PQconnectdb(fromdsn);
	if (PQstatus(fromconn) != CONNECTION_OK)
		elog(ERROR, "from conn failed");

	toconn = PQconnectdb(todsn);
	if (PQstatus(toconn) != CONNECTION_OK)
		elog(ERROR, "to conn failed");

	bdr_copytable(fromconn, toconn, fromquery, toquery);

	PQfinish(fromconn);
	PQfinish(toconn);

	PG_RETURN_VOID();
}

static bool
bdr_remote_has_bdr_func(PGconn *conn, const char *funcname)
{
	PGresult *res;
	const char * params[1];
	bool found;

	params[0] = funcname;

	/* Check if a function is defined in the bdr namespace in pg_proc */
	res = PQexecParams(conn, "SELECT 1 FROM pg_proc p "
							 "INNER JOIN pg_namespace n ON (p.pronamespace = n.oid) "
							 "WHERE n.nspname = 'bdr' AND p.proname = $1;",
							 1, NULL, params, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(ERROR,
				(errmsg("getting remote available functions failed"),
				 errdetail("Querying remote failed with: %s", PQerrorMessage(conn))));
	}

	Assert(PQnfields(res) == 1);
	Assert(PQntuples(res) == 0 || PQntuples(res) == 1);

	found = (PQntuples(res) == 1);

	PQclear(res);

	return found;
}

/*
 * The implementation guts of bdr_get_remote_nodeinfo, callable with
 * a pre-existing connection.
 */
void
bdr_get_remote_nodeinfo_internal(PGconn *conn, struct remote_node_info *ri)
{
	PGresult		*res;
	int				i;
	char			*remote_bdr_version_str;
	int				parsed_version_num;

	/* Make sure BDR is actually present and active on the remote */
	bdr_ensure_ext_installed(conn);

	/*
	 * Acquire remote version string. Present since 0.7.x. This lets us
	 * decide if we're going to ask for more info. Can also safely find
	 * out if we're superuser at this point.
	 */
	res = PQexec(conn, "SELECT bdr.bdr_version(), "
					   "       current_setting('is_superuser') AS issuper");

	Assert(PQnfields(res) == 2);
	Assert(PQntuples(res) == 1);

	remote_bdr_version_str = PQgetvalue(res, 0, 0);
	ri->version = pstrdup(remote_bdr_version_str);
	ri->is_superuser = DatumGetBool(
			DirectFunctionCall1(boolin, CStringGetDatum(PQgetvalue(res, 0, 1))));

	PQclear(res);

	/*
	 * Even though we should be able to get it from bdr_version_num, always
	 * parse the BDR version so that the parse code gets sanity checked,
	 * and so that we notice if the remote version is too old to have
	 * bdr_version_num.
	 */
	parsed_version_num = bdr_parse_version(remote_bdr_version_str, NULL, NULL,
										   NULL, NULL);

	ri->version_num = parsed_version_num;

	if (bdr_remote_has_bdr_func(conn, "bdr_version_num"))
	{
		/*
		 * Can safely query for numeric version and min remote version.
		 * They were added at the same time. The variant is also available;
		 * while it was added earlier, in reality nobody's going to be
		 * using it or caring.
		 */
		res = PQexec(conn, "SELECT bdr.bdr_version_num(), "
						   "       bdr.bdr_variant() AS variant, "
						   "       bdr.bdr_min_remote_version_num();");

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			ereport(ERROR,
					(errmsg("getting remote numeric BDR version failed"),
					 errdetail("Querying remote failed with: %s", PQerrorMessage(conn))));
		}

		Assert(PQnfields(res) == 3);
		Assert(PQntuples(res) == 1);

		ri->version_num = atoi(PQgetvalue(res, 0, 0));
		ri->variant = pstrdup(PQgetvalue(res, 0, 1));
		ri->min_remote_version_num = atoi(PQgetvalue(res, 0, 2));

		if (ri->version_num != parsed_version_num)
			elog(WARNING, "parsed bdr version %d from string %s != returned bdr version %d",
				 parsed_version_num, remote_bdr_version_str, ri->version_num);

		PQclear(res);
	}
	else
	{
		/*
		 * Must be an old version, can't get numeric version.
		 *
		 * All supported versions prior to introduction of bdr_version_num()
		 * have a min_remote_version_num of 000700, we can safely report that.
		 * For the version we have to parse it from the text version.
		 */

		/* Shouldn't happen, but as a sanity check: */
		if (parsed_version_num > 900)
			elog(ERROR, "Remote BDR version reported as %s (n=%d) but bdr.bdr_version_num() missing",
				 remote_bdr_version_str, parsed_version_num);

		ri->version_num = parsed_version_num;
		ri->variant = pstrdup("BDR");
		ri->min_remote_version_num = 700;
	}

	/*
	 * If the node is new enough, get the remote peer's identity. Otherwise
	 * zero them out.
	 */
	if (bdr_remote_has_bdr_func(conn, "bdr_get_local_nodeid"))
	{
		/* Acquire sysid, timeline, dboid */
		res = PQexec(conn, "SELECT sysid, timeline, dboid "
						   "FROM bdr.bdr_get_local_nodeid()");

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			ereport(ERROR,
					(errmsg("getting remote node id failed"),
					errdetail("SELECT sysid, timeline, dboid FROM bdr.bdr_get_local_nodeid() failed with: %s",
						PQerrorMessage(conn))));
		}

		Assert(PQnfields(res) == 3);

		if (PQntuples(res) != 1)
			elog(ERROR, "Got %d tuples instead of expected 1", PQntuples(res));

		for (i = 0; i < 3; i++)
		{
			if (PQgetisnull(res, 0, i))
				elog(ERROR, "Unexpectedly null field %s", PQfname(res, i));
		}

		ri->sysid_str = pstrdup(PQgetvalue(res, 0, 0));

		if (sscanf(ri->sysid_str, UINT64_FORMAT, &ri->sysid) != 1)
			elog(ERROR, "could not parse remote sysid %s", ri->sysid_str);

		ri->timeline = DatumGetObjectId(
				DirectFunctionCall1(oidin, CStringGetDatum(PQgetvalue(res, 0, 1))));
		ri->dboid = DatumGetObjectId(
				DirectFunctionCall1(oidin, CStringGetDatum(PQgetvalue(res, 0, 2))));

		PQclear(res);
	}
	else
	{
		/*
		 * No way to know the node id on old versions.
		 *
		 * Indicate this with a null sysid and invalid timeline
		 * and dboid. We can actually get the dboid from the
		 * peer but there's no point.
		 */
		ri->sysid_str = NULL;
		ri->sysid = 0;
		ri->timeline = DatumGetObjectId(InvalidOid);
		ri->dboid = DatumGetObjectId(InvalidOid);
	}

}

Datum
bdr_get_remote_nodeinfo(PG_FUNCTION_ARGS)
{
	const char *remote_node_dsn = text_to_cstring(PG_GETARG_TEXT_P(0));
	Datum		values[8];
	bool		isnull[8] = {false, false, false, false, false, false, false, false};
	TupleDesc	tupleDesc;
	HeapTuple	returnTuple;
	PGconn		*conn;

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	conn = bdr_connect_nonrepl(remote_node_dsn, "bdrnodeinfo");

	PG_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&conn));
	{
		struct remote_node_info ri;

		bdr_get_remote_nodeinfo_internal(conn, &ri);

		if (ri.sysid_str != NULL)
		{
			values[0] = CStringGetTextDatum(ri.sysid_str);
			values[1] = ObjectIdGetDatum(ri.timeline);
			values[2] = ObjectIdGetDatum(ri.dboid);
		}
		else
		{
			/* Old peer version lacks sysid info */
			values[0] = (Datum)0;
			isnull[0] = true;
			values[1] = (Datum)0;
			isnull[1] = true;
			values[2] = (Datum)0;
			isnull[2] = true;
		}
		values[3] = CStringGetTextDatum(ri.variant);
		values[4] = CStringGetTextDatum(ri.version);
		values[5] = Int32GetDatum(ri.version_num);
		values[6] = Int32GetDatum(ri.min_remote_version_num);
		values[7] = BoolGetDatum(ri.is_superuser);

		returnTuple = heap_form_tuple(tupleDesc, values, isnull);

		free_remote_node_info(&ri);
	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&conn));

	PQfinish(conn);

	PG_RETURN_DATUM(HeapTupleGetDatum(returnTuple));
}

/*
 * Test a given dsn as a replication connection, appending the replication
 * parameter, and return the node identity information from IDENTIFY SYSTEM.
 *
 * This can be used safely against the local_dsn, as it does not enforce
 * that the local node ID differ from the identity on the other end.
 */
Datum
bdr_test_replication_connection(PG_FUNCTION_ARGS)
{
	const char	*conninfo = text_to_cstring(PG_GETARG_TEXT_P(0));
	TupleDesc	tupleDesc;
	HeapTuple	returnTuple;
	PGconn		*conn;
	NameData	appname;
	uint64		remote_sysid;
	TimeLineID	remote_tlid;
	Oid			remote_dboid;
	Datum		values[3];
	bool		isnull[3] = {false, false, false};
	char		sysid_str[33];

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	strncpy(NameStr(appname), "BDR test connection", NAMEDATALEN);

	conn = bdr_connect(conninfo, &appname, &remote_sysid, &remote_tlid,
					   &remote_dboid);

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, remote_sysid);
	sysid_str[sizeof(sysid_str)-1] = '\0';

	values[0] = CStringGetTextDatum(sysid_str);
	values[1] = ObjectIdGetDatum(remote_tlid);
	values[2] = ObjectIdGetDatum(remote_dboid);

	returnTuple = heap_form_tuple(tupleDesc, values, isnull);

	PQfinish(conn);

	PG_RETURN_DATUM(HeapTupleGetDatum(returnTuple));
}

void
bdr_test_remote_connectback_internal(PGconn *conn,
		struct remote_node_info *ri, const char *my_dsn)
{
	PGresult		*res;
	const char *	mydsn_values[1];
	Oid				mydsn_types[1] = { TEXTOID };

	mydsn_values[0] = my_dsn;

	/* Make sure BDR is actually present and active on the remote */
	bdr_ensure_ext_installed(conn);

	/*
	 * Ask the remote to connect back to us in replication mode, then
	 * discard the results.
	 */
	res = PQexecParams(conn, "SELECT sysid, timeline, dboid "
							 "FROM bdr.bdr_test_replication_connection($1)",
					   1, mydsn_types, mydsn_values, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		/* TODO clone remote error to local */
		ereport(ERROR,
				(errmsg("connection from remote back to local in replication mode failed"),
				errdetail("remote reported: %s", PQerrorMessage(conn))));
	}

	PQclear(res);

	/*
	 * Acquire bdr_get_remote_nodeinfo's results from running it on the remote
	 * node to connect back to us.
	 */
	res = PQexecParams(conn, "SELECT sysid, timeline, dboid, variant, version, "
							 "       version_num, min_remote_version_num, is_superuser "
							 "FROM bdr.bdr_get_remote_nodeinfo($1)",
					   1, mydsn_types, mydsn_values, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		/* TODO clone remote error to local */
		ereport(ERROR,
				(errmsg("connection from remote back to local failed"),
				errdetail("remote reported: %s", PQerrorMessage(conn))));
	}

	Assert(PQnfields(res) == 8);

	if (PQntuples(res) != 1)
		elog(ERROR, "Got %d tuples instead of expected 1", PQntuples(res));

	ri->sysid_str = NULL;
	ri->sysid = 0;
	ri->timeline = 0;
	ri->dboid = InvalidOid;
	ri->variant = NULL;
	ri->version = NULL;
	ri->version_num = 0;
	ri->min_remote_version_num = 0;
	ri->is_superuser = true;

	if (!PQgetisnull(res, 0, 0))
	{
		ri->sysid_str = pstrdup(PQgetvalue(res, 0, 0));

		if (sscanf(ri->sysid_str, UINT64_FORMAT, &ri->sysid) != 1)
			elog(ERROR, "could not parse sysid %s", ri->sysid_str);
	}

	if (!PQgetisnull(res, 0, 1))
	{
		ri->timeline = DatumGetObjectId(
				DirectFunctionCall1(oidin, CStringGetDatum(PQgetvalue(res, 0, 1))));
	}

	if (!PQgetisnull(res, 0, 2))
	{
		ri->dboid = DatumGetObjectId(
				DirectFunctionCall1(oidin, CStringGetDatum(PQgetvalue(res, 0, 2))));
	}

	if (PQgetisnull(res, 0, 3))
		elog(ERROR, "variant should never be null");
	ri->variant = pstrdup(PQgetvalue(res, 0, 3));

	if (!PQgetisnull(res, 0, 4))
		ri->version = pstrdup(PQgetvalue(res, 0, 4));

	if (!PQgetisnull(res, 0, 5))
		ri->version_num = atoi(PQgetvalue(res, 0, 5));

	if (!PQgetisnull(res, 0, 6))
		ri->min_remote_version_num =  atoi(PQgetvalue(res, 0, 6));

	if (!PQgetisnull(res, 0, 7))
		ri->is_superuser = DatumGetBool(
				DirectFunctionCall1(boolin, CStringGetDatum(PQgetvalue(res, 0, 7))));

	PQclear(res);
}

/*
 * Establish a connection to a remote node and use that connection to connect
 * back to the local node in both replication and non-replication modes.
 *
 * This is used during setup to make sure the local node is useable.
 *
 * Reports the same data as bdr_get_remote_nodeinfo, but it's reported
 * about the local node via the remote node.
 */
Datum
bdr_test_remote_connectback(PG_FUNCTION_ARGS)
{
	const char *remote_node_dsn;
	const char *my_dsn;
	Datum		values[8];
	bool		isnull[8] = {false, false, false, false, false, false, false, false};
	TupleDesc	tupleDesc;
	HeapTuple	returnTuple;
	PGconn		*conn;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		elog(ERROR, "both arguments must be non-null");

	remote_node_dsn = text_to_cstring(PG_GETARG_TEXT_P(0));
	my_dsn = text_to_cstring(PG_GETARG_TEXT_P(1));

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	conn = bdr_connect_nonrepl(remote_node_dsn, "bdrconnectback");

	PG_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&conn));
	{
		struct remote_node_info ri;

		bdr_test_remote_connectback_internal(conn, &ri, my_dsn);

		if (ri.sysid_str != NULL)
			values[0] = CStringGetTextDatum(ri.sysid_str);
		else
			isnull[0] = true;

		if (ri.timeline != 0)
			values[1] = ObjectIdGetDatum(ri.timeline);
		else
			isnull[1] = true;

		if (ri.dboid != InvalidOid)
			values[2] = ObjectIdGetDatum(ri.dboid);
		else
			isnull[2] = true;

		if (ri.variant != NULL)
			values[3] = CStringGetTextDatum(ri.variant);
		else
			isnull[3] = true;

		if (ri.version != NULL)
			values[4] = CStringGetTextDatum(ri.version);
		else
			isnull[4] = true;

		if (ri.version_num != 0)
			values[5] = Int32GetDatum(ri.version_num);
		else
			isnull[5] = true;

		if (ri.min_remote_version_num != 0)
			values[6] = Int32GetDatum(ri.min_remote_version_num);
		else
			isnull[6] = true;

		values[7] = BoolGetDatum(ri.is_superuser);

		returnTuple = heap_form_tuple(tupleDesc, values, isnull);

		free_remote_node_info(&ri);
	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
								PointerGetDatum(&conn));

	PQfinish(conn);

	PG_RETURN_DATUM(HeapTupleGetDatum(returnTuple));
}


/*
 * Drops replication slot on remote node that has been used by the local node.
 */
Datum
bdr_drop_remote_slot(PG_FUNCTION_ARGS)
{
	const char *remote_sysid_str = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid			remote_tli = PG_GETARG_OID(1);
	Oid			remote_dboid = PG_GETARG_OID(2);
	uint64		remote_sysid;
	PGconn	   *conn;
	PGresult   *res;
	NameData	slotname;
	BdrConnectionConfig *cfg;

	if (sscanf(remote_sysid_str, UINT64_FORMAT, &remote_sysid) != 1)
		elog(ERROR, "Parsing of remote sysid as uint64 failed");

	cfg = bdr_get_connection_config(remote_sysid, remote_tli, remote_dboid, false);
	conn = bdr_connect_nonrepl(cfg->dsn, "bdr_drop_replication_slot");
	bdr_free_connection_config(cfg);

	PG_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&conn));
	{
		struct remote_node_info ri;
		const char *	values[1];
		Oid				types[1] = { TEXTOID };

		/* Try connecting and build slot name from retrieved info */
		bdr_get_remote_nodeinfo_internal(conn, &ri);
		bdr_slot_name(&slotname, GetSystemIdentifier(), ThisTimeLineID,
					  MyDatabaseId, remote_dboid);
		free_remote_node_info(&ri);

		values[0] = NameStr(slotname);

		/* Check if the slot exists */
		res = PQexecParams(conn,
						   "SELECT plugin "
						   "FROM pg_catalog.pg_replication_slots "
						   "WHERE slot_name = $1",
						   1, types, values, NULL, NULL, 0);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			ereport(ERROR,
					(errmsg("getting remote slot info failed"),
					errdetail("SELECT FROM pg_catalog.pg_replication_slots failed with: %s",
						PQerrorMessage(conn))));
		}

		/* Slot not found return false */
		if (PQntuples(res) == 0)
		{
			PQfinish(conn);
			PG_RETURN_BOOL(false);
		}

		/* Slot found, validate that it's BDR slot */
		if (PQgetisnull(res, 0, 0))
			elog(ERROR, "Unexpectedly null field %s", PQfname(res, 0));

		if (strcmp("bdr", PQgetvalue(res, 0, 0)) != 0)
			ereport(ERROR,
					(errmsg("slot %s is not BDR slot", NameStr(slotname))));

		res = PQexecParams(conn, "SELECT pg_drop_replication_slot($1)",
						   1, types, values, NULL, NULL, 0);

		/* And finally, drop the slot. */
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			ereport(ERROR,
					(errmsg("remote slot drop failed"),
					errdetail("SELECT pg_drop_replication_slot() failed with: %s",
						PQerrorMessage(conn))));
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
								PointerGetDatum(&conn));

	PQfinish(conn);

	PG_RETURN_BOOL(true);
}
