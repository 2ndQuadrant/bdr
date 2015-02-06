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

PG_FUNCTION_INFO_V1(bdr_get_remote_nodeinfo);
PG_FUNCTION_INFO_V1(bdr_test_replication_connection);
PG_FUNCTION_INFO_V1(bdr_test_remote_connectback);

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
	pfree(ri->sysid_str);
	pfree(ri->variant);
	pfree(ri->version);
}

/*
 * The implementation guts of bdr_get_remote_nodeinfo, callable with
 * a pre-existing connection.
 */
void
bdr_get_remote_nodeinfo_internal(PGconn *conn, struct remote_node_info *ri)
{
	PGresult		*res, *res2;
	int				i;
	char			*remote_bdr_version_str;
	int				parsed_version_num;

	/* Make sure BDR is actually present and active on the remote */
	bdr_ensure_ext_installed(conn);

	/* Acquire sysid, timeline, dboid, version and variant */
	res = PQexec(conn, "SELECT sysid, timeline, dboid, "
					   "bdr.bdr_variant() AS variant, "
					   "bdr.bdr_version() AS version, "
					   "current_setting('is_superuser') AS issuper "
					   "FROM bdr.bdr_get_local_nodeid()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(ERROR,
				(errmsg("getting remote node id failed"),
				errdetail("SELECT sysid, timeline, dboid FROM bdr.bdr_get_local_nodeid() failed with: %s",
					PQerrorMessage(conn))));
	}

	Assert(PQnfields(res) == 6);

	if (PQntuples(res) != 1)
		elog(ERROR, "Got %d tuples instead of expected 1", PQntuples(res));

	for (i = 0; i < 6; i++)
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
	ri->variant = pstrdup(PQgetvalue(res, 0, 3));
	remote_bdr_version_str = PQgetvalue(res, 0, 4);
	ri->version = pstrdup(remote_bdr_version_str);
	/* To be filled later: numeric version, min remote version */
	ri->is_superuser = DatumGetBool(
			DirectFunctionCall1(boolin, CStringGetDatum(PQgetvalue(res, 0, 5))));

	/*
	 * Even though we should be able to get it from bdr_version_num, always
	 * parse the BDR version so that the parse code gets sanity checked.
	 */
	parsed_version_num = bdr_parse_version(remote_bdr_version_str, NULL, NULL,
										   NULL, NULL);

	/*
	 * If bdr_version_num() is present then the remote version is greater
	 * than 0.8.0 and we can safely use it and bdr_min_remote_version_num()
	 */
	res2 = PQexec(conn, "SELECT 1 FROM pg_proc p "
						"INNER JOIN pg_namespace n ON (p.pronamespace = n.oid) "
						"WHERE n.nspname = 'bdr' AND p.proname = 'bdr_version_num';");

	if (PQresultStatus(res2) != PGRES_TUPLES_OK)
	{
		ereport(ERROR,
				(errmsg("getting remote available functions failed"),
				 errdetail("Querying remote failed with: %s", PQerrorMessage(conn))));
	}

	Assert(PQnfields(res2) == 1);
	Assert(PQntuples(res2) == 0 || PQntuples(res2) == 1);

	if (PQntuples(res2) == 1)
	{
		/*
		 * Can safely query for numeric version and min remote version
		 */
		PQclear(res2);

		res2 = PQexec(conn, "SELECT bdr.bdr_version_num(), "
							"       bdr.bdr_min_remote_version_num();");

		if (PQresultStatus(res2) != PGRES_TUPLES_OK)
		{
			ereport(ERROR,
					(errmsg("getting remote numeric BDR version failed"),
					 errdetail("Querying remote failed with: %s", PQerrorMessage(conn))));
		}

		Assert(PQnfields(res2) == 2);
		Assert(PQntuples(res2) == 1);

		ri->version_num = atoi(PQgetvalue(res2, 0, 0));
		ri->min_remote_version_num = atoi(PQgetvalue(res2, 0, 1));

		if (ri->version_num != parsed_version_num)
			elog(WARNING, "parsed bdr version %d from string %s != returned bdr version %d",
				 parsed_version_num, remote_bdr_version_str, ri->version_num);

		PQclear(res2);
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
		PQclear(res2);

		/* Shouldn't happen, but as a sanity check: */
		if (parsed_version_num > 900)
			elog(ERROR, "Remote BDR version reported as %s (n=%d) but bdr.bdr_version_num() missing",
				 remote_bdr_version_str, parsed_version_num);

		ri->version_num = parsed_version_num;
		ri->min_remote_version_num = 700;
	}

	PQclear(res);
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

		values[0] = CStringGetTextDatum(ri.sysid_str);
		values[1] = ObjectIdGetDatum(ri.timeline);
		values[2] = ObjectIdGetDatum(ri.dboid);
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
 * parameter.
 *
 * If non-null, the node identity information arguments will be checked
 * against the identity reported by the connection.
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
	int				i;
	char			*remote_bdr_version_str;
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

	for (i = 0; i < 8; i++)
	{
		if (PQgetisnull(res, 0, i))
			elog(ERROR, "Unexpectedly null field %s", PQfname(res, i));
	}

	ri->sysid_str = pstrdup(PQgetvalue(res, 0, 0));

	if (sscanf(ri->sysid_str, UINT64_FORMAT, &ri->sysid) != 1)
		elog(ERROR, "could not parse sysid %s", ri->sysid_str);

	ri->timeline = DatumGetObjectId(
			DirectFunctionCall1(oidin, CStringGetDatum(PQgetvalue(res, 0, 1))));
	ri->dboid = DatumGetObjectId(
			DirectFunctionCall1(oidin, CStringGetDatum(PQgetvalue(res, 0, 2))));
	ri->variant = pstrdup(PQgetvalue(res, 0, 3));
	remote_bdr_version_str = PQgetvalue(res, 0, 4);
	ri->version = pstrdup(remote_bdr_version_str);
	ri->version_num = atoi(PQgetvalue(res, 0, 5));
	ri->min_remote_version_num =  atoi(PQgetvalue(res, 0, 6));
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

		values[0] = CStringGetTextDatum(ri.sysid_str);
		values[1] = ObjectIdGetDatum(ri.timeline);
		values[2] = ObjectIdGetDatum(ri.dboid);
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
