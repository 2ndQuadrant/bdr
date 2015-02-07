/* -------------------------------------------------------------------------
 *
 * bdr_upgrade.c
 *     Support for upgrading between BDR versions
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_upgrade.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"

#include "libpq-fe.h"
#include "miscadmin.h"

#include "libpq/pqformat.h"

#include "catalog/pg_type.h"

#include "storage/ipc.h"

PGDLLEXPORT Datum bdr_upgrade_to_090(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bdr_upgrade_to_090);

static void
bdr_upgrade_to_090_insert_connection( PGconn *conn,
		const char *local_sysid, const char *local_timeline,
		const char *local_dboid, const char *my_conninfo)
{
	PGresult		*res;
	const char		*values[8];
	Oid				types[8] =
		{ TEXTOID, OIDOID, OIDOID, TEXTOID, OIDOID, OIDOID, BOOLOID, TEXTOID };

	values[0] = local_sysid;
	values[1] = local_timeline;
	values[2] = local_dboid;
	values[3] = "0";
	values[4] = "0";
	values[5] = "0";
	values[6] = "f";
	values[7] = &my_conninfo[0];
	/* TODO: replication sets too! */

	res = PQexecParams(conn, "INSERT INTO bdr.bdr_connections\n"
							 "(conn_sysid, conn_timeline, conn_dboid,\n"
							 " conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,\n"
							 " conn_is_unidirectional, conn_dsn)\n"
							 "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
					   8, types, values, NULL, NULL, false);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		elog(ERROR, "inserting local info into bdr_connections failed with %s: %s\n",
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	PQclear(res);
}

/*
 * Utility function for upgrading a BDR node running 0.8.0 or older to 0.9.0
 * (dynamic configuration).
 *
 * This function is only used for the 2nd and subsequent nodes. It is not
 * required or useful for upgrading the first node.
 *
 * This does some sanity checks to ensure the local node isn't already joined
 * and that the remote node is actually a known peer with a bdr_nodes entry.
 *
 * It then copies the remote end's bdr_connections entries to the local node so
 * the local node knows which peers to connect to. It inserts a copy of the
 * local node's bdr_connections entry in the remote and tells the local and
 * remote nodes to refresh their worker lists.
 *
 * This is one long function because it's one-shot code. It's written in C
 * so it can re-use libpq connections across multiple steps, doing everything
 * in one transaction.
 */
Datum
bdr_upgrade_to_090(PG_FUNCTION_ARGS)
{
	const char  *my_conninfo = PG_GETARG_CSTRING(0);
	const char	*remote_conninfo;
	const char  *my_local_conninfo = NULL;
	PGconn		*local_conn = NULL;
	const char	*local_dsn;

	char		local_sysid_str[33];
	char		local_timeline_str[33];
	char		local_dboid_str[33];

	stringify_my_node_identity(local_sysid_str, sizeof(local_sysid_str),
							   local_timeline_str, sizeof(local_timeline_str),
							   local_dboid_str, sizeof(local_dboid_str));

	if (!PG_ARGISNULL(1))
	{
		my_local_conninfo = PG_GETARG_CSTRING(1);
		local_dsn = my_local_conninfo;
	}
	else
	{
		local_dsn = my_conninfo;
	}

	if (PG_ARGISNULL(2))
	{
		elog(NOTICE, "upgrading the first node of a BDR group (remote_conninfo was null)");
		remote_conninfo = NULL;
	}
	else
	{
		elog(NOTICE, "upgrading the local node by connecting to an already upgraded peer node");
		remote_conninfo = PG_GETARG_CSTRING(2);
	}

	/*
	 * Connect to the local node in non-replication mode.
	 *
	 * We'll use this connection to COPY pg_connections data, instead of having
	 * to mess around constructing and deconstructing pg_connections tuples. It
	 * also lets us commit autonomously.
	 */
	local_conn = PQconnectdb(local_dsn);

	if (PQstatus(local_conn) != CONNECTION_OK)
	{
		ereport(ERROR,
				(errmsg("connection to supplied local dsn '%s' failed", local_dsn),
				 errdetail("Connection failed with %s", PQerrorMessage(local_conn))));
	}

	PG_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&local_conn));
	{
		PGconn *remote_conn = NULL;
		PGresult *res;
		remote_node_info	ri, li, li_via_remote;
		Oid			nodeid_types[3] = { TEXTOID, OIDOID, OIDOID };
		const char	*local_nodeid_values[3];

		const char * const bdr_nodes_query =
			"SELECT 1 FROM bdr.bdr_nodes "
			"WHERE node_sysid = $1 AND node_timeline = $2 AND node_dboid = $3";

		const char * const setup_query =
			"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
			"SET search_path = bdr, pg_catalog;\n"
			"SET bdr.permit_unsafe_ddl_commands = on;\n"
			"SET bdr.skip_ddl_replication = on;\n"
			"SET bdr.skip_ddl_locking = on;\n"
			"LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;\n"
			"LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;\n";

		local_nodeid_values[0] = &local_sysid_str[0];
		local_nodeid_values[1] = &local_timeline_str[0];
		local_nodeid_values[2] = &local_dboid_str[0];

		res = PQexec(local_conn, setup_query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			elog(ERROR, "BEGIN or table locking on local failed: %s",
					PQresultErrorMessage(res));

		PQclear(res);

		/*
		 * Check that the local connection supplied is usable, and that the
		 * node identity of the endpoint matches the node we're being called
		 * in.
		 *
		 * This will test the local-only remote_conn if supplied, otherwise the
		 * my-dsn remote_conn. Whichever one we're using for the init process.
		 * (There's no guarantee that my-dsn is even valid from the perspective
		 * of the local node if a local_dsn was also supplied).
		 *
		 * Replication mode isn't tested here. We'll ask the peer to
		 * connect back to us later instead.
		 */
		bdr_get_remote_nodeinfo_internal(local_conn, &li);

		if (!(li.sysid == GetSystemIdentifier()
			&& li.timeline == ThisTimeLineID
			&& li.dboid == MyDatabaseId))
		{
			ereport(ERROR,
					(errmsg("local dsn %s must point to the local node", local_dsn),
					 errdetail("Expected node identity ("UINT64_FORMAT",%u,%u) but got ("UINT64_FORMAT",%u,%u)",
						 GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId,
						 li.sysid, li.timeline, li.dboid)));
		}

		if (!li.is_superuser)
			elog(ERROR, "local connection '%s' must have superuser rights", local_dsn);

		{
			/*
			 * Check for ourselves in local bdr_nodes by UPDATEing our local
			 * bdr_nodes entry. This will get propagated to the remote end later.
			 *
			 * These values could already be set if a prior upgrade attempt failed
			 * after a local commit and before the remote commit.
			 */
			const char *	node_status;
			const char *	bdr_nodes_update_values[5];

			Oid				bdr_nodes_update_types[5] =
				{ TEXTOID, OIDOID, OIDOID, TEXTOID, TEXTOID };


			bdr_nodes_update_values[0] = &local_sysid_str[0];
			bdr_nodes_update_values[1] = &local_timeline_str[0];
			bdr_nodes_update_values[2] = &local_dboid_str[0];

			if (local_dsn != NULL)
				bdr_nodes_update_values[3] = local_dsn;
			else
				bdr_nodes_update_values[3] = NULL;

			if (remote_conninfo != NULL)
				bdr_nodes_update_values[4] = remote_conninfo;
			else
				bdr_nodes_update_values[4] = NULL;

			res = PQexecParams(local_conn,
							   "UPDATE bdr.bdr_nodes "
							   "SET node_local_dsn = $4, "
							   "    node_init_from_dsn = $5 "
							   "WHERE node_sysid = $1 "
							   "  AND node_timeline = $2 "
							   "  AND node_dboid = $3"
							   "RETURNING node_status",
							   5, bdr_nodes_update_types, bdr_nodes_update_values,
							   NULL, NULL, 0);

			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				elog(ERROR, "updating local bdr_nodes failed: state %s: %s\n",
					 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
			}

			if (PQntuples(res) != 1)
			{
				ereport(ERROR,
						(errmsg("no entry for local node found in bdr.bdr_nodes"),
						 errdetail("Expected (node_sysid="UINT64_FORMAT",node_timeline=%u,node_dboid=%u) but no such row found in bdr_nodes",
							 GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId)));
			}

			node_status = PQgetvalue(res, 0, 0);

			if (strcmp(node_status, "r") != 0)
			{
				ereport(ERROR,
						(errmsg("bdr_nodes entry for local node has status != 'r'"),
						 errdetail("Row with (node_sysid="UINT64_FORMAT",node_timeline=%u,node_dboid=%u) but status = '%s' not expected 'r'",
							 GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId, node_status)));
			}

		}

		/*
		 * Another sanity check: Local bdr_connections must be empty.
		 *
		 * If it isn't then a prior upgrade failed after the local commit
		 * but before the remote commit. The local bdr_connections must be
		 * deleted with replication disabled to prevent the deletion
		 * from being enqueued on the outbound slots. This is done
		 * manually by the user per the docs.
		 */
		res = PQexec(local_conn, "SELECT 1 FROM bdr.bdr_connections");

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(ERROR, "querying local bdr_connections failed: state %s: %s\n",
				 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}

		if (PQntuples(res) > 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the local node's bdr.bdr_connections is not empty"),
					 errdetail("No connections from the local node to other nodes may exist when upgrading"),
					 errhint("If a prior upgrade attempt failed see the documentation for recovery steps")));
		}

		PQclear(res);

		/*
		 * BDR requires a security label to be set on the database in order
		 * to start up.
		 */
		res = PQexec(local_conn, "SELECT bdr.internal_update_seclabel()");

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(ERROR, "setting local bdr security label failed: state %s: %s\n",
				 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}

		PQclear(res);


		/*
		 * If this is the first node, insert an entry for ourselves into
		 * the local bdr_connections. We can't insert into the remote and
		 * have it replicate because there is no remote.
		 */
		if (remote_conninfo == NULL)
		{
			bdr_upgrade_to_090_insert_connection(local_conn, local_sysid_str,
					local_timeline_str, local_dboid_str, my_conninfo);
		}

		/*
		 * Establish the connection we'll use to copy the bdr_connections
		 * entries we need and insert our own bdr_connections entry
		 * into the remote end.
		 */
		if (remote_conninfo != NULL)
		{
			StringInfoData	dsn;

			initStringInfo(&dsn);
			appendStringInfo(&dsn,
							"%s fallback_application_name='"BDR_LOCALID_FORMAT":init'",
							remote_conninfo, BDR_LOCALID_FORMAT_ARGS);
			/*
			 * Test to see if there's an entry in the remote's bdr.bdr_nodes for our
			 * system identifier. If there is, that'll tell us what stage of startup
			 * we are up to and let us resume an incomplete start.
			 */
			remote_conn = PQconnectdb(dsn.data);
			if (PQstatus(remote_conn) != CONNECTION_OK)
			{
				ereport(FATAL,
						(errmsg("could not connect to the server in non-replication mode: %s",
								PQerrorMessage(remote_conn)),
						 errdetail("dsn was: %s", dsn.data)));
			}
		}

		PG_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
								PointerGetDatum(&remote_conn));
		{

			char		remote_sysid_str[33];
			char		remote_timeline_str[33];
			char		remote_dboid_str[33];
			const char *remote_nodeid_values[3];

			if (remote_conn != NULL)
			{
				res = PQexec(remote_conn, setup_query);
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
					elog(ERROR, "BEGIN or table locking on remote failed: %s",
							PQresultErrorMessage(res));

				PQclear(res);

				/*
				 * Obtain the remote node's identity so we can look it up in the local
				 * bdr_nodes and see if we recognise this node. This will also ensure
				 * BDR is installed on the remote.
				 */
				bdr_get_remote_nodeinfo_internal(remote_conn, &ri);

				if (ri.sysid == GetSystemIdentifier()
					&& ri.timeline == ThisTimeLineID
					&& ri.dboid == MyDatabaseId)
				{
					bdr_error_nodeids_must_differ(ri.sysid, ri.timeline, ri.dboid);
				}

				if (ri.version_num != BDR_VERSION_NUM)
					elog(ERROR, "remote end must run BDR version %s but is running %s",
						 BDR_VERSION, ri.version);

				if (!ri.is_superuser)
					elog(ERROR, "connection must have superuser rights");

				if (strcmp(ri.variant, "BDR") != 0)
					elog(ERROR, "remote node must be running full BDR, not variant %s",
							ri.variant);

				/*
				 * As a further sanity check, make sure the remote node can connect back
				 * to the local node, and that the resulting IDs match.
				 */
				bdr_test_remote_connectback_internal(remote_conn, &li_via_remote, my_conninfo);

				if (!(li_via_remote.sysid == GetSystemIdentifier()
					&& li_via_remote.timeline == ThisTimeLineID
					&& li_via_remote.dboid == MyDatabaseId))
				{
					ereport(ERROR,
							(errmsg("remote node can connect to dsn %s but it doesn't match the local node identity", my_conninfo),
							 errdetail("Expected node identity ("UINT64_FORMAT",%u,%u) but got ("UINT64_FORMAT",%u,%u)",
								 GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId,
								 li_via_remote.sysid, li_via_remote.timeline, li_via_remote.dboid)));
				}

				if (!li_via_remote.is_superuser)
					elog(ERROR, "connection from remote node to local node using dsn '%s' must have superuser rights", my_conninfo);

				/*
				 * The basics look sane. Check to see if the target node is present
				 * in the local bdr_nodes. If it isn't then we can't join it with
				 * an upgrade, because it's not an existing peer.
				 */

				stringify_node_identity(remote_sysid_str, sizeof(remote_sysid_str),
										remote_timeline_str, sizeof(remote_timeline_str),
										remote_dboid_str, sizeof(remote_dboid_str),
										ri.sysid, ri.timeline, ri.dboid);

				remote_nodeid_values[0] = &remote_sysid_str[0];
				remote_nodeid_values[1] = &remote_timeline_str[0];
				remote_nodeid_values[2] = &remote_dboid_str[0];

				res = PQexecParams(local_conn, bdr_nodes_query, 3, nodeid_types, remote_nodeid_values, NULL, NULL, 0);

				if (PQresultStatus(res) != PGRES_TUPLES_OK)
				{
					elog(ERROR, "Querying local bdr_nodes for remote nodeid failed: state %s: %s\n",
						 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
				}

				if (PQntuples(res) == 0)
				{
					/* Looks like we didn't find the expected node entry */
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("The remote node identified by the passed remote connection string is not known locally"),
							 errdetail("The remote node's identity is ("UINT64_FORMAT",%u,%u) but no entry for the correponding (node_sysid,node_timeline,node_dboid) is present in the local bdr.bdr_nodes",
								 ri.sysid, ri.timeline, ri.dboid),
							 errhint("You can only upgrade a node by connecting to a node it was already joined to before the BDR version update")));
				}

				Assert(PQntuples(res) == 1);

				PQclear(res);

				/*
				 * Now ensure that our node is known to the remote end
				 */
				res = PQexecParams(remote_conn, bdr_nodes_query, 3, nodeid_types,
								   local_nodeid_values, NULL, NULL, 0);

				if (PQresultStatus(res) != PGRES_TUPLES_OK)
				{
					elog(ERROR, "Querying remote bdr_nodes for local nodeid failed: state %s: %s\n",
						 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
				}

				if (PQntuples(res) == 0)
				{
					/*
					 * We're not known to the remote node so we can't do an upgrade
					 * join to it.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("The node identified by the passed connection string does not recognise the local node"),
							 errdetail("The local node's identity is ("UINT64_FORMAT",%u,%u) but no entry for the correponding (node_sysid,node_timeline,node_dboid) is present in the remote bdr.bdr_nodes",
								 GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId),
							 errhint("You can only upgrade a node by connecting to a node it was already joined to before the BDR version update")));
				}

				Assert(PQntuples(res) == 1);

				PQclear(res);

				/*
				 * We now know there's a bdr_nodes entry on each end. Ensure that the
				 * remote end contains at least a bdr_connections entry for its self
				 * and does NOT contain a connection for us.
				 */
				res = PQexec(remote_conn,
							 "SELECT 1 "
							 "FROM bdr.bdr_connections c, "
							 "     bdr.bdr_get_local_nodeid() l "
							 "WHERE c.conn_sysid = l.sysid "
							 "  AND c.conn_timeline = l.timeline "
							 "  AND c.conn_dboid = l.dboid "
							 );

				if (PQresultStatus(res) != PGRES_TUPLES_OK)
				{
					elog(ERROR, "Querying remote bdr_connections failed: state %s: %s\n",
						 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
				}

				if (PQntuples(res) != 1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("The node identified by the passed connection string does not yet have a connection entry for its own node"),
							 errdetail("The remote node's identity is ("UINT64_FORMAT",%u,%u) but no entry for the correponding (conn_sysid,conn_timeline,conn_dboid) is present in the local bdr.bdr_connections",
								 ri.sysid, ri.timeline, ri.dboid),
							 errhint("You must have already upgraded the other node before you can use it to upgrade this node.")));
				}

				PQclear(res);

				res = PQexecParams(remote_conn,
								   "SELECT 1 "
								   "FROM bdr.bdr_connections c "
								   "WHERE c.conn_sysid = $1 "
								   "  AND c.conn_timeline = $2 "
								   "  AND c.conn_dboid = $3 ",
								   3, nodeid_types, local_nodeid_values, NULL, NULL, 0);

				if (PQresultStatus(res) != PGRES_TUPLES_OK)
				{
					elog(ERROR, "Querying remote bdr_connections failed: state %s: %s\n",
						 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
				}

				if (PQntuples(res) != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("The node identified by the passed connection string already has a connection string for the local node"),
							 errdetail("The local node's identity (conn_sysid="UINT64_FORMAT",conn_timeline=%u,conn_dboid=%u) already has an entry in the remote bdr.bdr_connections",
								 li.sysid, li.timeline, ri.dboid),
							 errhint("You must have already upgraded the other node before you can use it to upgrade this node.")));
				}

				PQclear(res);

				/*
				 * Alright, time to actually perform the upgrade.
				 *
				 * We need to:
				 *
				 * - Copy remote bdr_connections entries to the local node
				 *
				 * - Upsert a row for the local node in the remote's
				 *   bdr_connections
				 *
				 * - Register an on commit hook on the remote to rescan
				 *   bdr_connections.
				 *
				 * - Register an on commit hook on the local side to rescan
				 *   bdr_connections
				 *
				 * - set the local security label
				 *
				 * - Commit the remote transaction, adding the bdr_connections
				 *   row
				 *
				 * - Return, allowing a commit to occur to save the local
				 *   bdr_connections entries.
				 */

				bdr_copytable(remote_conn, local_conn,
						"COPY (SELECT * FROM bdr.bdr_connections) TO stdout",
						"COPY bdr.bdr_connections FROM stdin");

				/*
				 * Time to insert connection info about us into the remote node and ask it
				 * to connect back to us, then tell the other nodes. We don't update
				 * the remote's bdr_nodes entry for us, as the change we applied locally
				 * will get replicated.
				 *
				 * Since we have a remote conn we didn't insert our
				 * bdr_connections entry locally above. Insert it into the
				 * remote node now instead. It'll replicate back to the local
				 * node when we connect to the upstream.
				 */
				bdr_upgrade_to_090_insert_connection(remote_conn, local_sysid_str,
						local_timeline_str, local_dboid_str, my_conninfo);

				res = PQexec(remote_conn, "SELECT bdr.bdr_connections_changed()");
				if (PQresultStatus(res) != PGRES_TUPLES_OK)
					elog(ERROR, "SELECT bdr.bdr_connections_changed() on remote failed: %s",
							PQresultErrorMessage(res));

				PQclear(res);

				res = PQexec(remote_conn, "INSERT INTO bdr.bdr_queued_commands\n"
										  "(lsn, queued_at, perpetrator, command_tag, command)\n"
										  "VALUES (pg_current_xlog_insert_location(), current_timestamp,\n"
										  "        current_user, 'SELECT',\n"
										  "       'SELECT bdr.bdr_connections_changed()');");

				if (PQresultStatus(res) != PGRES_COMMAND_OK)
					elog(ERROR, "enqueuing bdr.bdr_connections_changed() in the ddl rep queue failed: %s",
							PQresultErrorMessage(res));
			}

			res = PQexec(local_conn, "SELECT bdr.bdr_connections_changed()");
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
				elog(ERROR, "SELECT bdr.bdr_connections_changed() on local failed: %s",
						PQresultErrorMessage(res));

			PQclear(res);

			res = PQexec(local_conn, "COMMIT");
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
				elog(ERROR, "COMMIT on remote failed: %s",
						PQresultErrorMessage(res));

			PQclear(res);

			if (remote_conn != NULL)
			{
				res = PQexec(remote_conn, "COMMIT");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
					elog(ERROR, "COMMIT on remote failed: %s",
							PQresultErrorMessage(res));

				PQclear(res);

				free_remote_node_info(&ri);
			}

			free_remote_node_info(&li);
		}
		PG_END_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
								PointerGetDatum(&remote_conn));

		PQfinish(remote_conn);

	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&local_conn));

	PQfinish(local_conn);

	PG_RETURN_VOID();
}
