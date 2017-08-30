/*-------------------------------------------------------------------------
 *
 * bdr_join.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_join.c
 *
 * Logic for consistently joining a BDR node to an existing node group
 *-------------------------------------------------------------------------
 *
 * bdr_join manages node parts and joins using a simple state machine for the
 * node's participation and join progress.
 */
#include "postgres.h"

#include "fmgr.h"

#include "utils/builtins.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_messaging.h"
#include "bdr_functions.h"

PGconn *
bdr_join_connect_remote(const char * remote_node_dsn)
{
	char *connkeys[3] = {"dbname", "application_name", NULL};
	char *connvalues[3]
	PGConn *conn;
	char appname[NAMEDATALEN];

	snprintf(appname, NAMEDATALEN, "bdr join: %s", my_node_name);
	appname[NAMEDATALEN-1] = '\0';

	connvalues[0] = remote_node_dsn;
	connvalues[1] = appname;
	connvalues[2] = NULL;

	conn = PQconnectdbParams(connkeys, connvalues, true);
	if (PQstatus(conn) != CONNECTION_OK)
		ereport(ERROR,
				(errmsg("failed to connect to remote BDR node: %s", PQerrorMessage(conn))));

	return conn;
}

/*
 * Make a normal libpq connection to the remote node and submit a node-join
 * request.
 *
 * TODO: Make this async so we can do it in the manager event loop
 *
 * TODO: Make this return the message handle so we can push it as state
 * 		 extradata when we issue a state transition, and use it to check
 * 		 join progress. That way we know it's us that joined successfully
 * 		 not someone else when we poll for success.
 */
void
bdr_join_submit_request(PGconn *conn, const char * node_group_name,
						const char *my_node_name)
{
	Oid paramTypes[2] = {TEXTOID, TEXTOID};
	char *paramValues[2];

	paramValues[0] = node_group_name;
	paramValues[1] = my_node_name;
	res = PQexecParams(conn, "SELECT bdr.internal_submit_join_request($1, $2)",
					   2, paramTypes, paramValues, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to submit join request on join target: %s", PQerrorMessage(conn))));
	}

	PQclear(res);
}

/*
 * Clone a remote BDR node's nodegroup to the local node and make the local node
 * a member of it. Create the default repset for the nodegroup in the process.
 *
 * Returns the created nodegroup id.
 */
Oid
bdr_join_copy_remote_nodegroup(PGconn *conn, const char *node_group_name, BdrNodeInfo *local)
{
	BdrNodeGroup		nodegroup;
	BdrNodeInfo		   *info;
	PGLogicalRepSet		repset;
	PGresult			res;
	const char		   *remote_group_name;
	Oid					remote_group_id;

	/*
	 * Look up the local node on the remote so we can get the info
	 * we need about the nodegroup.
	 */
	/*
	 * TODO: hide behind info func to ease future catalog changes?
	 * Right now assumes there's only one nodegroup, no nodegroups can exist in catalog that aren't associated with local node, etc....
	 */
	res = PQexec(conn, "SELECT node_group_id, node_group_name FROM bdr.node_group");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to get remote nodegroup information: %s", PQerrorMessage(conn))));
	}

	if (PQntuples(res) != 1)
	{
		int ntuples = PQntuples(res);
		PQclear(res);
		ereport(ERROR,
				(errmsg("expected only one nodegroup on remote, found %d", ntuples));
	}

	remote_group_name = PQgetvalue(res, 0, 0);

	if (node_group_name != NULL && strcmp(remote_group_name, node_group_name) == 0)
	{
		elog(ERROR, "remote node is member of nodegroup %s but we asked to join nodegroup %s",
			 remote_group_name, node_group_name);
	}

	if (sscanf(PQgetvalue(res, 0, 1), "%u", &remote_group_id) != 1)
		elog(ERROR, "could not parse remote nodegroup id");

	if (remote_group_id == 0)
		elog(ERROR, "invalid remote nodegroup id 0");

	/*
	 * Create local nodegroup with the same ID, a matching replication set, and
	 * bind our node to it. Very similar to what happens in
	 * bdr_create_nodegroup_sql().
	 */
	repset.id = InvalidOid;
	repset.nodeid = local->bdr_node->node_id;
	repset.name = remote_group_name;
	repset.replicate_insert = true;
	repset.replicate_update = true;
	repset.replicate_delete = true;
	repset.replicate_truncate = true;
	repset.isinternal = true;

	nodegroup.id = remote_group_id;
	nodegroup.name = remote_group_name;
	nodegroup.default_repset = create_replication_set(&repset);
	if (bdr_nodegroup_create(&nodegroup) != remote_group_id)
	{
		/* shouldn't happen */
		elog(ERROR, "failed to create nodegroup with id %u");
	}

	/* Assign the nodegroup to the local node */
	Assert(local->bdr_node_group == NULL);
	info->bdr_node->node_group_id = nodegroup.id;
	bdr_modify_node(info->bdr_node);

	return nodegroup.id;
}

/*
 * Copy all BDR node catalog entries that are members of the identified
 * nodegroup to the local node.
 */
void
bdr_join_copy_remote_nodes(PGconn *conn, Oid nodegroup_id)
{
	Oid paramTypes[1] = {OIDOID}
	char *paramValues[1];
	char nodeid[30];
	int i;

	snprintf(nodeid, 30, "%u", node_group_id);
	paramValues[0] = nodeid;
	res = PQexecParams(conn, "SELECT pglogical_node_id, local_state, seq_id FROM bdr.node WHERE node_group_id = $1",
					   1, paramTypes, paramValues, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to get remote node list: %s", PQerrorMessage(conn))));
	}

	if (PQntuples(res) == 0)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to get remote node list: no remote nodes found with nodegroup id %u", nodegroup_id)));
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		BdrNode node;

		node.node_id = /* copy */
		node.node_group_id = nodegroup_id;
		node.local_state = /* copy */
		node.seq_id = /* copy */
		node.confirmed_our_join = false;

		/* create */
		XXXX
	}
}

void do_the_join()
{
	/*
	 * Look up current join progress
	 */

	/*
	 * Connect to join target and validate the connection.
	 *
	 * TODO: should call an information func on remote end to collect
	 * extra data like BDR version too
	 */
	BdrNodeInfo *remoteinfo = NULL;
	/*
	remoteinfo = bdr_get_remote_node_info(join_target_dsn);
	*/

	if (remoteinfo->bdr_node == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no BDR node exists on join target")));

	if (remoteinfo->bdr_node_group == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("BDR node on join target has no node group, cannot join")));

	if (remoteinfo->pgl_node == NULL || remoteinfo->pgl_interface == NULL)
	{
		/* Shouldn't happen */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("remote BDR node has no pglogical node")));
	}

	if (strcmp(remoteinfo->pgl_node->name, localinfo->pgl_node->name) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("local node and remote node have the same node name"),
				 errhint("Make sure you didn't specify the local node's connection string as the join target")));

	/*
	 * TODO: further checking on version support flags, etc fetched from the remote
	 * node.
	 */

	/*
	 * Ask the join target to add our nodes entry on all peers, ensuring our
	 * node name is unique. Submit a random cookie with the request so we can
	 * tell if it was our request that won in case of multiple requests for the
	 * same name by concurrently joining nodes.
	 */

	/* Monitor remote's consensus journal until we see our
	 * request confirmed. (We can't just watch its bdr.node table since we need
	 * to know it was our request that "won" the name, which we can tell using
	 * the cookie submitted along with the join message).
	 */

	/*
	 * Update local node status to 'name reserved'
	 */

	/*
	 * Create subscription to target node
	 */

}
