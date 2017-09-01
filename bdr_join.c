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

#include "catalog/pg_type.h"

#include "fmgr.h"

#include "libpq-fe.h"

#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"

#include "bdr_catcache.h"
#include "bdr_messaging.h"
#include "bdr_msgformats.h"
#include "bdr_functions.h"
#include "bdr_join.h"
#include "bdr_worker.h"

PGconn *
bdr_join_connect_remote(const char * remote_node_dsn,
	BdrNodeInfo *local)
{
	const char *connkeys[3] = {"dbname", "application_name", NULL};
	const char *connvalues[3];
	PGconn *conn;
	char appname[NAMEDATALEN];

	snprintf(appname, NAMEDATALEN, "bdr join: %s",
		local->pgl_node->name);
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

void
bdr_finish_connect_remote(PGconn *conn)
{
	PQfinish(conn);
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
uint64
bdr_join_submit_request(PGconn *conn, const char * node_group_name,
						BdrNodeInfo *local)
{
	Oid paramTypes[7] = {TEXTOID, TEXTOID, OIDOID, INT4OID, TEXTOID, OIDOID, TEXTOID};
	const char *paramValues[7];
	const char *val;
	char my_node_id[MAX_DIGITS_INT32];
	char my_node_initial_state[MAX_DIGITS_INT32];
	char my_node_if_id[MAX_DIGITS_INT32];
	uint64 consensus_msg_handle;
	PGresult *res;

	Assert(local->pgl_interface != NULL);

	paramValues[0] = node_group_name;
	paramValues[1] = local->pgl_node->name;
	snprintf(my_node_id, MAX_DIGITS_INT32, "%u",
		local->bdr_node->node_id);
	paramValues[2] = my_node_id;
	snprintf(my_node_initial_state, MAX_DIGITS_INT32, "%d",
		local->bdr_node->local_state);
	paramValues[3] = my_node_initial_state;
	paramValues[4] = local->pgl_interface->name;
	snprintf(my_node_if_id, MAX_DIGITS_INT32, "%u",
		local->pgl_interface->id);
	paramValues[5] = my_node_if_id;
	paramValues[6] = local->pgl_interface->dsn;
	res = PQexecParams(conn, "SELECT bdr.internal_submit_join_request($1, $2, $3, $4, $5, $6, $7)",
					   7, paramTypes, paramValues, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to submit join request on join target: %s", PQerrorMessage(conn))));
	}

	val = PQgetvalue(res, 0, 0);
	if (sscanf(val, UINT64_FORMAT, &consensus_msg_handle) != 1)
		elog(ERROR, "could not parse consensus message handle from remote");

	PQclear(res);

	return consensus_msg_handle;
}

/*
 * Handle a remote request to join by creating the remote node entry.
 *
 * At this point the proposal isn't committed yet, and we're in a
 * consensus manager transaction that'll prepare it if we succeed.
 */
void
bdr_join_handle_join_proposal(BdrMessage *msg)
{
	BdrMsgJoinRequest req;
	StringInfoData si;
	BdrNodeGroup *local_nodegroup;
	BdrNode bnode;
	PGLogicalNode pnode;
	PGlogicalInterface pnodeif;

	Assert(is_bdr_manager());

	wrapInStringInfo(&si, msg->payload, msg->payload_length);
	msg_deserialize_join_request(&si, &req);

	local_nodegroup = bdr_get_nodegroup_by_name(req.nodegroup_name, false);
	if (req.nodegroup_id != 0 && local_nodegroup->id != req.nodegroup_id)
		elog(ERROR, "expected nodegroup %s to have id %u but local nodegroup id is %u",
			 req.nodegroup_name, req.nodegroup_id, local_nodegroup->id);

	if (req.joining_node_id == 0)
		elog(ERROR, "joining node id must be nonzero");

	pnode.id = req.joining_node_id;
	pnode.name = (char*)req.joining_node_name;

	bnode.node_id = req.joining_node_id;
	bnode.node_group_id = local_nodegroup->id;
	bnode.local_state = req.joining_node_state;
	bnode.seq_id = 0;

	pnodeif.id = req.joining_node_if_id;
	pnodeif.name = req.joining_node_if_name;
	pnodeif.nodeid = pnode.id;
	pnodeif.dsn = req.joining_node_if_dsn;

	/*
	 * TODO: should treat node as join-confirmed if we're an active
	 * node ourselves.
	 */
	bnode.confirmed_our_join = false;
	/* Ignoring join_target_node_name and join_target_node_id for now */

	create_node(&pnode);
	create_node_interface(&pnodeif);
	bdr_node_create(&bnode);
}

/*
 * Probe a remote node to get BdrNodeInfo for the node.
 */
BdrNodeInfo *
get_remote_node_info(PGconn *conn)
{
	BdrNodeInfo    *remote;
	char		   *val;
	PGresult	   *res;

	res = PQexec(conn, "SELECT node_id, node_name, node_local_state, node_seq_id, nodegroup_id, nodegroup_name FROM bdr.local_node_info()");
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
				(errmsg("expected only one row from bdr.local_node_info(), got %d", ntuples)));
	}

	remote = palloc(sizeof(BdrNodeInfo));
	remote->bdr_node = palloc(sizeof(BdrNode));
	remote->bdr_node_group = palloc(sizeof(BdrNodeGroup));
	remote->pgl_node = palloc(sizeof(PGLogicalNode));
	remote->pgl_interface = NULL;

	val = PQgetvalue(res, 0, 0);
	if (sscanf(val, "%u", &remote->bdr_node->node_id) != 1)
		elog(ERROR, "could not parse remote node id");

	remote->pgl_node->id = remote->bdr_node->node_id;

	remote->pgl_node->name = pstrdup(PQgetvalue(res, 0, 1));

	val = PQgetvalue(res, 0, 2);
	if (sscanf(val, "%u", &remote->bdr_node->local_state) != 1)
		elog(ERROR, "could not parse remote node state");

	val = PQgetvalue(res, 0, 3);
	if (sscanf(val, "%d", &remote->bdr_node->seq_id) != 1)
		elog(ERROR, "could not parse remote node sequence id");

	val = PQgetvalue(res, 0, 4);
	if (sscanf(val, "%u", &remote->bdr_node_group->id) != 1)
		elog(ERROR, "could not parse remote nodegroup id");

	remote->bdr_node_group->name = pstrdup(PQgetvalue(res, 0, 5));

	return remote;
}

/*
 * Clone a remote BDR node's nodegroup to the local node and make the local node
 * a member of it. Create the default repset for the nodegroup in the process.
 *
 * Returns the created nodegroup id.
 */
void
bdr_join_copy_remote_nodegroup(BdrNodeInfo *local, BdrNodeInfo *remote)
{
	BdrNodeGroup	   *newgroup = palloc(sizeof(BdrNodeGroup));
	PGLogicalRepSet		repset;

	/*
	 * Look up the local node on the remote so we can get the info
	 * we need about the newgroup.
	 */
	Assert(local->bdr_node_group == NULL);

	/*
	 * Create local newgroup with the same ID, a matching replication set, and
	 * bind our node to it. Very similar to what happens in
	 * bdr_create_newgroup_sql().
	 */
	repset.id = InvalidOid;
	repset.nodeid = local->bdr_node->node_id;
	repset.name = (char*)remote->bdr_node_group->name;
	repset.replicate_insert = true;
	repset.replicate_update = true;
	repset.replicate_delete = true;
	repset.replicate_truncate = true;
	repset.isinternal = true;

	newgroup->id = remote->bdr_node_group->id;
	newgroup->name = remote->bdr_node_group->name;
	newgroup->default_repset = create_replication_set(&repset);
	if (bdr_nodegroup_create(newgroup) != remote->bdr_node_group->id)
	{
		/* shouldn't happen */
		elog(ERROR, "failed to create newgroup with id %u",
			 remote->bdr_node_group->id);
	}

	/* Assign the newgroup to the local node */
	local->bdr_node->node_group_id = newgroup->id;
	bdr_modify_node(local->bdr_node);

	local->bdr_node_group = newgroup;
}

/*
 * Copy all BDR node catalog entries that are members of the identified
 * nodegroup to the local node.
 */
void
bdr_join_copy_remote_nodes(PGconn *conn, BdrNodeInfo *local)
{
	Oid			paramTypes[1] = {OIDOID};
	const char *paramValues[1];
	char		nodeid[MAX_DIGITS_INT32];
	int			i;
	PGresult   *res;
	Oid			node_group_id = local->bdr_node_group->id;

	/*
	 * We use a helper function on the other end to collect the info, shielding
	 * us somewhat from catalog changes and letting us fetch the pgl and bdr info
	 * all at once.
	 */
	snprintf(nodeid, 30, "%u", node_group_id);
	paramValues[0] = nodeid;
	res = PQexecParams(conn, "SELECT node_id, node_name, bdr_local_state, bdr_seq_id, node_if_id, node_if_name, node_if_dsn FROM bdr.node_group_member_info($1)",
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
				(errmsg("failed to get remote node list: no remote nodes found with nodegroup id %u", node_group_id)));
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		BdrNode bnode;
		PGLogicalNode pnode;
		PGlogicalInterface pinterface;
		const char *val;

		val = PQgetvalue(res, i, 0);
		if (sscanf(val, "%u", &bnode.node_id) != 1)
			elog(ERROR, "cannot parse '%s' as uint32", val);

		pnode.id = bnode.node_id;

		pnode.name = pstrdup(PQgetvalue(res, i, 1));

		val = PQgetvalue(res, i, 2);
		if (sscanf(val, "%u", &bnode.local_state) != 1)
			elog(ERROR, "cannot parse '%s' as uint32", val);

		val = PQgetvalue(res, i, 3);
		if (sscanf(val, "%d", &bnode.seq_id) != 1)
			elog(ERROR, "cannot parse '%s' as int", val);

		bnode.node_group_id = node_group_id;
		bnode.confirmed_our_join = false;

		val = PQgetvalue(res, i, 4);
		if (sscanf(val, "%u", &pinterface.id) != 1)
			elog(ERROR, "cannot parse '%s' as uint32", val);

		pinterface.name = PQgetvalue(res, i, 5);
		pinterface.dsn = PQgetvalue(res, i, 6);
		pinterface.nodeid = bnode.node_id;

		/* create */
		bdr_node_create(&bnode);
		create_node(&pnode);
		create_node_interface(&pinterface);
	}
}

XLogRecPtr
bdr_get_remote_insert_lsn(PGconn *conn)
{
	PGresult   *res;
	const char *val;
	XLogRecPtr	min_lsn;

	res = PQexec(conn, "SELECT pg_current_wal_insert_lsn()");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to get remote insert lsn: %s", PQerrorMessage(conn))));
	}

	Assert(PQntuples(res) == 1);

	val = PQgetvalue(res, 0, 0);
	min_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(val)));

	PQclear(res);
	return min_lsn;
}

/*
 * Create a subscription to the join target node, so we can dump its
 * data/schema.
 *
 * TODO: start paused, or pause after dump+restore completes
 */
void
bdr_join_subscribe_join_target(PGconn *conn, BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_copy_repset_memberships(PGconn *conn, BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_init_consensus_messages(PGconn *conn, BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_create_origins(BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_create_subscriptions(BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_send_catchup_announce(BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_create_slots(BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_send_active_announce(BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}

void
bdr_join_go_active(BdrNodeInfo *local)
{
	elog(WARNING, "not implemented");
}
