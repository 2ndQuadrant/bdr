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

#include "commands/dbcommands.h"

#include "fmgr.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "replication/origin.h"
#include "replication/slot.h"

#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"
#include "pglogical_sync.h"
#include "pglogical_worker.h"

#include "bdr_catcache.h"
#include "bdr_messaging.h"
#include "bdr_msgformats.h"
#include "bdr_functions.h"
#include "bdr_join.h"
#include "bdr_worker.h"

static void bdr_join_create_slot(BdrNodeInfo *local, BdrNodeInfo *remote);
static void bdr_create_subscription(BdrNodeInfo *local, BdrNodeInfo *remote, int apply_delay_ms, bool for_join);

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
	BdrMsgJoinRequest *req = msg->message;
	BdrNodeGroup *local_nodegroup;
	BdrNode bnode;
	PGLogicalNode pnode;
	PGlogicalInterface pnodeif;

	Assert(is_bdr_manager());

	elog(LOG, "XXX processing join request for %u", req->joining_node_id);

	local_nodegroup = bdr_get_nodegroup_by_name(req->nodegroup_name, false);
	if (req->nodegroup_id != 0 && local_nodegroup->id != req->nodegroup_id)
		elog(ERROR, "expected nodegroup %s to have id %u but local nodegroup id is %u",
			 req->nodegroup_name, req->nodegroup_id, local_nodegroup->id);

	if (req->joining_node_id == 0)
		elog(ERROR, "joining node id must be nonzero");

	if (req->joining_node_id == bdr_get_local_nodeid())
	{
		/*
		 * Join requests are not received by the node being joined, because
		 * it's not yet part of the consensus system. So this shouldn't happen.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("node %u received its own join request", req->joining_node_id)));
	}

	elog(LOG, "XXX forming node tuples for join request");

	pnode.id = req->joining_node_id;
	pnode.name = (char*)req->joining_node_name;

	bnode.node_id = req->joining_node_id;
	bnode.node_group_id = local_nodegroup->id;
	/*
	 * TODO: should set local state to joining
	 */
	bnode.local_state = req->joining_node_state;
	bnode.seq_id = 0;

	pnodeif.id = req->joining_node_if_id;
	pnodeif.name = req->joining_node_if_name;
	pnodeif.nodeid = pnode.id;
	pnodeif.dsn = req->joining_node_if_dsn;

	/*
	 * TODO: should treat node as join-confirmed if we're an active
	 * node ourselves.
	 */
	bnode.confirmed_our_join = false;
	/* Ignoring join_target_node_name and join_target_node_id for now */

	/* TODO: do upserts here in case pgl node or even bdr node exists already */
	elog(LOG, "XXX creating node for join request");
	create_node(&pnode);
	elog(LOG, "XXX creating node interface for join request");
	create_node_interface(&pnodeif);
	elog(LOG, "XXX creating bdr node for join request");
	bdr_node_create(&bnode);

	/*
	 * We don't subscribe to the node yet, that only happens once it goes
	 * active.
	 */

	/*
	 * TODO: move addition of the peer node from prepare
	 * phase to accept phase callback
	 */
	elog(LOG, "XXX adding messaging peer for node join request");
	bdr_messaging_add_peer(bnode.node_id, pnodeif.dsn);
	elog(LOG, "XXX added messaging peer for node join request");

	/*
	 * TODO FIXME XXX: ugly, ugly hack here. Because of races during join, we can't
	 * reliably expect to receive catchup-ready or node-active messages from a
	 * joining peer yet. So we act as if they came along with the join request.
	 * They only need the origin-id from messages so far so we can just pass
	 * our message to them (ew).
	 *
	 * The need for this will go away when we can properly refresh the manager's
	 * peer list and join is moved into the manager.
	 */
	elog(LOG, "XXX faking catchup proposal for node join request");
	/*
	 * TODO XXX FIXME Lie and pretend the message came from the joining node
	 * like it really will later, once we have proper join.
	 */
	msg->originator_id = req->joining_node_id;
	bdr_join_handle_catchup_proposal(msg);
	elog(LOG, "XXX faking active proposal for node join request");
	bdr_join_handle_active_proposal(msg);
	elog(LOG, "XXX finished processing for node join request");
}

uint64
bdr_join_send_catchup_ready(BdrNodeInfo *local)
{
	/*
	 * TODO XXX disabled, because we fake this up on receiver
	 * side for now, see bdr_join_handle_join_proposal
	 *
	return bdr_msgs_enqueue_one(BDR_MSG_NODE_CATCHUP_READY, NULL);
	*/
	return 1;
}

/*
 * A peer node says it wants to go into catchup mode.
 *
 * (This won't really require consensus, but it's simplest to treat it as if it does)
 *
 * At this point we need to create slots for the peer to use. The peer won't
 * be replaying data from them yet, but it should connect and advance them
 * so we don't retain excess resources. (TODO)
 *
 * TODO: should create an ephemeral slot here, and make it permanent on commit?
 */
void
bdr_join_handle_catchup_proposal(BdrMessage *msg)
{
	BdrNodeInfo		   *local, *remote;

	Assert(is_bdr_manager());

	/*
	 * Catchup ready announcements are empty, with no payload,
	 * but we might want to add one later, so we don't check.
	 */

	local = bdr_get_local_node_info(false, false);
	remote = bdr_get_node_info(msg->originator_id, false);

	if (local->bdr_node->node_id != remote->bdr_node->node_id)
	{
		bdr_join_create_slot(local, remote);

		/*
		 * TODO: should set local state to catchup
		 */

		/*
		 * TODO: write a catchup-confirmation message
		 * here, so the peer can tally join confirmations
		 */
	}
}

uint64
bdr_join_send_active_announce(BdrNodeInfo *local)
{
	return 1;
	/*
	 * TODO XXX disabled, because we fake this up on receiver
	 * side for now, see bdr_join_handle_join_proposal
	 *
	return bdr_msgs_enqueue_one(BDR_MSG_NODE_ACTIVE, NULL);
	*/
}

/*
 * A node in catchup mode announces that it wants to join as a full peer.
 */
void
bdr_join_handle_active_proposal(BdrMessage *msg)
{
	BdrNodeInfo		   *local, *remote;

	Assert(is_bdr_manager());

	/*
	 * Catchup ready announcements are empty, with no payload,
	 * but we might want to add one later, so we don't check.
	 */

	local = bdr_get_local_node_info(false, false);
	remote = bdr_get_node_info(msg->originator_id, false);

	if (local->bdr_node->node_id != remote->bdr_node->node_id)
	{
		/*
		 * We can now create a subscription to the node, to be started
		 * once we commit.
		 */
		bdr_create_subscription(local, remote, 0, false);
	}

	/*
	 * TODO: should set local state to active/ready
	 */

	/*
	 * TODO: should signal the manager to start the subscription
	 * once we commit
	 */
}

/*
 * Mirror of make_nodeinfo_result
 */
static BdrNodeInfo*
read_nodeinfo_result(PGresult *res, int rownum)
{
	BdrNodeInfo *info;
	char *val;

	info = palloc(sizeof(BdrNodeInfo));
	info->bdr_node = palloc0(sizeof(BdrNode));
	info->bdr_node_group = palloc0(sizeof(BdrNodeGroup));
	info->pgl_node = palloc0(sizeof(PGLogicalNode));
	info->pgl_interface = palloc0(sizeof(PGlogicalInterface));

	/*
	 * TODO: use SELECT *, and gracefully ignore missing fields
	 */
	if (PQnfields(res) < 10)
		elog(ERROR, "expected at least 10 fields, peer BDR too old?");

	if (rownum + 1 > PQntuples(res))
		elog(ERROR, "attempt to read row %d but only %d rows in output",
			 rownum, PQntuples(res));

	val = PQgetvalue(res, rownum, 0);
	if (sscanf(val, "%u", &info->bdr_node->node_id) != 1)
		elog(ERROR, "could not parse info node id");

	info->pgl_node->id = info->bdr_node->node_id;
	info->pgl_interface->nodeid = info->bdr_node->node_id;

	info->pgl_node->name = pstrdup(PQgetvalue(res, rownum, 1));

	val = PQgetvalue(res, rownum, 2);
	if (sscanf(val, "%u", &info->bdr_node->local_state) != 1)
		elog(ERROR, "could not parse info node state");

	val = PQgetvalue(res, rownum, 3);
	if (sscanf(val, "%d", &info->bdr_node->seq_id) != 1)
		elog(ERROR, "could not parse info node sequence id");

	info->bdr_node->confirmed_our_join = false;

	val = PQgetvalue(res, rownum, 4);
	if (sscanf(val, "%u", &info->bdr_node_group->id) != 1)
		elog(ERROR, "could not parse info nodegroup id");
	info->bdr_node->node_group_id = info->bdr_node_group->id;

	info->bdr_node_group->name = pstrdup(PQgetvalue(res, rownum, 5));

	val = PQgetvalue(res, rownum, 6);
	if (sscanf(val, "%u", &info->pgl_interface->id) != 1)
		elog(ERROR, "could not parse pglogical interface id");

	info->pgl_interface->name = pstrdup(PQgetvalue(res, rownum, 7));

	info->pgl_interface->dsn = pstrdup(PQgetvalue(res, rownum, 8));

	info->bdr_node->dbname = pstrdup(PQgetvalue(res, rownum, 9));

	check_nodeinfo(info);
	return info;
}

#define NODEINFO_FIELD_NAMES 

/*
 * Probe a remote node to get BdrNodeInfo for the node.
 */
BdrNodeInfo *
get_remote_node_info(PGconn *conn)
{
	BdrNodeInfo    *remote;
	PGresult	   *res;

	res = PQexec(conn, "SELECT node_id, node_name, node_local_state, node_seq_id, nodegroup_id, nodegroup_name, pgl_interface_id, pgl_interface_name, pgl_interface_dsn, bdr_dbname FROM bdr.local_node_info()");
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

	remote = read_nodeinfo_result(res, 0);

	PQclear(res);

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
	res = PQexecParams(conn, "SELECT node_id, node_name, node_local_state, node_seq_id, nodegroup_id, nodegroup_name, pgl_interface_id, pgl_interface_name, pgl_interface_dsn, bdr_dbname FROM bdr.node_group_member_info($1)",
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
		BdrNodeInfo *peer = read_nodeinfo_result(res, i);
		Assert(peer->bdr_node_group->id == local->bdr_node_group->id);

		if (peer->bdr_node->node_id == local->bdr_node->node_id)
			continue;

		/*
		 * TODO: upsert here, in case we already have the pgl node
		 * from some prior subscription
		 */
		create_node(peer->pgl_node);
		create_node_interface(peer->pgl_interface);
		bdr_node_create(peer->bdr_node);
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

static char*
bdr_gen_sub_name(BdrNodeInfo *subscriber, BdrNodeInfo *provider)
{
	StringInfoData	sub_name;

	Assert(provider->bdr_node_group != NULL);
	Assert(subscriber->bdr_node_group != NULL);
	Assert(provider->bdr_node_group->id == subscriber->bdr_node_group->id);
	Assert(provider->bdr_node->node_id != subscriber->bdr_node->node_id);

	initStringInfo(&sub_name);
	/*
	 * Annoyingly, sub names must be unique across all providers on a
	 * subscriber so we have to qualify the sub name by the provider name.
	 *
	 * This is redundant with the provider name in the slot created on the peer
	 * end, since it also has the subscriber name in it, but not much to be
	 * done about it.
	 */
	appendStringInfo(&sub_name, "%s_%s",
		provider->bdr_node_group->name,
		provider->pgl_node->name);
	return sub_name.data;
}

/*
 * Create a subscription, initially disabled, from 'local' to 'remote, possibly
 * dumping data too.
 */
static void
bdr_create_subscription(BdrNodeInfo *local, BdrNodeInfo *remote, int apply_delay_ms, bool for_join)
{
	List				   *replication_sets = NIL;
	NameData				slot_name;
	char				   *sub_name;
	PGLogicalSubscription	sub;
	PGLSubscriptionWriter	sub_writer;
	PGLogicalSyncStatus		sync;
	Interval				apply_delay;

	elog(bdr_debug_level, "creating subscription for %u on %u",
		 remote->bdr_node->node_id, local->bdr_node->node_id);

	/*
	 * For now we support only one replication set, with the same name as the
	 * BDR group. (DDL should be done through it too).
	 */
	replication_sets = lappend(replication_sets, pstrdup(local->bdr_node_group->name));

	/*
	 * Make sure there's no existing BDR subscription to this node.
	 */
	check_overlapping_replication_sets(replication_sets, 
		remote->pgl_node->id, remote->pgl_node->name);

	sub_name = bdr_gen_sub_name(local, remote);

	check_nodeinfo(local);
	check_nodeinfo(remote);

	if (local->bdr_node->node_id == remote->bdr_node->node_id)
	{
		Assert(false);
		elog(ERROR, "attempt to subscribe to own node");
	}

	/*
	 * Create the subscription using the remote node and interface
	 * we copied earlier.
	 */
	sub.id = InvalidOid;
	sub.name = sub_name;
	sub.origin_if = remote->pgl_interface;
	sub.target_if = local->pgl_interface;
	sub.replication_sets = replication_sets;
	/*
	 * BDR handles forwarding separately in the output plugin hooks
	 * so it can forward by nodegroup, not origin list.
	 */
	sub.forward_origins = NIL;
	/*
	 * Only the join target sub starts enabled. The others get enabled
	 * later.
	 *
	 * TODO: once we have catchup mode subs that just forward, we'll
	 * want to enable all subs initially
	 */
	sub.enabled = for_join;
	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  remote->pgl_node->name, sub_name);
	sub.slot_name = pstrdup(NameStr(slot_name));

	interval_from_ms(apply_delay_ms, &apply_delay);
	sub.apply_delay = &apply_delay;

	sub.isinternal = true;

	create_subscription(&sub);

	/*
	 * Create the writer for the subscription.
	 */
	sub_writer.id = InvalidOid;
	sub_writer.sub_id = sub.id;
	sub_writer.name = sub_name;
	sub_writer.writer = "HeapWriter";
	sub_writer.options = NIL;

	pgl_create_subscription_writer(&sub_writer);

	/*
	 * Prepare initial sync. BDR will only ever do two kinds - a full dump,
	 * on the join target, or no sync for other nodes.
	 */
	if (for_join)
		sync.kind = SYNC_KIND_FULL;
	else
		sync.kind = SYNC_KIND_INIT;

	sync.subid = sub.id;
	sync.nspname = NULL;
	sync.relname = NULL;
	sync.status = SYNC_STATUS_INIT;
	create_local_sync_status(&sync);

	/* Create the replication origin */
	(void) replorigin_create(sub.slot_name);

	pglogical_subscription_changed(sub.id);

	/*
	 * TODO: create bdr.subscriptions entry for this sub
	 */
	elog(bdr_debug_level, "created subscription for %u on %u",
		 remote->bdr_node->node_id, local->bdr_node->node_id);
}

/*
 * Create a subscription to the join target node, so we can dump its
 * data/schema.
 *
 * TODO: start paused, or pause after dump+restore completes
 */
void
bdr_join_subscribe_join_target(PGconn *conn, BdrNodeInfo *local, BdrNodeInfo *remote)
{
	/*
	 * Create the subscription, writer, and sync state catalog entries.
	 */
	bdr_create_subscription(local, remote, 0, true);
}

void
bdr_join_copy_repset_memberships(PGconn *conn, BdrNodeInfo *local)
{
	elog(WARNING, "replication set memberships copy not implemented");
}

void
bdr_join_init_consensus_messages(PGconn *conn, BdrNodeInfo *local)
{
	elog(WARNING, "bringing up consensus manager not implemented");
}

void
bdr_join_create_subscriptions(BdrNodeInfo *local, BdrNodeInfo *join_target)
{
	List	   *nodes;
	ListCell   *lc;

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);
	
	foreach (lc, nodes)
	{
		BdrNodeInfo *remote = lfirst(lc);

		if (remote->bdr_node->node_id == join_target->bdr_node->node_id)
			continue;

		if (remote->bdr_node->node_id == local->bdr_node->node_id)
			continue;

		bdr_create_subscription(local, remote, 0, false);
	}
}

/*
 *
 * Pg's replication slots code lacks any interface to check if a slot exists.
 * You can acquire it, but either it'll block if in use, or with nowait mode,
 * it'll ERROR. So there's no nice way to ask "does this slot exist?". And
 * there's no way to create one if it doesn't exist.
 *
 * So we must do the test ourselves. This is racey with a concurrent slot
 * creation, but ... "don't do that". If someone does create a conflicting
 * slot we'll error out when we try to create, and re-check when we run again.
 *
 * Returns true if the slot exists and is a pglogical slot. false if no such
 * slot exists. ERROR's if the slot exists but isn't a pglogical slot.
 */
static bool
bdr_replication_slot_exists(Name slot_name)
{
	ReplicationSlot    *found = NULL;
	int					i;

	LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && strcmp(NameStr(*slot_name), NameStr(s->data.name)) == 0)
		{
			found = s;
			break;
		}
	}

	if (found && found->data.database != MyDatabaseId)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("slot %s exists but is not a logical slot for the current database",
					 NameStr(*slot_name)),
				 errdetail("Expected slot for %u, found slot for %u",
				 	 MyDatabaseId, found->data.database)));

	if (found && found->data.persistency != RS_PERSISTENT)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("slot %s exists but is not a persistent slot",
					 NameStr(*slot_name))));

	if (found && strcmp(NameStr(found->data.plugin), "pglogical") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("slot %s exists but uses plugin '%s' not expected 'pglogical'",
					 NameStr(*slot_name), NameStr(found->data.plugin))));

	LWLockRelease(ReplicationSlotAllocationLock);

	return found != NULL;
}

static void
bdr_join_create_slot(BdrNodeInfo *local, BdrNodeInfo *remote)
{
	char		*sub_name;
	NameData	slot_name;

	/*
	 * Subscription names here are from the PoV of the remote
	 * node, since this creation is happening on the provider.
	 */
	sub_name = bdr_gen_sub_name(remote, local);

	gen_slot_name(&slot_name, remote->bdr_node->dbname,
				  local->pgl_node->name, sub_name);

	/*
	 * Slot creation is NOT transactional. If we're being asked to create a
	 * slot for peers we could've failed after some slots were created, so
	 * we can't assume a clean slate here.
	 *
	 * An already-existing pglogical slot for this db with the right name
	 * is fine to use, since it must be at or behind the position a new
	 * slot would get created at.
	 *
	 * We don't hold a lock over these two, so someone could create the
	 * slot after we check it, but then we'll just ERROR in creation and
	 * retry.
	 */
	if (bdr_replication_slot_exists(&slot_name))
	{
		elog(LOG, "XXX slot %s already exists", NameStr(slot_name));
		return;
	}

	elog(LOG, "XXX slot %s does not exist", NameStr(slot_name));
	ReplicationSlotCreate(NameStr(slot_name), true, RS_PERSISTENT);
	elog(LOG, "XXX slot %s created", NameStr(slot_name));
	ReplicationSlotRelease();
}

/*
 * Unlike for pglogical, BDR creates replication slots for its peers directly.
 * The peers don't have to ask for slot creation via a walsender command or SQL
 * function call. This is done so that nodes can create slots for a peer as
 * part of a consensus message exchange during setup.
 *
 * So these are inbound slots, which other peers will use to talk to us.
 * We expect the peer to in turn create the slots we need to talk to it.
 */
void
bdr_join_create_slots(BdrNodeInfo *local)
{
	List	   *nodes;
	ListCell   *lc;

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);

	foreach (lc, nodes)
	{
		BdrNodeInfo *remote = lfirst(lc);

		if (remote->bdr_node->node_id == local->bdr_node->node_id)
			continue;

		bdr_join_create_slot(local, remote);
	}
}

void
bdr_join_go_active(BdrNodeInfo *local)
{
	List	   *nodes;
	ListCell   *lc;

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);

	/*
	 * Enable subscriptions
	 *
	 * TODO: later we'll instead be switching these from catchup-only mode
	 * to actually replaying directly, after first switching the join
	 * subscription from catchup to normal replay.
	 */
	foreach (lc, nodes)
	{
		BdrNodeInfo	   *remote = lfirst(lc);
		List		   *subs;
		ListCell	   *lcsub;

		subs = bdr_get_node_subscriptions(remote->bdr_node->node_id);
		foreach (lcsub, subs)
		{
			PGLogicalSubscription *sub = lfirst(lcsub);
			sub->enabled = true;
			alter_subscription(sub);
		}
	}
}
