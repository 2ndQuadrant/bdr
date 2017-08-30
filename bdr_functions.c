/*-------------------------------------------------------------------------
 *
 * bdr_functions.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_functions.c
 *
 * SQL-callable function interface for BDR
 *-------------------------------------------------------------------------
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

/*
 * Ensure that the local BDR node exists
 */
static BdrNodeInfo *
bdr_check_local_node(bool for_update)
{
	BdrNodeInfo *nodeinfo;

	nodeinfo = bdr_get_local_node_info(for_update, true);
	if (nodeinfo == NULL || nodeinfo->bdr_node == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as bdr node"),
				 errhint("create bdr node first")));

	return nodeinfo;
}

PG_FUNCTION_INFO_V1(bdr_create_node_sql);

/*
 * Create a new local BDR node.
 *
 * If there's an existing pglogical node, it will be re-used if it has the same name
 * or the specified name is null.
 *
 * If no local pglogical node exists, one is created.
 */
Datum
bdr_create_node_sql(PG_FUNCTION_ARGS)
{
	BdrNode bnode;
	PGLogicalLocalNode *pgllocal;
	const char * node_name = NULL;
	const char * local_dsn = NULL;

	if (!PG_ARGISNULL(0))
		node_name = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (!PG_ARGISNULL(1))
		local_dsn = text_to_cstring(PG_GETARG_TEXT_P(1));
	
	/* Look up underlying pglogical node (if any) */
	pgllocal = get_local_node(false, true);

	/*
	 * Ensure local pglogical node exists and has matching characteristics,
	 * creating it if necessary.
	 */
	if (pgllocal == NULL)
	{
		if (node_name == NULL || local_dsn == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("node name and connection string must be specified when no local pglogical node already exists")));
		elog(NOTICE, "creating new pglogical node");
		create_node_defaults((char*)node_name, (char*)local_dsn);
		pgllocal = get_local_node(false, false);
	}
	else if (node_name != NULL && strcmp(pgllocal->node->name, node_name) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("A local pglogical node with name %s exists, cannot create node with name %s",
						pgllocal->node->name, node_name)));
	}
	else if (local_dsn != NULL && strcmp(pgllocal->node_if->dsn, local_dsn) != 0)
	{
		ereport(WARNING,
				(errmsg("connection string for existing local node does not match supplied connstring"),
				 errhint("Check the connection string for the local pglogical interface after node creation")));
	}

	/*
	 * Then make the BDR node on top. Most of the node is populated later.
	 */
	bnode.node_id = pgllocal->node->id;
	bnode.seq_id = -1;
	bnode.confirmed_our_join = false;
	bnode.node_group_id = InvalidOid;

	bdr_node_create(&bnode);

	PG_RETURN_OID(bnode.node_id);
}

PG_FUNCTION_INFO_V1(bdr_create_nodegroup_sql);

/*
 * Create a new BDR nodegroup and make it the nodegroup
 * of the local BDR node.
 */
Datum
bdr_create_nodegroup_sql(PG_FUNCTION_ARGS)
{
	BdrNodeGroup nodegroup;
	BdrNodeInfo *info;
	PGLogicalRepSet		repset;
	char * nodegroup_name;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("node group name may not be null")));

	nodegroup_name = text_to_cstring(PG_GETARG_TEXT_P(0));

	info = bdr_check_local_node(true);

	if (info->bdr_node->node_group_id != InvalidOid)
	{
		if (info->bdr_node_group == NULL)
		{
			/* shoudln't happen */
			elog(ERROR, "local node has node group set but no catalog entry for it found");
		}
		if (info->pgl_node == NULL)
		{
			/* shouldn't happen */
			elog(ERROR, "bdr node exists but no local pglogical node");
		}
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("local BDR node %s is already a member of a node group named %s",
				 		info->pgl_node->name, info->bdr_node_group->name)));
	}
	else
	{
		Assert(info->bdr_node_group == NULL);
	}

	/*
	 * BDR creates an 'internal' replication set with the same name as the BDR
	 * node group.
	 */
	repset.id = InvalidOid;
	repset.nodeid = info->bdr_node->node_id;
	repset.name = nodegroup_name;
	repset.replicate_insert = true;
	repset.replicate_update = true;
	repset.replicate_delete = true;
	repset.replicate_truncate = true;
	repset.isinternal = true;

	nodegroup.id = InvalidOid;
	nodegroup.name = nodegroup_name;
	nodegroup.default_repset = create_replication_set(&repset);
	nodegroup.id = bdr_nodegroup_create(&nodegroup);

	/* Assign the nodegroup to the local node */
	info->bdr_node->node_group_id = nodegroup.id;
	bdr_modify_node(info->bdr_node);

	PG_RETURN_OID(nodegroup.id);
}

PG_FUNCTION_INFO_V1(bdr_replication_set_add_table);

/*
 * BDR wrapper around pglogical's pglogical.replication_set_add_table
 * that adds a node to repsets on all nodes.
 *
 * If no repset is named, the default repset for the current node group
 * is used.
 */
Datum
bdr_replication_set_add_table(PG_FUNCTION_ARGS)
{
	Oid					reloid;
	Node			   *row_filter = NULL;
	List			   *att_list = NIL;
	PGLogicalRepSet    *repset = NULL;
	BdrNodeInfo		   *local;

	local = bdr_check_local_node(true);

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation cannot be NULL")));

	if (local->bdr_node_group == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("this bdr node is not part of a node group, cannot alter replication sets")));

	reloid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
	{
		/* Find default repset for local node's nodegroup */
		repset = get_replication_set(local->bdr_node_group->default_repset);
	}
	else
	{
		const char *repset_name = text_to_cstring(PG_GETARG_TEXT_P(1));
		repset = get_replication_set_by_name(local->bdr_node->node_id, repset_name, false);
		if (!repset->isinternal)
		{
			/*
			 * TODO: we should be checking a "bdr nodegroup repsets" catalog here to be sure
			 * it's really ours, but for now we don't actually support creation of repsets
			 * other than the default so it's kind of moot.
			 */
			elog(ERROR, "replication set '%s' does not appear to owned by a BDR node group",
				 repset->name);
		}
	}

	Assert(repset != NULL);

	if (!PG_ARGISNULL(2) && PG_GETARG_BOOL(2))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("initial table synchronisation not supported on bdr replication sets yet")));

	if (!PG_ARGISNULL(3))
		/* TODO Need to generalise and call the code in pglogical_replication_set_add_table */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("column filter not supported on bdr replication sets yet")));

	if (!PG_ARGISNULL(4))
		/* TODO Need to generalise and call the code in pglogical_replication_set_add_table */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row filter not supported on bdr replication sets yet")));

	replication_set_add_table(repset->id, reloid, att_list, row_filter);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(bdr_replication_set_remove_table);

/*
 * BDR wrapper around pglogical's pglogical.replication_set_add_table that
 * removes a table from the nodegroup's repset on all nodes.
 */
Datum
bdr_replication_set_remove_table(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	PGLogicalRepSet    *repset;
	BdrNodeInfo		   *local;

	local = bdr_check_local_node(true);

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation cannot be NULL")));

	if (local->bdr_node_group == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("this bdr node is not part of a node group, cannot alter replication sets")));

	reloid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
	{
		/* Find default repset for local node's nodegroup */
		repset = get_replication_set(local->bdr_node_group->default_repset);
	}
	else
	{
		const char *repset_name = text_to_cstring(PG_GETARG_TEXT_P(1));
		repset = get_replication_set_by_name(local->bdr_node->node_id, repset_name, false);
		if (!repset->isinternal)
		{
			/*
			 * TODO: we should be checking a "bdr nodegroup repsets" catalog here to be sure
			 * it's really ours, but for now we don't actually support creation of repsets
			 * other than the default so it's kind of moot.
			 */
			elog(ERROR, "replication set '%s' does not appear to owned by a BDR node group",
				 repset->name);
		}
	}

	Assert(repset != NULL);

	replication_set_remove_table(repset->id, reloid, false);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(bdr_decode_message_payload);

Datum
bdr_decode_message_payload(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unimplemented")));
}


PG_FUNCTION_INFO_V1(bdr_decode_state);

Datum
bdr_decode_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unimplemented")));
}

PG_FUNCTION_INFO_V1(bdr_submit_comment);

/*
 * Test function to submit a no-op message into the consensus
 * messaging system for replay to peer nodes.
 */
Datum
bdr_submit_comment(PG_FUNCTION_ARGS)
{
	BdrMessage *msg;
	const char *dummy_payload = text_to_cstring(PG_GETARG_TEXT_P(0));
	Size dummy_payload_length;
	uint64 handle;
	char handle_str[33];

	if (!bdr_is_active_db())
		elog(ERROR, "BDR is not active in this database");

	dummy_payload_length = strlen(dummy_payload)+ 1;

	msg = palloc(offsetof(BdrMessage,payload) + dummy_payload_length);
	msg->message_type = BDR_MSG_COMMENT;
	msg->payload_length = dummy_payload_length;
	memcpy(msg->payload, dummy_payload, dummy_payload_length);

	handle = bdr_msgs_enqueue_one(msg);
	if (handle == 0)
		/*
		 * TODO: block
		 */
		elog(WARNING, "manager couldn't enqueue message, try again later");
	else
		elog(INFO, "manager enqueued message with handle "UINT64_FORMAT, handle);

	snprintf(&handle_str[0], 33, UINT64_FORMAT, handle);
	PG_RETURN_TEXT_P(cstring_to_text(handle_str));
}
