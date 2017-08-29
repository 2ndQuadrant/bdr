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

#include "bdr_catcache.h"
#include "bdr_messaging.h"
#include "bdr_functions.h"

PG_FUNCTION_INFO_V1(bdr_create_node_sql)

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
	const char *nodegroup_name;
	BdrNodeGroup *nodegroup;
	BdrNodeInfo *info;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("node group name may not be null")));

	info = bdr_get_local_node_info(true);
	if (info->bdr_node == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no local BDR node exists to assign to new node group")));

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
				(errcode(ERRCODE_NOT_IN_PREREQUISITE_STATE),
				 errmsg("local BDR node %s is already a member of a node group named %s",
				 		info->pgl_node->name, info->bdr_node_group->name)));
	}
	else
	{
		Assert(info->bdr_node_group == NULL);
	}

	nodegroup.id = InvalidOid;
	nodegroup.name = text_to_cstring(PG_GETARG_TEXT_P(0));
	nodegroup.id  = bdr_nodegroup_create(&nodegroup);

	info->bdr_node->node_group_id = nodegroup.id;
	bdr_modify_node(info->bdr_node);

	PG_RETURN_OID(nodegroup.id);
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
