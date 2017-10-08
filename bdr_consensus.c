/*-------------------------------------------------------------------------
 *
 * bdr_consensus.c
 * 		BDR specific consensus handling
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_consensus.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "access/xact.h"

#include "utils/builtins.h"

#include "pglogical_plugins.h"

#include "mn_consensus.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_join.h"
#include "bdr_state.h"
#include "bdr_worker.h"

static mn_waitevents_fill_cb		waitevents_fill_cb = NULL;
static int							waitevents_requested = 0;

static bool bdr_proposal_receive(MNConsensusProposalRequest *request);
static bool bdr_proposals_prepare(List *requests);
static void bdr_proposals_commit(List *requests);
static void bdr_proposals_rollback(void);
static void bdr_consensus_request_waitevents(int nwaitevents,
											 mn_waitevents_fill_cb cb);


static MNConsensusCallbacks consensus_callbacks = {
	bdr_proposal_receive,
	bdr_proposals_prepare,
	bdr_proposals_commit,
	bdr_proposals_rollback,
	bdr_consensus_request_waitevents
};

typedef void (*msg_serialize_fn)(StringInfo outmsgdata, void *msg);
typedef void* (*msg_deserialize_fn)(StringInfo msgdata);
typedef void (*msg_stringify_fn)(StringInfo out, void *msg);

typedef struct MNMessageFuncs {
	BdrMessageType		msgtype;
	msg_serialize_fn	serialize;
	msg_deserialize_fn	deserialize;
	msg_stringify_fn	stringify;
} MNMessageFuncs;

static void msg_serialize_join_request(StringInfo join_request, void *request);
static void* msg_deserialize_join_request(StringInfo join_request);
static void msg_stringify_join_request(StringInfo out, void *request);

static void msg_serialize_slot_created(StringInfo slot_created_request, void *created);
static void* msg_deserialize_slot_created( StringInfo slot_created_request);
static void msg_stringify_slot_created(StringInfo out, void *created);

static void msg_serialize_comment(StringInfo comment_request, void* comment);
static void* msg_deserialize_comment( StringInfo comment_request);
static void msg_stringify_comment(StringInfo out, void* comment);

static const MNMessageFuncs message_funcs[] = {
	{BDR_MSG_COMMENT,			msg_serialize_comment,		msg_deserialize_comment,	  msg_stringify_comment},
	{BDR_MSG_NODE_JOIN_REQUEST, msg_serialize_join_request, msg_deserialize_join_request, msg_stringify_join_request},
	{BDR_MSG_NODE_SLOT_CREATED, msg_serialize_slot_created, msg_deserialize_slot_created, msg_stringify_slot_created},
	/* must be last: */
	{BDR_MSG_NOOP, NULL, NULL, NULL}
};

static const MNMessageFuncs*
message_funcs_lookup(BdrMessageType msgtype)
{
	const MNMessageFuncs *ret = &message_funcs[0];
	while (ret->msgtype != BDR_MSG_NOOP && ret->msgtype != msgtype)
		ret++;

	if (ret->msgtype == BDR_MSG_NOOP)
		/* no entry */
		return NULL;

	return ret;
}

struct bdr_msg_errcontext {
	/* When deserializing a message: */
	BdrMessageType	msgtype;
	StringInfo		msgdata;
};

static void
deserialize_msg_errcontext_callback(void *arg)
{
	struct bdr_msg_errcontext *ctx = arg;	
	char * hexmsg = palloc(ctx->msgdata->len * 2 + 1);

	hex_encode(ctx->msgdata->data, ctx->msgdata->len, hexmsg);
	hexmsg[ctx->msgdata->len * 2 + 1] = '\0';

	/*
	 * This errcontext is only used when we're deserializing messages, so it's
	 * OK for it to be pretty verbose.
	 */
	errcontext("while deserialising bdr %s consensus message of length %d at cursor %d with data %s",
			   bdr_message_type_to_string(ctx->msgtype),
			   ctx->msgdata->len, ctx->msgdata->cursor,
			   hexmsg);

	pfree(hexmsg);
}

void
bdr_start_consensus(BdrNodeState cur_state)
{
	bool txn_started = false;

	/*
	 * Node isn't ready for consensus manager startup yet. If it's during join,
	 * the join process will make it start later on.
	 */
	if (cur_state < BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS)
		return;

	if (!IsTransactionState())
	{
		txn_started = true;
		StartTransactionCommand();
	}

	mn_consensus_start(bdr_get_local_nodeid(), BDR_SCHEMA_NAME,
					   BDR_MSGJOURNAL_REL_NAME, &consensus_callbacks);

	if (txn_started)
		CommitTransactionCommand();
}

void
bdr_consensus_refresh_nodes(BdrNodeState cur_state)
{
	bool txn_started = false;
	List	   *nodes;
	ListCell   *lc;

	/*
	 * Node isn't ready for consensus manager yet. If it's during join, the
	 * join process will start and refresh later on.
	 */
	if (cur_state < BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS)
		return;

	if (!IsTransactionState())
	{
		txn_started = true;
		StartTransactionCommand();
	}

	nodes = bdr_get_nodes_info(bdr_get_local_nodegroup_id(false));

	foreach (lc, nodes)
	{
		BdrNodeInfo *node = lfirst(lc);

		if (node->pgl_node->id == bdr_get_local_nodeid())
			continue;

		mn_consensus_add_or_update_node(node->pgl_node->id, node->pgl_interface->dsn,
			true);
	}

	/*
	 * TODO: remove nodes no longer involved
	 * TODO: check node status and only add nodes we want to hear from
	 */
	if (txn_started)
		CommitTransactionCommand();
}

void
bdr_shutdown_consensus(void)
{
	mn_consensus_shutdown();
}

uint64
bdr_consensus_enqueue_proposal(BdrMessageType message_type, void *message)
{
	uint64				handle;
	uint64				handle2 PG_USED_FOR_ASSERTS_ONLY;
	StringInfoData		s;
	MNConsensusProposal	proposal;

	if (!mn_consensus_begin_enqueue())
		return 0;

	initStringInfo(&s);
	msg_serialize_proposal(&s, message_type, message);

	elog(bdr_debug_level, "%u enqueuing consensus proposal %s of size %d",
		 bdr_get_local_nodeid(), bdr_message_type_to_string(message_type),
		 s.len);

	proposal.payload = s.data;
	proposal.payload_length = s.len;

	handle = mn_consensus_enqueue(&proposal);
	handle2 = mn_consensus_finish_enqueue();
	Assert(handle2 == handle);

	return handle;
}

static bool
bdr_proposal_receive(MNConsensusProposalRequest *request)
{
	BdrMessage *bmsg;
	const MNMessageFuncs *funcs;
	const char *msgdetail;

	bmsg = msg_deserialize_proposal(request);

	if (!bmsg)
	{
		/*
		 * Bad message. TODO: abort txn
		 */
		return false;
	}

	funcs = message_funcs_lookup(bmsg->message_type);
	if (funcs != NULL && bmsg->message != NULL)
	{
		StringInfoData msgbody;
		initStringInfo(&msgbody);
		funcs->stringify(&msgbody, bmsg->message);
		msgdetail = msgbody.data;
	}
	else
		msgdetail = "(no payload)";

	elog(bdr_debug_level, "%u %s proposal from %u: %s",
		 bdr_get_local_nodeid(),
		 bdr_message_type_to_string(bmsg->message_type),
		 request->sender_nodeid,
		 msgdetail);

	/* TODO: can nack messages here to abort consensus txn early */
	return true;
}

static bool
bdr_proposals_prepare(List *requests)
{
	ListCell *lc;

	elog(bdr_debug_level, "%u (%s) handling CONSENSUS PREPARE for %d message transaction",
		 bdr_get_local_nodeid(),
		 mn_consensus_active_nodeid() == bdr_get_local_nodeid() ? "self" : "peer",
		 list_length(requests));

	foreach (lc, requests)
	{
		MNConsensusProposalRequest	   *request = lfirst(lc);
		BdrMessage		   *bmsg;

		bmsg = msg_deserialize_proposal(request);

		elog(bdr_debug_level, "%u dispatching prepare of proposal %s from %u",
			 bdr_get_local_nodeid(),
			 bdr_message_type_to_string(bmsg->message_type),
			 bmsg->originator_id);

		/*
		 * TODO: should dispatch message processing via the local node state
		 * machine, but for now we'll do it directly here.
		 */
		switch (bmsg->message_type)
		{
			case BDR_MSG_COMMENT:
				break;
			case BDR_MSG_NODE_JOIN_REQUEST:
				bdr_join_handle_join_proposal(bmsg);
				break;
			case BDR_MSG_NODE_STANDBY_READY:
				bdr_join_handle_standby_proposal(bmsg);
				break;
			case BDR_MSG_NODE_ACTIVE:
				bdr_join_handle_active_proposal(bmsg);
				break;
			default:
				elog(ERROR, "unhandled message type %u in prepare proposal",
					 bmsg->message_type);
		}
	}

    /* TODO: here's where we apply in-transaction state changes like insert nodes */

    /* TODO: can nack messages here too */
    return true;
}

static void
bdr_proposals_commit(List *requests)
{
	elog(LOG, "XXX CONSENSUS COMMIT"); /* TODO */

    /* TODO: here's where we allow state changes to take effect */
}

/* TODO add message-id ranges rejected */
static void
bdr_proposals_rollback(void)
{
	elog(LOG, "XXX CONSENSUS ROLLBACK"); /* TODO */

    /* TODO: here's where we wind back any temporary state changes */
}

void
bdr_consensus_wait_event_set_recreated(WaitEventSet *new_set)
{
	if (!bdr_is_active_db())
		return;

	/*
	 * This can be called from other requestor before we initialized the
	 * waitevents_fill_cb.
	 */
	if (waitevents_fill_cb)
		waitevents_fill_cb(new_set);
}

int
bdr_consensus_get_wait_event_space_needed(void)
{
	return waitevents_requested;
}

static void
bdr_consensus_request_waitevents(int nwaitevents, mn_waitevents_fill_cb cb)
{
	waitevents_requested = nwaitevents;
	waitevents_fill_cb = cb;

	pglogical_manager_recreate_wait_event_set();
}

/*
 * Size of the BdrMessage header when sent as part of a ConsensusMessage
 * payload. Most of the BdrMessage gets copied from the ConsensusMessage
 * so there's only a small unique part: currently the message type
 */
#define SizeOfBdrMsgHeader (sizeof(uint32))

/*
 * Serialize a BDR message proposal for submission to the consensus
 * system or over shmem. This is the BDR specific header that'll
 * be added to consensus messages before the payload.
 *
 * No length word is included; it's expected that the container wrapping this
 * message payload will provide length information.
 */
void
msg_serialize_proposal(StringInfo out, BdrMessageType message_type,
					   void *message)
{
	const MNMessageFuncs *funcs;

	resetStringInfo(out);
	Assert(out->len == 0);

	pq_sendint(out, message_type, 4);
	Assert(out->len == SizeOfBdrMsgHeader);

	funcs = message_funcs_lookup(message_type);
	Assert( (message == NULL) == (funcs == NULL) );
	if (message != NULL)
		funcs->serialize(out, message);
}

/*
 * Deserialization of a BdrMessage copies fields from the ConsensusMessage it
 * came from into the BdrMessage header, so it needs more than the
 * ConsensusMessage's payload.
 *
 * Thus this isn't an exact mirror of msg_serialize_proposal.
 */
BdrMessage*
msg_deserialize_proposal(MNConsensusProposalRequest *in)
{
	BdrMessage *out;
	StringInfoData si;
	const MNMessageFuncs *funcs;

	out = palloc(sizeof(BdrMessage));

	out->global_consensus_no = in->global_proposal_id;
	out->originator_id = in->sender_nodeid;
	out->originator_propose_time = in->sender_timestamp;
	out->originator_propose_lsn = in->sender_lsn;
	/* TODO: support majority consensus */
	out->majority_consensus_ok = false;
	out->message = NULL;

	if (in->proposal->payload_length < 4)
	{
		elog(LOG, "ignored bad bdr message: consensus message from %u missing payload",
			 out->originator_id);
		return NULL;
	}

	wrapInStringInfo(&si, in->proposal->payload, in->proposal->payload_length);
	out->message_type = pq_getmsgint(&si, 4);

	funcs = message_funcs_lookup(out->message_type);

	if (funcs != NULL)
	{
		ErrorContextCallback myerrcontext;
		struct bdr_msg_errcontext ctxinfo;

		ctxinfo.msgtype = out->message_type;
		ctxinfo.msgdata = &si;
		myerrcontext.callback = deserialize_msg_errcontext_callback;
		myerrcontext.arg = &ctxinfo;
		myerrcontext.previous = error_context_stack;
		error_context_stack = &myerrcontext;

		out->message = funcs->deserialize(&si);
		
		error_context_stack = myerrcontext.previous;
	}

	return out;
}

void
msg_serialize_join_request(StringInfo join_request, void *jr)
{
	BdrMsgJoinRequest *request = jr;
	pq_sendstring(join_request, request->nodegroup_name);
	pq_sendint(join_request, request->nodegroup_id, 4);
	pq_sendstring(join_request, request->joining_node_name);
	pq_sendint(join_request, request->joining_node_id, 4);
	pq_sendint(join_request, request->joining_node_state, 4);
	pq_sendstring(join_request, request->joining_node_if_name);
	pq_sendint(join_request, request->joining_node_if_id, 4);
	pq_sendstring(join_request, request->joining_node_if_dsn);
	pq_sendstring(join_request, request->joining_node_dbname);
	pq_sendstring(join_request, request->join_target_node_name);
	pq_sendint(join_request, request->join_target_node_id, 4);
}

void*
msg_deserialize_join_request(StringInfo join_request)
{
	BdrMsgJoinRequest *request = palloc(sizeof(BdrMsgJoinRequest));
	request->nodegroup_name = pq_getmsgstring(join_request);
	request->nodegroup_id = pq_getmsgint(join_request, 4);
	request->joining_node_name = pq_getmsgstring(join_request);
	request->joining_node_id = pq_getmsgint(join_request, 4);
	request->joining_node_state = pq_getmsgint(join_request, 4);
	request->joining_node_if_name = pq_getmsgstring(join_request);
	request->joining_node_if_id = pq_getmsgint(join_request, 4);
	request->joining_node_if_dsn = pq_getmsgstring(join_request);
	request->joining_node_dbname = pq_getmsgstring(join_request);
	request->join_target_node_name = pq_getmsgstring(join_request);
	request->join_target_node_id = pq_getmsgint(join_request, 4);
	return request;
}

void
msg_stringify_join_request(StringInfo out, void *jr)
{
	BdrMsgJoinRequest *request = jr;
	appendStringInfo(out,
		"nodegroup: name %s, id: %u; ",
		request->nodegroup_name, request->nodegroup_id);

	appendStringInfo(out,
		"joining node: name %s, id %u, state %d, ifname %s, ifid %u, dsn %s; dbname %s",
		request->joining_node_name, request->joining_node_id,
		request->joining_node_state, request->joining_node_if_name,
		request->joining_node_if_id, request->joining_node_if_dsn,
		request->joining_node_dbname);

	appendStringInfo(out,
		"join target: name %s, id %u",
		request->join_target_node_name, request->join_target_node_id);
}

static void
msg_serialize_slot_created(StringInfo created_request, void *cr)
{
	BdrMsgSlotCreated *created = cr;
	pq_sendint(created_request, created->for_peer_id, 4);
	pq_sendstring(created_request, created->slot_name);
	pq_sendint64(created_request, created->start_lsn);
}

static void*
msg_deserialize_slot_created(
	StringInfo created_request)
{
	BdrMsgSlotCreated *created = palloc(sizeof(BdrMsgSlotCreated));
	created->for_peer_id = pq_getmsgint(created_request, 4);
	created->slot_name = pq_getmsgstring(created_request);
	created->start_lsn = pq_getmsgint64(created_request);
	return created;
}

static void
msg_stringify_slot_created(StringInfo out, void *cr)
{
	BdrMsgSlotCreated *created = cr;
	appendStringInfo(out,
		"slot created: for peer %u, slot name %s, start lsn %X/%X",
		created->for_peer_id, created->slot_name,
		(uint32)(created->start_lsn>>32),
		(uint32)(created->start_lsn));
}

static void
msg_serialize_comment(StringInfo comment_request,
	void* comment)
{
	pq_sendstring(comment_request, (const char*)comment);
}

static void*
msg_deserialize_comment(
	StringInfo comment_request)
{
	return (void*)pq_getmsgstring(comment_request);
}

static void
msg_stringify_comment(StringInfo out,
	void* comment)
{
	appendStringInfoString(out, "comment: ");
	appendStringInfoString(out, (const char*)comment);
}

const char *
bdr_message_type_to_string(BdrMessageType msgtype)
{
	StringInfoData si;

	switch (msgtype)
	{
		case BDR_MSG_NOOP:
			return CppAsString2(BDR_MSG_NOOP);
		case BDR_MSG_COMMENT:
			return CppAsString2(BDR_MSG_COMMENT);
		case BDR_MSG_NODE_JOIN_REQUEST:
			return CppAsString2(BDR_MSG_NODE_JOIN_REQUEST);
		case BDR_MSG_NODE_ID_SEQ_ALLOCATE:
			return CppAsString2(BDR_MSG_NODE_ID_SEQ_ALLOCATE);
		case BDR_MSG_NODE_STANDBY_READY:
			return CppAsString2(BDR_MSG_NODE_STANDBY_READY);
		case BDR_MSG_NODE_ACTIVE:
			return CppAsString2(BDR_MSG_NODE_ACTIVE);
		case BDR_MSG_NODE_SLOT_CREATED:
			return CppAsString2(BDR_MSG_NODE_SLOT_CREATED);
		case BDR_MSG_DDL_LOCK_REQUEST:
			return CppAsString2(BDR_MSG_DDL_LOCK_REQUEST);
		case BDR_MSG_DDL_LOCK_GRANT:
			return CppAsString2(BDR_MSG_DDL_LOCK_GRANT);
		case BDR_MSG_DDL_LOCK_REJECT:
			return CppAsString2(BDR_MSG_DDL_LOCK_REJECT);
		case BDR_MSG_DDL_LOCK_RELEASE:
			return CppAsString2(BDR_MSG_DDL_LOCK_RELEASE);
	}

	initStringInfo(&si);
	appendStringInfo(&si, "(unrecognised BDR message type %d)", msgtype);
	return si.data;
}
