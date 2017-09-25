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
bdr_consensus_refresh_nodes(void)
{
	bool txn_started = false;
	List	   *subs;
	ListCell   *lc;

	if (!IsTransactionState())
	{
		txn_started = true;
		StartTransactionCommand();
	}

	subs = bdr_get_node_subscriptions(bdr_get_local_nodeid());

	foreach (lc, subs)
	{
		BdrSubscription *bsub = lfirst(lc);
		PGLogicalSubscription *sub = get_subscription(bsub->pglogical_subscription_id);
		elog(LOG, "XXX adding/refreshing %u", sub->origin->id);
		mn_consensus_add_node(sub->origin->id, sub->origin_if->dsn, true);
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

	bmsg = msg_deserialize_proposal(request);

	if (!bmsg)
	{
		/*
		 * Bad message. TODO: abort txn
		 */
		return false;
	}

	switch (bmsg->message_type)
	{
		case BDR_MSG_COMMENT:
			elog(bdr_debug_level, "%u %s proposal from %u: %s",
				 bdr_get_local_nodeid(),
				 bdr_message_type_to_string(bmsg->message_type),
				 request->sender_nodeid,
				 (char*)bmsg->message);
			break;
		case BDR_MSG_NODE_JOIN_REQUEST:
			{
				StringInfoData logmsg;
				initStringInfo(&logmsg);
				msg_stringify_join_request(&logmsg, bmsg->message);
				elog(bdr_debug_level, "%u %s proposal from %u: %s",
					 bdr_get_local_nodeid(),
					 bdr_message_type_to_string(bmsg->message_type),
					 request->sender_nodeid, logmsg.data);
				pfree(logmsg.data);
				break;
			}
		case BDR_MSG_NODE_CATCHUP_READY:
			elog(bdr_debug_level, "%u %s proposal from %u",
				 bdr_get_local_nodeid(),
				 bdr_message_type_to_string(bmsg->message_type),
				 request->sender_nodeid);
			break;
		case BDR_MSG_NODE_ACTIVE:
			elog(bdr_debug_level, "%u %s proposal from %u",
				 bdr_get_local_nodeid(),
				 bdr_message_type_to_string(bmsg->message_type),
				 request->sender_nodeid);
			break;
		default:
			elog(bdr_debug_level,
				 "%u unhandled BDR proposal type '%s' ignored",
				 bdr_get_local_nodeid(),
				 bdr_message_type_to_string(bmsg->message_type));
			return false;
	}

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
			case BDR_MSG_NODE_CATCHUP_READY:
				bdr_join_handle_catchup_proposal(bmsg);
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
	resetStringInfo(out);
	Assert(out->len == 0);

	pq_sendint(out, message_type, 4);
	Assert(out->len == SizeOfBdrMsgHeader);

	switch (message_type)
	{
		case BDR_MSG_COMMENT:
			pq_sendstring(out, message);
			break;
		case BDR_MSG_NODE_JOIN_REQUEST:
			msg_serialize_join_request(out, message);
			break;
		case BDR_MSG_NODE_CATCHUP_READY:
		case BDR_MSG_NODE_ACTIVE:
			/* Empty messages */
			Assert(message == NULL);
			break;
		default:
			Assert(false);
			elog(ERROR, "serialization for message %d not implemented",
				 message_type);
			break;
	}
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

	out = palloc(sizeof(BdrMessage));

	out->global_consensus_no = in->global_proposal_id;
	out->originator_id = in->sender_nodeid;
	out->originator_propose_time = in->sender_timestamp;
	out->originator_propose_lsn = in->sender_lsn;
	/* TODO: support majority consensus */
	out->majority_consensus_ok = false;

	if (in->proposal->payload_length < 4)
	{
		elog(LOG, "ignored bad bdr message: consensus message from %u missing payload",
			 out->originator_id);
		return NULL;
	}

	wrapInStringInfo(&si, in->proposal->payload, in->proposal->payload_length);
	out->message_type = pq_getmsgint(&si, 4);

	switch (out->message_type)
	{
		case BDR_MSG_COMMENT:
			out->message = (char*)pq_getmsgstring(&si);
			break;
		case BDR_MSG_NODE_JOIN_REQUEST:
			out->message = palloc(sizeof(BdrMsgJoinRequest));
			msg_deserialize_join_request(&si, out->message);
			break;
		case BDR_MSG_NODE_CATCHUP_READY:
		case BDR_MSG_NODE_ACTIVE:
			/*
			 * Empty messages, at least as far as we know, but later
			 * verisons might add fields so we won't assert anything.
			 */
			out->message = NULL;
			break;
		default:
			elog(bdr_debug_level, "ignored payload of unsupported bdr message type %u from node %u",
				 out->message_type, in->sender_nodeid);
			out->message = NULL;
			break;
	}

	return out;
}

void
msg_serialize_join_request(StringInfo join_request,
	BdrMsgJoinRequest *request)
{
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

void
msg_deserialize_join_request(StringInfo join_request,
	BdrMsgJoinRequest *request)
{
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
}

void
msg_stringify_join_request(StringInfo out, BdrMsgJoinRequest *request)
{
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
		case BDR_MSG_NODE_CATCHUP_READY:
			return CppAsString2(BDR_MSG_NODE_CATCHUP_READY);
		case BDR_MSG_NODE_ACTIVE:
			return CppAsString2(BDR_MSG_NODE_ACTIVE);
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
