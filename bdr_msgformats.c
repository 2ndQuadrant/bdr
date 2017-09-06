/*-------------------------------------------------------------------------
 *
 * bdr_msgformats.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_msgformats.c
 *
 * Functions to serialize and deserialize messages (WAL messages,
 * consensus proposals, etc).
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "bdr_worker.h"
#include "bdr_messaging.h"
#include "bdr_msgformats.h"

void
wrapInStringInfo(StringInfo si, char *data, Size length)
{
	si->data = data;
	si->len = length;
	si->maxlen = -1;
	si->cursor = 0;
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
	void* message)
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
msg_deserialize_proposal(ConsensusProposal *in)
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

	if (in->payload_length < 4)
	{
		elog(LOG, "ignored bad bdr message: consensus message from %u missing payload",
			 out->originator_id);
		return NULL;
	}

	wrapInStringInfo(&si, in->payload, in->payload_length);
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
		"joining node: name %s, id %u, state %d, ifname %s, ifid %u, dsn %s; ",
		request->joining_node_name, request->joining_node_id,
		request->joining_node_state, request->joining_node_if_name,
		request->joining_node_if_id, request->joining_node_if_dsn);

	appendStringInfo(out,
		"join target: name %s, id %u",
		request->join_target_node_name, request->join_target_node_id);
}
