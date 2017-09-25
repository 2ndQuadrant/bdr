/*-------------------------------------------------------------------------
 *
 * mn_consenus.c
 * 		Multi-node consensus implementation
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  mn_consenus.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"

#include "libpq/pqformat.h"

#include "miscadmin.h"

#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"

#include "utils/elog.h"
#include "utils/memutils.h"

#include "shm_mq_pool.h"
#include "mn_consensus.h"
#include "mn_consensus_impl.h"

#define recv_queue_size 1024

typedef enum MNConsensusMsgType
{
	MSG_UNUSED_TYPE = 0,
	MSG_START_ENQUEUE,
	MSG_START_ENQUEUE_RESULT,
	MSG_ENQUEUE_PROPOSAL,
	MSG_ENQUEUE_RESULT,
	MSG_FINISH_ENQUEUE,
	MSG_FINISH_ENQUEUE_RESULT,
	MSG_QUERY_STATUS,
	MSG_QUERY_STATUS_RESULT
} MNConsensusMsgType;

/*
 * A message (inbound or reply) carried on the message submission
 * queues.
 *
 * Used for enqueuing messages, getting response statuses, etc.
 *
 * (We do NOT need this for getting committed message payloads to other
 * backends, since they're available from the DB.)
 */
typedef struct MNConsensusMsg
{
	MNConsensusMsgType msg_type;
	Size	payload_length;
	char	payload[FLEXIBLE_ARRAY_MEMBER];
} MNConsensusMsg;

/* Pool in the receiver server proccess. */
static MQPool	   *MyMQPool = NULL;
static bool			am_server = false;

/* Connection to pool in backend. */
static MQPoolConn  *MyMQConn = NULL;

static void mn_consensus_message_cb(MQPoolConn *mqconn, void *data, Size len);

static Size
mn_consensus_msg_len(MNConsensusMsg *msg)
{
	return offsetof(MNConsensusMsg, payload) + msg->payload_length;
}

void
mn_consensus_start(uint32 local_node_id, const char *journal_schema,
				   const char *journal_relation, MNConsensusCallbacks *cbs)
{
	am_server = true;
	MyMQPool = shm_mq_pooler_new_pool("mn_consensus", 100, recv_queue_size,
									  NULL, NULL, mn_consensus_message_cb);

	consensus_startup(local_node_id, journal_schema, journal_relation,cbs);
}

void
mn_consensus_shutdown(void)
{
	consensus_shutdown();
}

/*
 * Prep a message for submission on the queue.
 *
 * Usable on both manager and peers.
 */
static MNConsensusMsg *
mn_consensus_make_msg(MNConsensusMsgType msgtype, Size payload_size,
					  void *payload)
{
	MNConsensusMsg	   *ret;

	ret = palloc0(offsetof(MNConsensusMsg, payload) + payload_size);
	ret->msg_type = msgtype;
	ret->payload_length = payload_size;
	memcpy(ret->payload, payload, payload_size);

	return ret;
}

static void
mn_consensus_send_msg(MQPoolConn *mqconn, MNConsensusMsg *msg)
{
	StringInfoData	s;

	s.data = (char *) msg;
	s.len = mn_consensus_msg_len(msg);

	if (!shm_mq_pool_write(mqconn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("peer appears to have disconnected")));
}

/*
 * We got a message from a peer in the manager and need to dispatch a reply
 * once we've acted on it.
 */
static void
mn_consensus_message_cb(MQPoolConn *mqconn, void *data, Size len)
{
	MNConsensusMsg *recvmsg;
	MNConsensusMsg *replymsg = NULL;

	/*
	 * Unpack the message and decide what the sender wants to do.
	 *
	 * Since the message structure is from the same postgres instance and hasn't
	 * gone over the wire, we can just access it directly.
	 *
	 * FIXME: alignment; should memcpy header into stack struct?
	 */
 	recvmsg = (MNConsensusMsg *) data;

	Assert(len == mn_consensus_msg_len(recvmsg));

	/* Proccess the message and construct reply as needed. */
	switch(recvmsg->msg_type)
	{
		case MSG_START_ENQUEUE:
		{
			bool ret = consensus_begin_enqueue();
			replymsg = mn_consensus_make_msg(MSG_START_ENQUEUE_RESULT,
											 sizeof(bool), &ret);
			break;
		}
		case MSG_ENQUEUE_PROPOSAL:
		{
			uint64				handle;
			MNConsensusProposal *proposal;
			/*
			 * TODO: error reporting to peers on submit failure
			 */
			proposal = mn_deserialize_consensus_proposal(recvmsg->payload,
														 recvmsg->payload_length);
			handle = consensus_enqueue_proposal(proposal);
			replymsg = mn_consensus_make_msg(MSG_ENQUEUE_RESULT, sizeof(uint64),
											 &handle);
			break;
		}
		case MSG_FINISH_ENQUEUE:
		{
			uint64 ret = consensus_finish_enqueue();
			replymsg = mn_consensus_make_msg(MSG_FINISH_ENQUEUE_RESULT,
											 sizeof(uint64), &ret);
			break;
		}
		case MSG_QUERY_STATUS:
		{
			MNConsensusStatus status;
			status = consensus_proposals_status(*((uint64*) recvmsg->payload));
			replymsg = mn_consensus_make_msg(MSG_QUERY_STATUS_RESULT,
											 sizeof(MNConsensusStatus),
											 &status);
			break;
		}
		case MSG_UNUSED_TYPE:
		case MSG_START_ENQUEUE_RESULT:
		case MSG_ENQUEUE_RESULT:
		case MSG_FINISH_ENQUEUE_RESULT:
		case MSG_QUERY_STATUS_RESULT:
			elog(ERROR, "peer sent unsupported message type %u to manager, shmem protocol violation",
				 replymsg->msg_type);
	}

	/* Send reply. */
	if (replymsg)
	{
		mn_consensus_send_msg(mqconn, replymsg);
		pfree(replymsg);
	}
}

/*
 * From a non-manager backend, submit a message to the manager
 * and return the manager's reply.
 */
static MNConsensusMsg*
mn_consensus_send_msg_with_reply(MNConsensusMsg *msg)
{
	StringInfoData		s;
	MNConsensusMsg	   *replymsg;

	initStringInfo(&s);
	s.data = (char *) msg;
	s.len = mn_consensus_msg_len(msg);
	if (!shm_mq_pool_write(MyMQConn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("manager appears to have exited")));

	resetStringInfo(&s);
	if (!shm_mq_pool_receive(MyMQConn, &s, false))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("manager appears to have exited")));

	if (s.len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unexpected zero length reply")));

	replymsg = (MNConsensusMsg *) s.data;
	Assert(s.len == mn_consensus_msg_len(replymsg));

	return replymsg;
}

bool
mn_consensus_begin_enqueue(void)
{
	MQPool			   *mqpool;
	MNConsensusMsg	   *msg;
	MNConsensusMsg	   *replymsg;

	if (am_server)
		return consensus_begin_enqueue();

	mqpool = shm_mq_pool_get_pool("mn_consensus");
	if (!mqpool)
		ereport(ERROR, (errmsg("could not get mn_consensus pool")));
	MyMQConn = shm_mq_pool_get_connection(mqpool, false);
	if (!MyMQConn)
		ereport(ERROR, (errmsg("could not get mn_consensus connection")));

	msg = mn_consensus_make_msg(MSG_START_ENQUEUE, 0, NULL);
	replymsg = mn_consensus_send_msg_with_reply(msg);
	Assert(replymsg->msg_type == MSG_START_ENQUEUE_RESULT);
	Assert(replymsg->payload_length == sizeof(bool));

	return *((bool*)replymsg->payload);
}

/*
 * Enqueue a message for processing on other peers. The message data, if passed,
 * must match the type and be recognised by msg_serialize_proposal /
 * msg_deserialize_proposal.
 *
 * Returns a handle that can be used to determine when the final message in the
 * set is finalized and what the outcome was. Use bdr_msg_get_outcome(...)
 * to look up the status.
 */
uint64
mn_consensus_enqueue(MNConsensusProposal *proposal)
{
	uint64				handle;
	StringInfoData		s;
	MNConsensusMsg	   *msg;
	MNConsensusMsg	   *replymsg;

	if (am_server)
		return consensus_enqueue_proposal(proposal);

	Assert(MyMQConn);

	initStringInfo(&s);
	mn_serialize_consensus_proposal(proposal, &s);
	msg = mn_consensus_make_msg(MSG_ENQUEUE_PROPOSAL, s.len, s.data);
	replymsg = mn_consensus_send_msg_with_reply(msg);
	Assert(replymsg->msg_type == MSG_ENQUEUE_RESULT);
	Assert(replymsg->payload_length == sizeof(uint64));
	handle = *((uint64*)replymsg->payload);

	return handle;
}

uint64
mn_consensus_finish_enqueue(void)
{
	uint64				handle;
	MNConsensusMsg	   *msg;
	MNConsensusMsg	   *replymsg;

	if (am_server)
		return consensus_finish_enqueue();

	Assert(MyMQConn);

	msg = mn_consensus_make_msg(MSG_FINISH_ENQUEUE, 0, NULL);
	replymsg = mn_consensus_send_msg_with_reply(msg);
	Assert(replymsg->msg_type == MSG_FINISH_ENQUEUE_RESULT);
	Assert(replymsg->payload_length == sizeof(uint64));
	handle = *((uint64*)replymsg->payload);

	shm_mq_pool_disconnect(MyMQConn);
	MyMQConn = NULL;

	return handle;
}

/*
 * Given a handle from bdr_proposals_enqueue, look up whether
 * a message is committed or not.
 */
MNConsensusStatus
mn_consensus_status(uint64 msg_handle)
{
	MQPool			   *mqpool;
	MNConsensusMsg	   *msg;
	MNConsensusMsg	   *replymsg;
	MNConsensusStatus	res;

	if (am_server)
		return consensus_proposals_status(msg_handle);

	mqpool = shm_mq_pool_get_pool("mn_consensus");
	if (!mqpool)
		ereport(ERROR, (errmsg("could not get mn_consensus pool")));
	MyMQConn = shm_mq_pool_get_connection(mqpool, false);
	if (!MyMQConn)
		ereport(ERROR, (errmsg("could not get mn_consensus connection")));

	msg = mn_consensus_make_msg(MSG_QUERY_STATUS, 0, NULL);
	replymsg = mn_consensus_send_msg_with_reply(msg);
	Assert(replymsg->msg_type == MSG_QUERY_STATUS_RESULT);
	Assert(replymsg->payload_length == sizeof(uint32));
	res = *((int32*)replymsg->payload);

	shm_mq_pool_disconnect(MyMQConn);
	MyMQConn = NULL;

	return res;
}

void
mn_consensus_wakeup(struct WaitEvent *events, int nevents,
					long *max_next_wait_ms)
{
	/*
	 * This is a good chance for us to check if we've had any new messages
	 * submitted to us for processing.
	 */
	shm_mq_pooler_work();

	/*
	 * Now process any consensus messages, and do any underlying
	 * message broker communication required.
	 */
	consensus_wakeup(events, nevents, max_next_wait_ms);

}

void
mn_consensus_add_node(uint32 nodeid, const char *dsn, bool update_if_exists)
{
	consensus_add_node(nodeid, dsn, update_if_exists);
}

void
mn_consensus_remove_node(uint32 nodeid)
{
	consensus_remove_node(nodeid);
}

void
mn_serialize_consensus_proposal(MNConsensusProposal *proposal,
								StringInfo s)
{
	pq_sendint64(s, proposal->payload_length);
	pq_sendbytes(s, proposal->payload, proposal->payload_length);
}

MNConsensusProposal *
mn_deserialize_consensus_proposal(const char *data, Size len)
{
	StringInfoData		s;
	MNConsensusProposal *proposal = palloc0(sizeof(MNConsensusProposal));

	/*
	 * We are not going to change the data but the StringInfo contains char,
	 * not const char...
	 */
	wrapInStringInfo(&s, (char *) data, len);
	proposal->payload_length = pq_getmsgint64(&s);
	proposal->payload = palloc(proposal->payload_length);
	pq_copymsgbytes(&s, proposal->payload, proposal->payload_length);

	return proposal;
}

void
mn_serialize_consensus_proposal_req(MNConsensusProposalRequest *req,
									StringInfo s)
{

	pq_sendint(s, req->sender_nodeid, 4);
	pq_sendint64(s, req->sender_local_msgnum);
	pq_sendint64(s, req->sender_lsn);
	pq_sendint64(s, req->sender_timestamp);
	pq_sendint64(s, req->global_proposal_id);
	mn_serialize_consensus_proposal(req->proposal, s);
}

MNConsensusProposalRequest *
mn_deserialize_consensus_proposal_req(const char *data, Size len)
{
	StringInfoData		s;
	MNConsensusProposalRequest *req = palloc0(sizeof(MNConsensusProposalRequest));

	/*
	 * We are not going to change the data but the StringInfo contains char,
	 * not const char...
	 */
	wrapInStringInfo(&s, (char *) data, len);
	memset(req, 0, sizeof(MNConsensusProposalRequest));
	req->sender_nodeid = pq_getmsgint(&s, 4);
	req->sender_local_msgnum = pq_getmsgint64(&s);
	req->sender_lsn = pq_getmsgint64(&s);
	req->sender_timestamp = pq_getmsgint64(&s);
	req->global_proposal_id = pq_getmsgint64(&s);
	req->proposal = mn_deserialize_consensus_proposal(&s.data[s.cursor],
													  len - s.cursor);

	return req;
}

uint32
mn_consensus_active_nodeid(void)
{
	return consensus_active_nodeid();
}
