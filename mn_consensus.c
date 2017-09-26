
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

#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "shm_mq_pool.h"
#include "mn_consensus.h"
#include "mn_consensus_impl.h"

#define recv_queue_size 1024

typedef enum MNConsensusRequestType
{
	MNRT_UNKNOWN_REQUEST = 0,
	MNRT_START_ENQUEUE,
	MNRT_ENQUEUE_PROPOSAL,
	MNRT_FINISH_ENQUEUE,
	MNRT_QUERY_STATUS,
} MNConsensusRequestType;

typedef enum MNConsensusResponseType
{
	MNRT_UNKNOWN_RESPONSE = 0,
	MNRT_START_ENQUEUE_RESULT,
	MNRT_ENQUEUE_PROPOSAL_RESULT,
	MNRT_FINISH_ENQUEUE_RESULT,
	MNRT_QUERY_STATUS_RESULT
} MNConsensusResponseType;

/*
 * A request carried on the submission queues.
 *
 * Used for enqueuing messages, getting response statuses, etc.
 *
 * (We do NOT need this for getting committed message payloads to other
 * backends, since they're available from the DB.)
 */
typedef struct MNConsensusRequest
{
	MNConsensusRequestType req_type;
	union {
		uint64				req_handle;
		MNConsensusProposal *proposal;
	} payload;
} MNConsensusRequest;

/*
 * Reply for the request above, written into the reply queue.
 */
typedef struct MNConsensusResponse
{
	MNConsensusResponseType res_type;
	union {
		uint64				req_handle;
		bool				req_result;
		MNConsensusStatus	req_status;
	} payload;
} MNConsensusResponse;


/* Pool in the receiver server proccess. */
static MQPool	   *MyMQPool = NULL;
static bool			am_server = false;

/* Connection to pool in backend. */
static MQPoolConn  *MyMQConn = NULL;

static void mn_consensus_request_cb(MQPoolConn *mqconn, void *data, Size len);

void
mn_consensus_start(uint32 local_node_id, const char *journal_schema,
				   const char *journal_relation, MNConsensusCallbacks *cbs)
{
	am_server = true;
	MyMQPool = shm_mq_pooler_new_pool("mn_consensus", 100, recv_queue_size,
									  NULL, NULL, mn_consensus_request_cb);

	consensus_startup(local_node_id, journal_schema, journal_relation,cbs);
}

void
mn_consensus_shutdown(void)
{
	consensus_shutdown();
}

static Size
mn_consensus_proposal_size(MNConsensusProposal *proposal)
{
	return sizeof(proposal->payload_length) + proposal->payload_length;
}

static Size
mn_consensus_request_len(MNConsensusRequest *req)
{
	switch (req->req_type)
	{
		case MNRT_START_ENQUEUE:
		case MNRT_FINISH_ENQUEUE:
		case MNRT_QUERY_STATUS:
			return sizeof(MNConsensusRequest);
		case MNRT_ENQUEUE_PROPOSAL:
			return sizeof(MNConsensusRequest) + mn_consensus_proposal_size(req->payload.proposal);
		default:
			elog(ERROR, "unrecognized consensus request type %u",
				 req->req_type);
	}
}

static void
mn_serialize_response(StringInfo s, MNConsensusResponse *res)
{
	pq_sendbytes(s, (char *) res, sizeof(MNConsensusResponse));
}

static MNConsensusResponse *
mn_deserialize_response(StringInfo s)
{
	MNConsensusResponse *res;
	res = palloc0(sizeof(MNConsensusResponse));

	pq_copymsgbytes(s, (char *) res, sizeof(MNConsensusResponse));

	return res;
}

static void
mn_serialize_request(StringInfo s, MNConsensusRequest *req)
{
	pq_sendbytes(s, (char *) req, sizeof(MNConsensusRequest));
	if (req->req_type == MNRT_ENQUEUE_PROPOSAL)
	{
		pq_sendint64(s, req->payload.proposal->payload_length);
		pq_sendbytes(s, req->payload.proposal->payload,
					 req->payload.proposal->payload_length);
	}
}

static MNConsensusRequest *
mn_deserialize_request(StringInfo s)
{
	MNConsensusRequest *req;
	req = palloc0(sizeof(MNConsensusRequest));

	pq_copymsgbytes(s, (char *) req, sizeof(MNConsensusRequest));
	if (req->req_type == MNRT_ENQUEUE_PROPOSAL)
	{
		req->payload.proposal->payload_length = pq_getmsgint64(s);
		req->payload.proposal->payload = palloc0(req->payload.proposal->payload_length);
		pq_copymsgbytes(s, req->payload.proposal->payload,
						req->payload.proposal->payload_length);
	}

	return req;
}

static void
mn_consensus_send_response(MQPoolConn *mqconn, MNConsensusResponse *res)
{
	StringInfoData	s;

	initStringInfo(&s);
	mn_serialize_response(&s, res);

	if (!shm_mq_pool_write(mqconn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("peer appears to have disconnected")));

	pfree(s.data);
}

/*
 * We got a message from a peer in the manager and need to dispatch a reply
 * once we've acted on it.
 */
static void
mn_consensus_request_cb(MQPoolConn *mqconn, void *data, Size len)
{
	MNConsensusRequest *request;
	MNConsensusResponse	response = { MNRT_UNKNOWN_RESPONSE };
	StringInfoData		s;

	/*
	 * Unpack the message and decide what the sender wants to do.
	 *
	 * Since the message structure is from the same postgres instance and hasn't
	 * gone over the wire, we can just access it directly.
	 */
	wrapInStringInfo(&s, (char *) data, len);
 	request = mn_deserialize_request(&s);

	Assert(len == mn_consensus_request_len(request));

	/* Proccess the message and construct reply as needed. */
	switch(request->req_type)
	{
		case MNRT_START_ENQUEUE:
		{
			bool ret = consensus_begin_enqueue();
			response.res_type = MNRT_START_ENQUEUE_RESULT;
			response.payload.req_result = ret;
			break;
		}
		case MNRT_ENQUEUE_PROPOSAL:
		{
			uint64				handle;
			/*
			 * TODO: error reporting to peers on submit failure
			 */
			handle = consensus_enqueue_proposal(request->payload.proposal);
			response.res_type = MNRT_ENQUEUE_PROPOSAL_RESULT;
			response.payload.req_handle = handle;
			break;
		}
		case MNRT_FINISH_ENQUEUE:
		{
			uint64 handle = consensus_finish_enqueue();
			response.res_type = MNRT_FINISH_ENQUEUE_RESULT;
			response.payload.req_handle = handle;
			break;
		}
		case MNRT_QUERY_STATUS:
		{
			MNConsensusStatus status;
			status = consensus_proposals_status(request->payload.req_handle);
			response.res_type = MNRT_QUERY_STATUS_RESULT;
			response.payload.req_status = status;
			break;
		}
		default:
			elog(ERROR, "peer sent unsupported message type %u to manager, shmem protocol violation",
				 request->req_type);
	}

	/* Send reply. */
	Assert(response.res_type != MNRT_UNKNOWN_RESPONSE);
	mn_consensus_send_response(mqconn, &response);
}

/*
 * From a non-manager backend, submit a message to the manager
 * and return the manager's reply.
 */
static MNConsensusResponse*
mn_consensus_send_request(MNConsensusRequest *req)
{
	StringInfoData			s;
	MNConsensusResponse	   *response;

	initStringInfo(&s);
	mn_serialize_request(&s, req);
	if (!shm_mq_pool_write(MyMQConn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("manager appears to have exited")));

	pfree(s.data);
	initStringInfo(&s);

	if (!shm_mq_pool_receive(MyMQConn, &s, false))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("manager appears to have exited")));

	if (s.len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unexpected zero length reply")));
	response = mn_deserialize_response(&s);

	return response;
}

bool
mn_consensus_begin_enqueue(void)
{
	MQPool				   *mqpool;
	MNConsensusRequest		req;
	MNConsensusResponse	   *res;

	if (am_server)
		return consensus_begin_enqueue();

	mqpool = shm_mq_pool_get_pool("mn_consensus");
	if (!mqpool)
		ereport(ERROR, (errmsg("could not get mn_consensus pool")));
	MyMQConn = shm_mq_pool_get_connection(mqpool, false);
	if (!MyMQConn)
		ereport(ERROR, (errmsg("could not get mn_consensus connection")));

	req.req_type = MNRT_START_ENQUEUE;
	res = mn_consensus_send_request(&req);
	Assert(res->res_type == MNRT_START_ENQUEUE_RESULT);

	return res->payload.req_result;
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
	uint64					handle;
	MNConsensusRequest		req;
	MNConsensusResponse	   *res;

	if (am_server)
		return consensus_enqueue_proposal(proposal);

	Assert(MyMQConn);

	req.req_type = MNRT_ENQUEUE_PROPOSAL;
	req.payload.proposal = proposal;
	res = mn_consensus_send_request(&req);

	Assert(res->res_type == MNRT_ENQUEUE_PROPOSAL_RESULT);
	handle = res->payload.req_handle;

	return handle;
}

uint64
mn_consensus_finish_enqueue(void)
{
	uint64					handle;
	MNConsensusRequest		req;
	MNConsensusResponse	   *res;

	if (am_server)
		return consensus_finish_enqueue();

	Assert(MyMQConn);

	req.req_type = MNRT_FINISH_ENQUEUE;
	res = mn_consensus_send_request(&req);
	Assert(res->res_type == MNRT_FINISH_ENQUEUE_RESULT);
	handle = res->payload.req_handle;

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
	MNConsensusRequest		req;
	MNConsensusResponse	   *res;
	MNConsensusStatus		status;
	bool					conected = false;

	if (am_server)
		return consensus_proposals_status(msg_handle);

	if (!MyMQConn)
	{
		mqpool = shm_mq_pool_get_pool("mn_consensus");
		if (!mqpool)
			ereport(ERROR, (errmsg("could not get mn_consensus pool")));
		MyMQConn = shm_mq_pool_get_connection(mqpool, false);
		if (!MyMQConn)
			ereport(ERROR, (errmsg("could not get mn_consensus connection")));

		conected = true;
	}

	req.req_type = MNRT_QUERY_STATUS;
	res = mn_consensus_send_request(&req);
	Assert(res->res_type == MNRT_QUERY_STATUS_RESULT);
	status = res->payload.req_status;

	if (conected)
	{
		shm_mq_pool_disconnect(MyMQConn);
		MyMQConn = NULL;
	}

	return status;
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
mn_consensus_add_or_update_node(uint32 nodeid, const char *dsn, bool update_if_exists)
{
	consensus_add_or_update_node(nodeid, dsn, update_if_exists);
}

void
mn_consensus_remove_node(uint32 nodeid)
{
	consensus_remove_node(nodeid);
}

struct mn_msg_errcontext {
	const char	   *msgtype;
	StringInfo		msgdata;
};

static void
deserialize_mn_msg_errcontext_callback(void *arg)
{
	struct mn_msg_errcontext *ctx = arg;
	char * hexmsg = palloc(ctx->msgdata->len * 2 + 1);

	hex_encode(ctx->msgdata->data, ctx->msgdata->len, hexmsg);
	hexmsg[ctx->msgdata->len * 2] = '\0';

	/*
	 * This errcontext is only used when we're deserializing messages, so it's
	 * OK for it to be pretty verbose.
	 */
	errcontext("while deserialising %s message of length %d at cursor %d with data %s",
			   ctx->msgtype, ctx->msgdata->len, ctx->msgdata->cursor, hexmsg);

	pfree(hexmsg);
}

void
mn_serialize_consensus_proposal(MNConsensusProposal *proposal,
								StringInfo s)
{
	pq_sendint64(s, proposal->payload_length);
	pq_sendbytes(s, (char *) proposal->payload, proposal->payload_length);
}

MNConsensusProposal *
mn_deserialize_consensus_proposal(const char *data, Size len)
{
	StringInfoData		s;
	MNConsensusProposal *proposal = palloc0(sizeof(MNConsensusProposal));
	ErrorContextCallback myerrcontext;
	struct mn_msg_errcontext ctxinfo;

	/*
	 * We are not going to change the data but the StringInfo contains char,
	 * not const char...
	 */
	wrapInStringInfo(&s, (char *) data, len);

	ctxinfo.msgtype = "consensus proposal";
	ctxinfo.msgdata = &s;
	myerrcontext.callback = deserialize_mn_msg_errcontext_callback;
	myerrcontext.arg = &ctxinfo;
	myerrcontext.previous = error_context_stack;
	error_context_stack = &myerrcontext;

	proposal->payload_length = pq_getmsgint64(&s);
	proposal->payload = palloc(proposal->payload_length);
	pq_copymsgbytes(&s, proposal->payload, proposal->payload_length);

	error_context_stack = myerrcontext.previous;

	return proposal;
}

void
mn_serialize_consensus_proposal_msg(MNConsensusMessage *msg,
									StringInfo s)
{
	pq_sendint(s, msg->sender_nodeid, 4);
	pq_sendint64(s, msg->sender_local_msgnum);
	pq_sendint64(s, msg->sender_lsn);
	pq_sendint64(s, msg->sender_timestamp);
	pq_sendint64(s, msg->global_id);
	pq_sendint(s, msg->msg_kind, 4);
	if (msg->msg_kind == MNCONSENSUS_MSG_KIND_PROPOSAL)
		mn_serialize_consensus_proposal(msg->proposal, s);
}

MNConsensusMessage *
mn_deserialize_consensus_proposal_msg(const char *data, Size len)
{
	StringInfoData		s;
	MNConsensusMessage *msg = palloc0(sizeof(MNConsensusMessage));
	ErrorContextCallback myerrcontext;
	struct mn_msg_errcontext ctxinfo;

	/*
	 * We are not going to change the data but the StringInfo contains char,
	 * not const char...
	 */
	wrapInStringInfo(&s, (char *) data, len);

	ctxinfo.msgtype = "consensus proposal message";
	ctxinfo.msgdata = &s;
	myerrcontext.callback = deserialize_mn_msg_errcontext_callback;
	myerrcontext.arg = &ctxinfo;
	myerrcontext.previous = error_context_stack;
	error_context_stack = &myerrcontext;

	memset(msg, 0, sizeof(MNConsensusMessage));
	msg->sender_nodeid = pq_getmsgint(&s, 4);
	msg->sender_local_msgnum = pq_getmsgint64(&s);
	msg->sender_lsn = pq_getmsgint64(&s);
	msg->sender_timestamp = pq_getmsgint64(&s);
	msg->global_id = pq_getmsgint64(&s);
	msg->msg_kind = pq_getmsgint(&s, 4);
	if (msg->msg_kind == MNCONSENSUS_MSG_KIND_PROPOSAL)
		msg->proposal = mn_deserialize_consensus_proposal(&s.data[s.cursor],
														  len - s.cursor);

	error_context_stack = myerrcontext.previous;

	return msg;
}

uint32
mn_consensus_active_nodeid(void)
{
	return consensus_active_nodeid();
}
