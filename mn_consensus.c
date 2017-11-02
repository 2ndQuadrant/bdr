
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
#include "mn_msgbroker.h"
#include "mn_msgbroker_receive.h"
#include "mn_msgbroker_send.h"

#define recv_queue_size 1024

typedef struct RequestConn {
	dlist_node		node;
	uint64			req_id;
	MQPoolConn	   *mqconn;
} RequestConn;

static dlist_head	reqconns = DLIST_STATIC_INIT(reqconns);

/* Pool in the receiver server proccess. */
static MQPool	   *MyMQPool = NULL;
static bool			am_server = false;

/* Connection to pool in backend. */
static MQPoolConn  *MyMQConn = NULL;

/* local Raft server. */
static RaftServer  *MyRaftServer = NULL;

static uint32		MyNodeId = 0;

static void mn_consensus_local_message_cb(MQPoolConn *mqconn, void *data, Size len);
static void mn_consensus_message_cb(uint32 origin, const char *payload, Size payload_size);

static void mn_consensus_local_disconnect(MQPoolConn *mqconn);
static MQPoolConn *mn_pop_conn_for_request(uint64 req_id);
static void mn_add_conn_for_request(uint64 req_id, MQPoolConn *mqconn);


void
mn_consensus_start(uint32 local_node_id, const char *journal_schema,
				   const char *journal_relation, MNConsensusCallbacks *cbs)
{
	char	pool_name[128];
	MemoryContext	oldctx;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	am_server = true;
	MyNodeId = local_node_id;
	snprintf(pool_name, sizeof(pool_name), "%s%d", "mn_consensus", MyNodeId);
	MyMQPool = shm_mq_pooler_new_pool(pool_name, 100, recv_queue_size,
									  NULL, mn_consensus_local_disconnect,
									  mn_consensus_local_message_cb);

	msgb_startup("mn_consensus_msg", local_node_id, mn_consensus_message_cb,
				 recv_queue_size);

	MyRaftServer = raft_new_server(local_node_id, cbs->execute_request,
								   cbs->execute_query);
	MemoryContextSwitchTo(oldctx);
}

void
mn_consensus_shutdown(void)
{
	/* TODO */
}

void
mn_consensus_serialize_request(StringInfo s, MNConsensusRequest *req)
{
	pq_sendint64(s, req->req_id);
	pq_sendint(s, req->origin_id, 4);
	pq_sendint64(s, req->payload_length);

	if (req->payload_length)
		pq_sendbytes(s, req->payload, req->payload_length);
}

void
mn_consensus_deserialize_request(StringInfo s, MNConsensusRequest *req)
{
	req->req_id = pq_getmsgint64(s);
	req->origin_id = pq_getmsgint(s, 4);
	req->payload_length = pq_getmsgint64(s);

	if (req->payload_length)
	{
		req->payload = palloc(req->payload_length);
		pq_copymsgbytes(s, req->payload, req->payload_length);
	}
	else
		req->payload = NULL;
}

void
mn_consensus_serialize_response(StringInfo s, MNConsensusResponse *res)
{
	pq_sendint64(s, res->req_id);
	pq_sendint(s, res->status, 4);
	pq_sendint64(s, res->payload_length);

	if (res->payload_length)
		pq_sendbytes(s, res->payload, res->payload_length);
}

void
mn_consensus_deserialize_response(StringInfo s, MNConsensusResponse *res)
{
	res->req_id = pq_getmsgint64(s);
	res->status = pq_getmsgint(s, 4);
	res->payload_length = pq_getmsgint64(s);

	if (res->payload_length)
	{
		res->payload = palloc(res->payload_length);
		pq_copymsgbytes(s, res->payload, res->payload_length);
	}
	else
		res->payload = NULL;}

void
mn_consensus_serialize_query(StringInfo s, MNConsensusQuery *query)
{
	mn_consensus_serialize_request(s, query);
}

void
mn_consensus_deserialize_query(StringInfo s, MNConsensusQuery *query)
{
	mn_consensus_deserialize_request(s, query);
}

void
mn_consensus_serialize_result(StringInfo s, MNConsensusResult *res)
{
	mn_consensus_serialize_response(s, res);
}

void
mn_consensus_deserialize_result(StringInfo s, MNConsensusResult *res)
{
	mn_consensus_deserialize_response(s, res);
}


static void
mn_consensus_send_local_response(MQPoolConn *mqconn, MNConsensusResponse *res)
{
	StringInfoData	s;

	initStringInfo(&s);
	pq_sendint(&s, MNCONSENSUS_MSG_KIND_RESPONSE, 4);
	mn_consensus_serialize_response(&s, res);

	if (!shm_mq_pool_write(mqconn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("peer appears to have disconnected")));

	pfree(s.data);
}

void
mn_consensus_send_response(uint64 node_id, MNConsensusResponse *res)
{
	StringInfoData	s;

	initStringInfo(&s);
	mn_consensus_serialize_response(&s, res);

	mn_consensus_send_message(node_id,
							  MNCONSENSUS_MSG_KIND_RESPONSE, s.data,
							  s.len);

	pfree(s.data);
}

static void
mn_consensus_send_local_result(MQPoolConn *mqconn, MNConsensusResult *res)
{
	StringInfoData	s;

	initStringInfo(&s);
	pq_sendint(&s, MNCONSENSUS_MSG_KIND_RESULT, 4);
	mn_consensus_serialize_result(&s, res);

	if (!shm_mq_pool_write(mqconn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("peer appears to have disconnected")));

	pfree(s.data);
}

void
mn_consensus_send_result(uint64 node_id, MNConsensusResult *res)
{
	StringInfoData	s;

	initStringInfo(&s);
	mn_consensus_serialize_result(&s, res);

	mn_consensus_send_message(node_id,
							  MNCONSENSUS_MSG_KIND_RESULT, s.data,
							  s.len);

	pfree(s.data);
}

/*
 * From a non-consensus backend, submit a message to the consensus
 * and optionally return the consensus's reply.
 */
static MNConsensusStatus
mn_consensus_send_local_request(MQPoolConn *mqconn, MNConsensusRequest *req,
								bool nowait)
{
	StringInfoData			s;
	MNConsensusResponse		response;
	MNConsensusMessageKind	msgkind;

	initStringInfo(&s);
	pq_sendint(&s, MNCONSENSUS_MSG_KIND_REQUEST, 4);
	mn_consensus_serialize_request(&s, req);
	if (!shm_mq_pool_write(mqconn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("manager appears to have exited")));

	pfree(s.data);
	initStringInfo(&s);

	if (nowait)
		return MNCONSENSUS_IN_PROGRESS;

	if (!shm_mq_pool_receive(mqconn, &s, false))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("manager appears to have exited")));

	if (s.len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unexpected zero length reply")));

	msgkind = pq_getmsgint(&s, 4);
	Assert(msgkind == MNCONSENSUS_MSG_KIND_RESPONSE);

	mn_consensus_deserialize_response(&s, &response);
	Assert(response.req_id == req->req_id);

	return response.status;
}

/*
 * From a non-consensus backend, submit a message to the consensus
 * and return the consensus's reply.
 */
static MNConsensusResult*
mn_consensus_send_local_query(MQPoolConn *mqconn, MNConsensusQuery *query)
{
	StringInfoData			s;
	MNConsensusResult	   *result;
	MNConsensusMessageKind	msgkind;

	initStringInfo(&s);
	pq_sendint(&s, MNCONSENSUS_MSG_KIND_QUERY, 4);
	mn_consensus_serialize_query(&s, query);
	if (!shm_mq_pool_write(mqconn, &s))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("manager appears to have exited")));

	pfree(s.data);
	initStringInfo(&s);

	if (!shm_mq_pool_receive(mqconn, &s, false))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("manager appears to have exited")));

	if (s.len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unexpected zero length reply")));

	msgkind = pq_getmsgint(&s, 4);
	Assert(msgkind == MNCONSENSUS_MSG_KIND_RESULT);

	result = palloc(sizeof(MNConsensusResult));
	mn_consensus_deserialize_result(&s, result);

	return result;
}

/*
 * We got a message from a backend in the manager.
 */
static void
mn_consensus_local_message_cb(MQPoolConn *mqconn, void *data, Size len)
{
	StringInfoData		s;
	uint32				raft_leader_node;
	uint64				req_id;
	MNConsensusRequest	request;
	MNConsensusQuery	query;
	MNConsensusMessageKind	msgkind;

	raft_leader_node = raft_get_leader(MyRaftServer);

	wrapInStringInfo(&s, (char *) data, len);
	msgkind = pq_getmsgint(&s, 4);

	switch (msgkind)
	{
		case MNCONSENSUS_MSG_KIND_REQUEST:
		{
			mn_consensus_deserialize_request(&s, &request);
			req_id = request.req_id;

			break;
		}
		case MNCONSENSUS_MSG_KIND_QUERY:
		{
			mn_consensus_deserialize_query(&s, &query);
			req_id = request.req_id;

			break;
		}
		default:
			elog(ERROR, "unexpected message kind %u", msgkind);
			return; /* Quiet compiler. */
	}


	/* When there is no leader available, bail. */
	if (raft_leader_node == 0)
	{
		if (msgkind == MNCONSENSUS_MSG_KIND_REQUEST)
		{
			MNConsensusResponse	res = {0};
			res.req_id = req_id;
			res.status = MNCONSENSUS_NO_LEADER;
			mn_consensus_send_local_response(mqconn, &res);
		}
		else /* Has to be MNCONSENSUS_MSG_KIND_QUERY. */
		{
			MNConsensusResult	res = {0};
			res.req_id = req_id;
			res.status = MNCONSENSUS_NO_LEADER;
			mn_consensus_send_local_result(mqconn, &res);
		}
	}
	/* If the leader is the local ndoe we can just call it directly. */
	else if (MyNodeId == raft_leader_node)
	{
		mn_add_conn_for_request(req_id, mqconn);

		if (msgkind == MNCONSENSUS_MSG_KIND_REQUEST)
			raft_request(MyRaftServer, &request);
		else /* Has to be MNCONSENSUS_MSG_KIND_QUERY. */
			raft_query(MyRaftServer, &query);
	}
	/* Otherwise forward it to the leader. */
	else
	{
		mn_add_conn_for_request(req_id, mqconn);
		msgb_queue_message(raft_leader_node, data, len);
	}
}

void
mn_consensus_wakeup(struct WaitEvent *events, int nevents,
					long *max_next_wait_ms)
{
	static TimestampTz last_wakeup_ts = 0;

	msgb_wakeup_receive();

	/*
	 * This is a good chance for us to check if we've had any new messages
	 * submitted to us for processing.
	 */
	shm_mq_pooler_work(true);

	/* Also let Raft do it's processing */
	if (MyRaftServer)
	{
		TimestampTz current_ts = GetCurrentTimestamp() / 1000; /* miliseconds. */
		raft_wakeup(MyRaftServer, current_ts - last_wakeup_ts,
					max_next_wait_ms);
		last_wakeup_ts = current_ts;
	}

	/* And finally do any message processing. */
	msgb_wakeup_send(events, nevents, max_next_wait_ms);
}

void
mn_consensus_add_or_update_node(uint32 nodeid, const char *dsn, bool update_if_exists)
{
	if (!raft_add_node(MyRaftServer, nodeid))
	{
		if (update_if_exists)
			msgb_update_peer(nodeid, dsn);
		else
			elog(ERROR, "node %u already exists", nodeid);
	}
	else
		msgb_add_peer(nodeid, dsn);

}

void
mn_consensus_remove_node(uint32 nodeid)
{
	raft_remove_node(MyRaftServer, nodeid);
}

void
mn_consensus_send_remote_message(uint32 target, MNConsensusMessageKind msgkind,
								 const char *data, Size len)
{
	StringInfoData			s;

	initStringInfo(&s);

	pq_sendint(&s, msgkind, 4);
	pq_sendbytes(&s, data, len);

	msgb_queue_message(target, s.data, s.len);

	pfree(s.data);
}

static void
mn_consensus_process_message(uint32 origin, MNConsensusMessageKind msgkind,
							 StringInfo s)
{
	switch (msgkind)
	{
		case MNCONSENSUS_MSG_KIND_REQUEST:
		{
			MNConsensusRequest		request;

			mn_consensus_deserialize_request(s, &request);

			/*
			 * Note Raft will handle the situation correctly if this node
			 * is not leader.
			 */
			raft_request(MyRaftServer, &request);

			break;
		}
		case MNCONSENSUS_MSG_KIND_RESPONSE:
		{
			MNConsensusResponse		response;
			MQPoolConn			   *mqconn;

			mn_consensus_deserialize_response(s, &response);
			mqconn = mn_pop_conn_for_request(response.req_id);

			/* Bail if client is no longer listening for answer? */
			if (!mqconn)
				return;

			mn_consensus_send_local_response(mqconn, &response);
			break;
		}

		case MNCONSENSUS_MSG_KIND_QUERY:
		{
			MNConsensusQuery		query;

			mn_consensus_deserialize_query(s, &query);
			/*
			 * Note Raft will handle the situation correctly if this node
			 * is not leader.
			 */
			raft_query(MyRaftServer, &query);

			break;
		}
		case MNCONSENSUS_MSG_KIND_RESULT:
		{
			MNConsensusResult		result;
			MQPoolConn			   *mqconn;

			mn_consensus_deserialize_result(s, &result);
			mqconn = mn_pop_conn_for_request(result.req_id);

			/* Bail if client is no longer listening for answer? */
			if (!mqconn)
				return;

			mn_consensus_send_local_result(mqconn, &result);
			break;
		}

		/* Let Raft handle Raft messages. */
		case MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE:
		case MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE_RES:
		case MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES:
		case MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES_RES:
			raft_received_message(MyRaftServer, origin, msgkind, s);
			break;
		default:
			elog(ERROR, "unknown message kind %u", msgkind);
	}
}

static void
mn_consensus_message_cb(uint32 origin, const char *data, Size len)
{
	StringInfoData		s;
	MNConsensusMessageKind	msgkind;

	if (len < 4)
		elog(ERROR, "expected message size of at least 4 bytes, got %zu",
			 len);

	wrapInStringInfo(&s, (char *) data, len);

	msgkind = pq_getmsgint(&s, 4);

	mn_consensus_process_message(origin, msgkind, &s);
}

void
mn_consensus_send_message(uint32 target, MNConsensusMessageKind msgkind,
						  const char *data, Size len)
{
	if (target == MyNodeId)
	{
		StringInfoData		s;
		wrapInStringInfo(&s, (char *) data, len);
		mn_consensus_process_message(MyNodeId, msgkind, &s);
	}
	else
	{
		mn_consensus_send_remote_message(target, msgkind, data, len);
	}
}

static void
mn_consensus_local_disconnect(MQPoolConn *mqconn)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, &reqconns)
	{
		RequestConn	   *reqconn = dlist_container(RequestConn, node, iter.cur);

		if (reqconn->mqconn == mqconn)
		{
			dlist_delete(iter.cur);
			pfree(reqconn);
		}
	}
}

static MQPoolConn *
mn_pop_conn_for_request(uint64 req_id)
{
	dlist_mutable_iter	iter;
	MQPoolConn		   *mqconn;

	dlist_foreach_modify(iter, &reqconns)
	{
		RequestConn	   *reqconn = dlist_container(RequestConn, node, iter.cur);

		if (reqconn->req_id == req_id)
		{
			mqconn = reqconn->mqconn;
			dlist_delete(iter.cur);
			return mqconn;
		}
	}

	return NULL;
}

static void
mn_add_conn_for_request(uint64 req_id, MQPoolConn *mqconn)
{
	RequestConn	   *reqconn = palloc(sizeof(RequestConn));

	reqconn->req_id = req_id;
	reqconn->mqconn = mqconn;

	dlist_push_tail(&reqconns, &reqconn->node);
}

/*
 * Send request to the consensus module
 *
 * This is blocking API.
 */
MNConsensusStatus
mn_consensus_request(uint32 origin_id, char *data, Size len)
{
	MQPool				   *mqpool;
	MNConsensusRequest		req;
	MNConsensusStatus		status;
	char    pool_name[128];

	snprintf(pool_name, sizeof(pool_name), "%s%d", "mn_consensus", origin_id);
	mqpool = shm_mq_pool_get_pool(pool_name);
	if (!mqpool)
		ereport(ERROR, (errmsg("could not get mn_consensus pool")));
	MyMQConn = shm_mq_pool_get_connection(mqpool, false);
	if (!MyMQConn)
		ereport(ERROR, (errmsg("could not get mn_consensus connection")));

	/* TODO */
	req.req_id = random();
	req.origin_id = origin_id;
	req.payload = data;
	req.payload_length = len;

	status = mn_consensus_send_local_request(MyMQConn, &req, false);

	return status;
}

/*
 * Send request to the consensus module
 *
 * This is non-blocking API.
 */
void
mn_consensus_request_enqueue(uint32 origin_id, char *data, Size len)
{
	MQPool				   *mqpool;
	MNConsensusRequest		req;
	char    pool_name[128];

	snprintf(pool_name, sizeof(pool_name), "%s%d", "mn_consensus", origin_id);
	mqpool = shm_mq_pool_get_pool(pool_name);
	if (!mqpool)
		ereport(ERROR, (errmsg("could not get mn_consensus pool")));
	MyMQConn = shm_mq_pool_get_connection(mqpool, false);
	if (!MyMQConn)
		ereport(ERROR, (errmsg("could not get mn_consensus connection")));

	req.req_id = random(); /* TODO */
	req.origin_id = origin_id;
	req.payload = data;
	req.payload_length = len;

	mn_consensus_send_local_request(MyMQConn, &req, true);
}

MNConsensusResult *
mn_consensus_query(uint32 origin_id, char *data, Size len)
{
	MQPool				   *mqpool;
	MNConsensusQuery			query;
	MNConsensusResult	   *res;
	char    pool_name[128];

	snprintf(pool_name, sizeof(pool_name), "%s%d", "mn_consensus", origin_id);
	mqpool = shm_mq_pool_get_pool(pool_name);
	if (!mqpool)
		ereport(ERROR, (errmsg("could not get mn_consensus pool")));
	MyMQConn = shm_mq_pool_get_connection(mqpool, false);
	if (!MyMQConn)
		ereport(ERROR, (errmsg("could not get mn_consensus connection")));

	/* TODO */
	query.req_id = random();
	query.origin_id = origin_id;
	query.payload = data;
	query.payload_length = len;

	res = mn_consensus_send_local_query(MyMQConn, &query);

	return res;
}

char *
mn_consensus_status_to_str(MNConsensusStatus status)
{
	switch (status)
	{
		case MNCONSENSUS_IN_PROGRESS:
			return "IN_PROGRESS";
		case MNCONSENSUS_EXECUTED:
			return "EXECUTED";
		case MNCONSENSUS_FAILED:
			return "FAILED";
		case MNCONSENSUS_NO_LEADER:
			return "NO_LEADER";
		default:
			elog(ERROR, "unknown status %u", status);
			return NULL;
	}
}

MNConsensusStatus
mn_consensus_status_from_str(const char *status)
{
	if (strcmp(status, "IN_PROGRESS") == 0)
		return MNCONSENSUS_IN_PROGRESS;
	if (strcmp(status, "EXECUTED") == 0)
		return MNCONSENSUS_EXECUTED;
	if (strcmp(status, "FAILED") == 0)
		return MNCONSENSUS_FAILED;
	if (strcmp(status, "NO_LEADER") == 0)
		return MNCONSENSUS_NO_LEADER;

	elog(ERROR, "unknown status %s", status);
	return MNCONSENSUS_FAILED;
}

bool
mn_consensus_want_waitevent_rebuild(void)
{
	return msgb_want_waitevent_rebuild();
}

int
mn_consensus_wait_event_count(void)
{
	return msgb_wait_event_count();
}

void
mn_consensus_add_events(WaitEventSet *weset, int nevents)
{
	return msgb_add_events(weset, nevents);
}
