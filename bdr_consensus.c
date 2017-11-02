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

#include "miscadmin.h"

#include "pgstat.h"

#include "storage/proc.h"

#include "utils/builtins.h"
#include "utils/memutils.h"

#include "pglogical_plugins.h"

#include "mn_consensus.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_join.h"
#include "bdr_state.h"
#include "bdr_worker.h"

void bdr_consensus_main(Datum main_arg);

static void bdr_consensus_execute_request(MNConsensusRequest *request, MNConsensusResponse *res);
static bool bdr_consensus_execute_query(MNConsensusQuery *query, MNConsensusResult *res);

static MNConsensusCallbacks consensus_callbacks = {
	bdr_consensus_execute_request,
	bdr_consensus_execute_query
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

static void bdr_consensus_worker_refresh_nodes(void);

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
	BackgroundWorker	bgw;
	BackgroundWorkerHandle *bgw_handle;

	/*
	 * Node isn't ready for consensus manager startup yet. If it's during join,
	 * the join process will make it start later on.
	 */
	if (cur_state < BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS)
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 "bdr");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "bdr_consensus_main");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "bdr consensus worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = UInt32GetDatum(MyDatabaseId);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(ERROR,
			(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
			 errmsg("worker registration failed, increase max_worker_processes setting")));
	}
}

static WaitEventSet *
bdr_consensus_rebuild_waitevents(void)
{
	WaitEventSet   *weset;
	int				nevents = mn_consensus_wait_event_count();

	weset = CreateWaitEventSet(TopMemoryContext, nevents + 2);

	AddWaitEventToSet(weset, WL_LATCH_SET, PGINVALID_SOCKET,
					  &MyProc->procLatch, NULL);
	AddWaitEventToSet(weset, WL_POSTMASTER_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	mn_consensus_add_events(weset, nevents);

	return weset;
}

void
bdr_consensus_main(Datum main_arg)
{
	WaitEventSet   *mainloop_waits = NULL;
	Oid				dboid = DatumGetUInt32(main_arg);
	long			max_next_sleep = 60000; /* 60s wait. */

	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid);

	StartTransactionCommand();

	bdr_refresh_cache_local_nodeinfo();

	mn_consensus_start(bdr_get_local_nodeid(), BDR_SCHEMA_NAME,
					   BDR_MSGJOURNAL_REL_NAME, &consensus_callbacks);

	bdr_consensus_worker_refresh_nodes();

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bdr consensus worker");

	CommitTransactionCommand();

	mainloop_waits = bdr_consensus_rebuild_waitevents();

	for (;;)
	{
		WaitEvent	event;
		int			nevents;

		nevents = WaitEventSetWait(mainloop_waits, max_next_sleep, &event,
								   1, PG_WAIT_EXTENSION);

		ResetLatch(&MyProc->procLatch);

		if (event.events & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		if (!bdr_is_active_db())
			return;

		/* TODO: optimize. */
		StartTransactionCommand();
		bdr_consensus_worker_refresh_nodes();
		CommitTransactionCommand();

		max_next_sleep = 60000;
		mn_consensus_wakeup(&event, nevents, &max_next_sleep);

		if (mn_consensus_want_waitevent_rebuild())
		{
			FreeWaitEventSet(mainloop_waits);
			mainloop_waits = bdr_consensus_rebuild_waitevents();
		}
	}
}

void
bdr_consensus_refresh_nodes(BdrNodeState cur_state)
{
	/*
	 * Node isn't ready for consensus manager yet. If it's during join, the
	 * join process will start and refresh later on.
	 */
	if (cur_state < BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS)
		return;
}

static void
bdr_consensus_worker_refresh_nodes(void)
{
	bool txn_started = false;
	List	   *nodes;
	ListCell   *lc;

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

MNConsensusStatus
bdr_consensus_request(BdrMessage *msg)
{
	StringInfoData		s;
	MNConsensusStatus	status;

	elog(bdr_debug_level, "%u sending consensus request %s",
		 bdr_get_local_nodeid(), bdr_message_type_to_string(msg->message_type));

	initStringInfo(&s);
	bdr_serialize_message(&s, msg);

	status = mn_consensus_request(bdr_get_local_nodeid(), s.data, s.len);

	elog(bdr_debug_level, "%u request result %s",
		 bdr_get_local_nodeid(), mn_consensus_status_to_str(status));

	pfree(s.data);

	return status;
}

void
bdr_consensus_request_enqueue(BdrMessage *msg)
{
	StringInfoData		s;

	elog(bdr_debug_level, "%u enqueuing consensus request %s",
		 bdr_get_local_nodeid(), bdr_message_type_to_string(msg->message_type));

	initStringInfo(&s);
	bdr_serialize_message(&s, msg);

	mn_consensus_request_enqueue(bdr_get_local_nodeid(), s.data, s.len);

	pfree(s.data);
}

MNConsensusResult *
bdr_consensus_query(BdrMessage *msg)
{
	StringInfoData		s;
	MNConsensusResult  *res;

	elog(bdr_debug_level, "%u sending consensus query %s",
		 bdr_get_local_nodeid(), bdr_message_type_to_string(msg->message_type));

	initStringInfo(&s);
	bdr_serialize_message(&s, msg);

	res = mn_consensus_query(bdr_get_local_nodeid(), s.data, s.len);

	pfree(s.data);

	return res;
}

static void
bdr_consensus_execute_request(MNConsensusRequest *request, MNConsensusResponse *res)
{
	BdrMessage	   *bmsg;
	StringInfoData	s;

	/* Start transaction early to keep all allocations local. */
	StartTransactionCommand();

	wrapInStringInfo(&s, request->payload, request->payload_length);

	bmsg = msg_deserialize_message(&s);

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
			break;
	}

	CommitTransactionCommand();

	res->status = MNCONSENSUS_EXECUTED;
}

static bool
bdr_consensus_execute_query(MNConsensusQuery *query, MNConsensusResult *res)
{
	return false;
}

/*
 * Size of the BdrMessage header when sent as part of a ConsensusMessage
 * payload. Most of the BdrMessage gets copied from the ConsensusMessage
 * so there's only a small unique part: currently the message type
 */
#define SizeOfBdrMsgHeader (sizeof(uint32))

/*
 * Serialize a BDR message for submission to the consensus
 * system or over shmem. This is the BDR specific header that'll
 * be added to consensus messages before the payload.
 *
 * No length word is included; it's expected that the container wrapping this
 * message payload will provide length information.
 */
void
bdr_serialize_message(StringInfo out, BdrMessage *msg)
{
	const MNMessageFuncs *funcs;

	resetStringInfo(out);
	Assert(out->len == 0);

	funcs = message_funcs_lookup(msg->message_type);

	pq_sendint(out, msg->message_type, 4);
	pq_sendint(out, msg->origin_id, 4);

	if (funcs != NULL)
		funcs->serialize(out, msg);
}

/*
 * Deserialization of a BdrMessage copies fields from the ConsensusMessage it
 * came from into the BdrMessage header, so it needs more than the
 * ConsensusMessage's payload.
 *
 * Thus this isn't an exact mirror of bdr_serialize_message.
 */
BdrMessage *
msg_deserialize_message(StringInfo s)
{
	BdrMessageType	message_type;
	uint32			origin_id;
	BdrMessage	   *out;
	const MNMessageFuncs *funcs;

	if (s->len < 4)
	{
		elog(LOG, "ignored bad bdr message: consensus message missing payload");
		return NULL;
	}

	message_type = pq_getmsgint(s, 4);
	origin_id = pq_getmsgint(s, 4);

	funcs = message_funcs_lookup(message_type);

	if (funcs == NULL)
	{
		out = palloc(sizeof(BdrMessage));
	}
	else
	{
		ErrorContextCallback myerrcontext;
		struct bdr_msg_errcontext ctxinfo;

		ctxinfo.msgtype = message_type;
		ctxinfo.msgdata = s;
		myerrcontext.callback = deserialize_msg_errcontext_callback;
		myerrcontext.arg = &ctxinfo;
		myerrcontext.previous = error_context_stack;
		error_context_stack = &myerrcontext;

		out = funcs->deserialize(s);

		error_context_stack = myerrcontext.previous;
	}

	out->message_type = message_type;
	out->origin_id = origin_id;

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

	/* We MUST fail gracefully here due to downgrades etc */
	initStringInfo(&si);
	appendStringInfo(&si, "(unrecognised BDR message type %d)", msgtype);
	return si.data;
}
