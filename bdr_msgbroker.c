/*-------------------------------------------------------------------------
 *
 * bdr_msgbroker.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_msgbroker.c
 *
 * libpq-based asynchronous messaging
 *-------------------------------------------------------------------------
 *
 * The message broker runs in the manager worker, maintaining connections to
 * all currently alive and reachable peer manager workers. It asynchronously
 * dispatches messages and processes replies from each worker.
 *
 * The message broker doesn't remember anything and has no persistent state.
 * It doesn't guarantee message reliability, we overlay a distributed
 * consensus protocol on top for that. See bdr_consensus.c.
 *
 * The message broker tries to be independent of the rest of BDR; we want to be
 * able to plug in an alternative transport, and/or re-use this for other
 * products.
 *
 * Messages are delivered to peers using libpq connections that make
 * function calls on the other end. A log filter hook suppresses them
 * from the statement logs. The functions deliver message payloads
 * to the message broker via a shmem queue, which is read by the
 * broker when the hosting process (bdr manager, in this case)'s latch
 * is set. We need one shm_mq per connected peer.
 */
#include "postgres.h"

#include "catalog/pg_type.h"

#include "fmgr.h"

#include "nodes/pg_list.h"

#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"

#include "utils/elog.h"
#include "utils/memutils.h"

#include "libpq-fe.h"

#include "bdr_msgbroker.h"

typedef struct MsgbMessageBuffer
{
	int msgid;
	enum MsgbSendStatus send_status;
	Size payload_size;
	char payload[FLEXIBLE_ARRAY_MEMBER];
} MsgbMessageBuffer;

typedef struct MsgbConnection
{
	uint32 destination_id;
	char *dsn;
	PGconn *pgconn;
	int wait_set_index;				/* wait-set index or -1 if none assigned */
	int msgb_msgid_counter;			/* next message ID to allocate */
	MemoryContext queue_context;	/* memory context for MsgbMessageBuffer */
	List *send_queue;				/* List of MsgbMessageBuffer */
	/* TODO: need backoff timer */
} MsgbConnection;

msgb_received_hook_type msgb_received_hook = NULL;
msgb_recreate_wait_event_set_hook_type msgb_recreate_wait_event_set_hook = NULL;

/* The wait event set we maintain all our sockets in */
static WaitEventSet *wait_set = NULL;

/* connection and peer state */
static MsgbConnection *conns = NULL;
static int max_conns;

static bool atexit_registered = false;
static MemoryContext msgbuf_context = NULL;

/* TODO need shm_mq set here for all peers */

/*
 * When this process is a normal user backend that's acting as the delivery side
 * of a message broker connection, the origin node id of the connected node,
 * otherwise 0.
 */
static uint32 origin_node = 0;

/*
 * Are any connections pending and not yet registered in the wait-list
 * (sockets not created yet)?
 */
static bool conns_polling = false;

static void msgb_remove_destination_by_index(int index);
static void msgb_atexit(int code, Datum arg);
static void msgb_recreate_wait_event_set(void);
static void msgb_start_connect(MsgbConnection *conn);
static void msgb_continue_async_connect(MsgbConnection *conn);
static void msgb_finish_connect(MsgbConnection *conn);
static void msgb_register_wait_event(MsgbConnection *conn, int initial_flags);
static void msgb_clear_bad_connection(MsgbConnection *conn);
static int msgb_send_pending(MsgbConnection *conn);
static int msgb_recv_pending(MsgbConnection *conn);
static int msgb_idx_for_destination(uint32 destination, const char *errm_action);
static int msgb_flush_conn(MsgbConnection *conn);

inline void ensure_received_hook(void)
{
	if (msgb_received_hook == NULL)
		ereport(ERROR, (errmsg_internal("no message hook is registered")));
}

/*
 * In a normal user backend, attach to the manager's shmem queue for
 * the connecting peer and prepare to send messages.
 */
PG_FUNCTION_INFO_V1(msgb_connect);

Datum
msgb_connect(PG_FUNCTION_ARGS)
{
	uint32 origin_node = PG_GETARG_UINT32(0);
	uint32 destination_node = PG_GETARG_UINT32(1);

	if (origin_node == 0 || destination_node == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg_internal("peer node sent origin node or destinationnode with id 0")));


	bdr_ensure_active_db();

	bdr_cache_local_nodeinfo();

	if (destination_node != bdr_get_local_node_id())
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("peer %d expected to connect to our node with id %d but we are node %d",
				 		origin_node, destination_node, bdr_get_local_node_id())));

	/* TODO find shmem mq and attach */

	elog(ERROR, "not implemented");

	elog(DEBUG1, "peer %d connected message queue", origin_node);
}


/*
 * SQL-callable to deliver a message to the message broker on the receiving
 * side.
 *
 * Uses a message buffer connection already established by msgb_connect.
 */
PG_FUNCTION_INFO_V1(msgb_deliver_message);

Datum
msgb_deliver_message(PG_FUNCTION_ARGS)
{
	uint32 destination_node = PG_GETARG_UINT32(0);
	int message_id = PG_GETARG_INT32(1);
	bytea *payload = PG_GETARG_BYTEA_PP(2);

	/* TODO */

	elog(ERROR, "not implemented");
}

/*
 * Start up the message broker.
 *
 * We will register all our sockets in the passed wait-event set, and set it as
 * the active set that will be maintained by future add/remove destination
 * operations. Only one wait event set may be maintained at a time.
 */
void
msgb_startup(int max_connections)
{
	int i;

	/*
	 * Wait event sets lack support for removing or replacing socket,
	 * so we must be able to re-create it.
	 */
	if (msgb_recreate_wait_event_set_hook == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("caller must install msgb_recreate_wait_event_set_hook")));

	if (!atexit_registered)
		on_proc_exit(msgb_atexit, (Datum)0);

	if (msgbuf_context != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("msgbroker already running")));

	if (max_conns <= 0)
		ereport(ERROR,
				(errmsg_internal("max conns must be positive")));

	max_conns = max_connections;

	msgb_recreate_wait_event_set();

	msgbuf_context = AllocSetContextCreate(TopMemoryContext,
										   "msgbroker context",
										   ALLOCSET_DEFAULT_SIZES);

	conns = MemoryContextAlloc(msgbuf_context,
							   sizeof(MsgbConnection) * max_connections);


	memset(conns, 0, sizeof(MsgbConnection) * max_connections);

	for (i = 0; i < max_connections; i++)
	{
		conns[i].wait_set_index = -1;
		/* msgid 0 is reserved for setup */
		conns[i].msgb_msgid_counter = 1;
	}
}

/*
 * Clean up the old connection, destroy any wait event, and schedule it for
 * reconnection. Mark any pending messages not confirmed delivered as queued
 * so we redeliver.
 *
 * We preserve the queue and counter, we're not throwing away messages here,
 * just trying to re-establish a connection.
 *
 * TODO: should update a random backoff timer here
 */
static void
msgb_clear_bad_connection(MsgbConnection *conn)
{
	ListCell *lc;

	if (conn->pgconn)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("connection to peer %d went down: %s",
						conn->destination_id, PQerrorMessage(conn->pgconn))));
		PQfinish(conn->pgconn);
		conn->pgconn = NULL;
	}

	foreach (lc, conn->send_queue)
	{
		MsgbMessageBuffer *msg = lfirst(lc);
		if (msg->send_status != MSGB_MSGSTATUS_DELIVERED)
			msg->send_status = MSGB_MSGSTATUS_QUEUED;
	}

	if (conn->wait_set_index != -1)
		msgb_recreate_wait_event_set();
}

/*
 * Tell libpq to start connecting to a node. Most of the actual
 * work is done in PQconnectPoll in msgb_continue_async_connect(...)
 */
static void
msgb_start_connect(MsgbConnection *conn)
{
	int status;

	Assert(conn->pgconn == NULL);
	Assert(conn->destination_id != 0);
	Assert(conn->wait_set_index == -1);
	Assert(conn->dsn != NULL);

	conn->pgconn = PQconnectStart(conn->dsn);
	if (conn->pgconn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg_internal("out of memory")));

	status = PQstatus(conn->pgconn);
	if (status == CONNECTION_BAD)
		msgb_clear_bad_connection(conn);

	/*
	 * Even if PQsocket(conn) would return a socket here, we shouldn't add it
	 * to the wait list and use wait events yet. libpq may drop a connection
	 * multiple times and re-create the connection, say due to mismatches in
	 * protocol or sslmode.
	 *
	 * So we need to keep polling the connection.
	 */
}

/*
 * Continue a libpq asynchronous connection attempt
 * per https://www.postgresql.org/docs/current/static/libpq-connect.html
 *
 * Must have a valid PGconn for which PQstatus has returned something
 * something other than CONNECTION_BAD
 *
 * TODO: set up an errcontext here
 */
static void
msgb_continue_async_connect(MsgbConnection *conn)
{
	int pollstatus = PQconnectPoll(conn->pgconn);

	switch (pollstatus)
	{
		case PGRES_POLLING_OK:
			msgb_finish_connect(conn);
		case PGRES_POLLING_READING:
		case PGRES_POLLING_WRITING:
			/* nothing to do but wait until socket readable/writeable again */
			break;
		case PGRES_POLLING_FAILED:
			msgb_clear_bad_connection(conn);	
			break;
	}
}

/*
 * Establish our shared memory queue connection between our backend on the peer
 * and the peer's manager worker by calling a SQL-callable function on the
 * backend.
 */
static int
msgb_peer_connect(MsgbConnection *conn)
{
	int ret;
	MsgbMessageBuffer *buf;
	const int nParams = 2;
	Oid paramTypes[2] = { OIDOID, OIDOID };
	const char * paramValues[2];
	char destination_id[30];
	char origin_id[30];
	MemoryContext old_mctx;

	snprintf(destination_id, 30, "%u", conn->destination_id);
	paramValues[0] = destination_id;
	snprintf(origin_id, 30, "%u", bdr_get_local_node_id());
	paramValues[1] = origin_id;

	/*
	 * Pop a dummy message onto the head of the queue so we can
	 * process it using the normal message handling logic.
	 */
	old_mctx = MemoryContextSwitchTo(conn->queue_context);
	buf = palloc(sizeof(MsgbMessageBuffer));
	/* msgid 0 is only used for this setup message */
	buf->msgid = 0;
	buf->payload_size = 0;
	buf->send_status = MSGB_MSGSTATUS_QUEUED;
	conns->send_queue = lcons(buf, conn->send_queue);
	(void) MemoryContextSwitchTo(old_mctx);

	ret = PQsendQueryParams(conn->pgconn, "bdr.msgb_connect($1, $2, $3)", nParams,
							paramTypes, paramValues, NULL, NULL, 0);

	if (!ret)
	{
		buf->send_status = MSGB_MSGSTATUS_SENDING;
		/* always wants read, maybe more write too */
		return msgb_flush_conn(conn);
	}
	else
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to dispatch message to %u: %s",
						conn->destination_id, PQerrorMessage(conn->pgconn))));
		/* Assume bad connection */
		msgb_clear_bad_connection(conn);
		return 0;
	}

}

/*
 * Once a connection is fully established we can switch from polling to wait
 * events for handling the connection.
 */
static void
msgb_finish_connect(MsgbConnection *conn)
{
	int initial_wait_flags;

	Assert(conn->pgconn != NULL);
	Assert(conn->destination_id != 0);
	Assert(conn->wait_set_index == -1);
	Assert(conn->dsn != NULL);

	if (PQsetnonblocking(conn->pgconn, 1) != 0)
	{
		/* shouldn't happen */
		elog(WARNING, "failed to put connection in non-blocking mode: %s",
					  PQerrorMessage(conn->pgconn));
		msgb_clear_bad_connection(conn);
	}

	/*
	 * Now we have to attach to the shmem memory queue on the other end and
	 * ensure it's ready to process messages, so dispatch the query for that...
	 */
	initial_wait_flags = msgb_peer_connect(conn);

	/* And begin event based processing */
	msgb_register_wait_event(conn, initial_wait_flags);
}

static void
msgb_register_wait_event(MsgbConnection *conn, int initial_wait_flags)
{
	Assert(conn->pgconn != NULL);
	Assert(conn->destination_id != 0);
	Assert(conn->wait_set_index == -1);

	/*
	 * Register the connection in our wait event set.
	 *
	 * We'll stop polling the connection when we see a wait_set_index for it.
	 */
	conn->wait_set_index = AddWaitEventToSet(wait_set,
											 initial_wait_flags,
											 PQsocket(conn->pgconn),
											 NULL, (void*)conn);

	/* AddWaitEventToSet Will elog(...) on failure */
	Assert(conn->wait_set_index != -1);
}

/*
 * Flush pending data from libpq, if any, and reset connection state on error.
 *
 * Returns WL_SOCKET_WRITEABLE|WL_SOCKET_READABLE if more data is still pending to be sent,
 * since we might also have to consume input to let the server read from its own recv
 * buffer and free up ours.
 *
 * If there's nothing more to send, return WL_SOCKET_READABLE on the assumption we
 * expect a reply.
 */
static int
msgb_flush_conn(MsgbConnection *conn)
{
	int ret = PQflush(conn->pgconn);
	if (ret == -1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to flush output to %u: %s",
						conn->destination_id, PQerrorMessage(conn->pgconn))));
		/* Assume bad connection */
		msgb_clear_bad_connection(conn);
		return 0;
	}
	else if (ret == 1)
		return WL_SOCKET_WRITEABLE|WL_SOCKET_READABLE;
	else
		return WL_SOCKET_READABLE;
}

/*
 * A query finished on one of our connections and it's time to process the
 * results. This won't be a new message, it's confirmation of delivery of a
 * message we sent.
 *
 * Returns true if more results expected so we must keep reading.
 */
static bool
msgb_process_result(MsgbConnection *conn)
{
	PGresult *res = PQgetResult(conn->pgconn);
	MsgbMessageBuffer *buf;

	if (res == NULL)
	{
		/* no more results pending */
		return false;
	}

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("while getting result from node %u: %s",
				 		conn->destination_id, PQerrorMessage(conn->pgconn))));
		PQclear(res);
		msgb_clear_bad_connection(conn);
		return false;
	}

	/* Our functions return void */
	Assert(PQntuples(res) == 0);
	Assert(PQnfields(res) == 0);

	/* Message delivered */
	buf = linitial(conn->send_queue);
	Assert(buf->send_status = MSGB_MSGSTATUS_SENDING);
	conn->send_queue = list_delete_first(conn->send_queue);
	pfree(buf);

	PQclear(res);

	/* PQgetResult didn't return NULL, might be more results */
	return true;
}

/*
 * Process readable replies on a socket. This isn't for receiving messages,
 * but for processing results of message deliveries.
 *
 * Receiving messages is done via shm_mq from function invocations in user
 * backends.
 *
 * Returns a flag set: WL_SOCKET_READABLE if more input still needed from
 * the socket and WL_SOCKET_WRITEABLE if there's pending output in libpq's
 * buffer.
 */
static int
msgb_recv_pending(MsgbConnection *conn)
{
	if (PQconsumeInput(conn->pgconn) == 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("while receiving from node %u: %s",
				 		conn->destination_id, PQerrorMessage(conn->pgconn))));
		msgb_clear_bad_connection(conn);
		return 0;
	}
	else
	{
		bool need_recv_more = true;

		while (need_recv_more && !PQisBusy(conn->pgconn))
			need_recv_more = msgb_process_result(conn);

		/* always marks socket want-read, maybe write too */
		return msgb_flush_conn(conn);
	}
}

/*
 * Put the next message on the send queue into libpq's send buffer and flush
 * as much of it as the send buffer will hold.
 *
 * Returns needed wait event flags, based on whether the buffer could be
 * flushed or not.
 */
static int
msgb_send_next(MsgbConnection *conn)
{
	MsgbMessageBuffer *buf;
	const int nParams = 3;
	Oid paramTypes[3] = { OIDOID, INT4OID, BYTEAOID };
	const char * paramValues[3];
	char destination_id[30];
	char msgid_str[30];
	size_t escapedLen;
	int ret;

	const char * const msg_send_query = "SELECT msgb_deliver_message($1, $2, $3)";

	Assert(conn->send_queue != NIL);
	buf = linitial(conn->send_queue);
	Assert(buf->send_status == MSGB_MSGSTATUS_QUEUED);

	snprintf(destination_id, 30, "%u", conn->destination_id);
	paramValues[0] = destination_id;
	snprintf(msgid_str, 30, "%d", buf->msgid);
	paramValues[1] = msgid_str;
	paramValues[2] = (char*) PQescapeByteaConn(conn->pgconn,
											   (const unsigned char *)buf->payload,
											   buf->payload_size, &escapedLen);

	if (paramValues[2] == NULL)
		ereport(ERROR,
				(errmsg("bytea formatting failed: %s", PQerrorMessage(conn->pgconn))));

	/* TODO: prepared statements */
	ret = PQsendQueryParams(conn->pgconn, msg_send_query, nParams, paramTypes,
							paramValues, NULL, NULL, 0);
	if (!ret)
	{
		buf->send_status = MSGB_MSGSTATUS_SENDING;
		/* always wants read, maybe more write too */
		return msgb_flush_conn(conn);
	}
	else
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to dispatch message to %u: %s",
						conn->destination_id, PQerrorMessage(conn->pgconn))));
		/* Assume bad connection */
		msgb_clear_bad_connection(conn);
		return 0;
	}
}

/*
 * The socket became writeable, so send any current libpq buffer.
 *
 * If there's no query on the wire and none waiting, ignore further
 * socket-writeable events.
 */
static int
msgb_send_pending(MsgbConnection *conn)
{
	if (conn->send_queue == NIL)
	{
		/*
		 * We only pop messages off the queue once confirmed
		 * delivered, so there must be nothing pending. We still
		 * want to hear about it if the server asynchronously
		 * notifies us of something.
		 */
		return WL_SOCKET_READABLE;
	}
	else
	{
		MsgbMessageBuffer *buf = linitial(conn->send_queue);
		if (buf->send_status == MSGB_MSGSTATUS_SENDING)
			return msgb_flush_conn(conn);
		else if (buf->send_status == MSGB_MSGSTATUS_QUEUED)
			return msgb_send_next(conn);
		else
			ereport(ERROR, (errmsg_internal("unexpected send status %d", buf->send_status)));
	}
}

/*
 * TODO: set up an errcontext here
 */
void
msgb_service_connections(WaitEvent *occurred_events, int nevents)
{
	bool new_conns_polling = false;
	int i;

	ensure_received_hook();

	if (nevents == 0 && !conns_polling)
		return;
	
	for (i = 0; i < nevents; i++)
	{
		WaitEvent const *e = &occurred_events[i];
		MsgbConnection *conn = e->user_data;
		int new_wait_flags = 0;
		if (e->events & WL_SOCKET_READABLE)
			new_wait_flags |= msgb_recv_pending(conn);
		if (e->events & WL_SOCKET_WRITEABLE)
			new_wait_flags |= msgb_send_pending(conn);
		if (conn->pgconn != NULL)
			ModifyWaitEvent(wait_set, conn->wait_set_index,
				new_wait_flags, NULL);
	}

	if (conns_polling)
	{
		for (i = 0; i < max_conns; i++)
		{
			MsgbConnection *conn = NULL;

			/*
			 * connection removal is done immediately, so
			 * can't be any maintenance tasks here.
			 */
			if (conn->destination_id == 0)
				continue;

			/*
			 * A connection died and needs to be re-established. Start
			 * connecting and wait for a socket to be available.
			 */
			if (conn->pgconn == NULL)
			{
				msgb_start_connect(conn);
				new_conns_polling = true;
				break;
			}

			switch (PQstatus(conn->pgconn))
			{
				case CONNECTION_OK:
					if (conn->wait_set_index == -1)
					{
						msgb_finish_connect(conn);
						Assert(conn->wait_set_index != -1);
					}
					break;
					
				case CONNECTION_BAD:
					/*
					 * failed, must ensure wait event cleared and reconnect
					 * next time around.
					 */
					msgb_clear_bad_connection(conn);
					new_conns_polling = true;
					break;
				default:
					/*
					 * All other states are async connect progress states
					 * where we must continue to PQconnectPoll(...)
					 */
					msgb_continue_async_connect(conn);
					/* Might've finished establishing connection */
					new_conns_polling |= conn->wait_set_index == -1;
					break;
			}

			Assert(new_conns_polling || conn->wait_set_index != 0);
		}
	}

	/*
	 * Do we have to keep looping over the whole conns array or can we go back
	 * to servicing connections on wait events?
	 */
	conns_polling = new_conns_polling;
}

void
msgb_add_destination(uint32 destination_id, const char *dsn)
{
	int i;
	MsgbConnection *conn = NULL;

	ensure_received_hook();
	
	for (i = 0; i < max_conns; i++)
	{
		if (conns[i].destination_id == 0)
		{
			conn = &conns[i];
			break;
		}
	}

	if (conn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no free message broker slots"),
				 errdetail("All %d message broker slots already in use", max_conns)));

	conn->pgconn = NULL;
	conn->destination_id = destination_id;
	conn->dsn = MemoryContextStrdup(msgbuf_context, dsn);
	conn->wait_set_index = -1;
	Assert(conn->queue_context == NULL);
	Assert(conn->send_queue == NIL);

	conn->queue_context = AllocSetContextCreate(TopMemoryContext,
										   "msgbroker queue",
										   ALLOCSET_DEFAULT_SIZES);

	conns_polling = true;
}

static int
msgb_idx_for_destination(uint32 destination_id, const char *errm_action)
{
	int i, found = -1;

	ensure_received_hook();
	for (i = 0; i < max_conns; i++)
	{
		if (conns[i].destination_id == destination_id)
		{
			found = i;
			break;
		}
	}

	if (found == -1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("attempt to %s destination id %d that is not registered",
				 				 errm_action, destination_id)));
	}

	return found;
}

/*
 * Remove a registered destination and discard any messages that are queued for
 * it.
 */
void
msgb_remove_destination(uint32 destination_id)
{
	int idx = msgb_idx_for_destination(destination_id, "remove");
	if (idx >= 0)
		msgb_remove_destination_by_index(idx);
}

/*
 * Enqueue a message. On failure to dispatch returns -1, otherwise returns a
 * message identifier that can be used to look up message delivery status
 * later.
 */
int
msgb_queue_message(uint32 destination, const char * payload, Size payload_size)
{
	MsgbMessageBuffer *msg = NULL;
	MemoryContext old_mctx;
	int idx;
	MsgbConnection *conn;

	ensure_received_hook();

	idx = msgb_idx_for_destination(destination, "enqueue message for");
	if (idx < 0)
		return -1;

	conn = &conns[idx];

	/* TODO: queue length limit for memory safety? */

	old_mctx = MemoryContextSwitchTo(conn->queue_context);

	msg = palloc(sizeof(MsgbMessageBuffer) + payload_size);
	/* TODO: counter wraparound? */
	msg->msgid = conn->msgb_msgid_counter ++;
	msg->payload_size = payload_size;
	msg->send_status = MSGB_MSGSTATUS_QUEUED;
	memcpy(&msg->payload[0], payload, payload_size);
	
	conn->send_queue = lappend(conn->send_queue, msg);

	(void) MemoryContextSwitchTo(old_mctx);

	return msg->msgid;
}

/*
 * Query a message for delivery status based on the message id
 * returned when it was enqueued.
 */
MsgbSendStatus
msgb_message_status(uint32 destination, int msgid)
{
	ListCell *lc;
	MsgbMessageBuffer *msg = NULL;
	int next_pending_msgid;
	int idx;
	MsgbConnection *conn;

	idx = msgb_idx_for_destination(destination, "get message status for");
	if (idx < 0)
		return MSGB_MSGSTATUS_NOTFOUND;

	conn = &conns[idx];

	if (conn->send_queue == NIL)
		return MSGB_MSGSTATUS_NOTFOUND;

	next_pending_msgid = ((MsgbMessageBuffer*)linitial(conn->send_queue))->msgid;

	foreach(lc, conn->send_queue)
	{
		msg = lfirst(lc);
		if (msg->msgid == msgid)
			break;
	}

	if (msg->msgid == msgid)
		return msg->send_status;
	else if (msgid < next_pending_msgid)
		return MSGB_MSGSTATUS_DELIVERED;
	else
	{
		Assert(msgid <= conn->msgb_msgid_counter);
		return MSGB_MSGSTATUS_NOTFOUND;
	}
}

static void
msgb_remove_destination_by_index(int index)
{
	MsgbConnection *conn = &conns[index];

	conn->destination_id = 0;

	if (conn->pgconn != NULL)
	{
		/* PQfinish is used by async conns too */
		PQfinish(conn->pgconn);
		conn->pgconn = NULL;
	}

	if (conn->dsn != NULL)
	{
		pfree(conn->dsn);
		conn->dsn = NULL;
	}


	if (conn->wait_set_index != -1)
	{
		/*
		 * There's no API in 9.6 or Pg10 to remove a socket being waited
		 * on from a wait set. See
		 * http://www.postgresql.org/search/?m=1&q=CAMsr%2BYG8zjxu6WfAAA-i33PQ17jvgSO7_CfSh9cncg_kRQ2NDw%40mail.gmail.com
		 * So we must work around it by dropping and re-creating the wait event set. This'll scan the connections
		 * array and re-create it with only known valid sockets.
		 */
		msgb_recreate_wait_event_set();
	}

	if (conn->send_queue != NIL)
	{
		ereport(DEBUG2, 
				(errmsg_internal("peer removed with %d pending messages",
				 list_length(conn->send_queue))));

		/* context delete will clear data */
		conn->send_queue = NIL;
	}

	if (conn->queue_context != NULL)
	{
		MemoryContextDelete(conn->queue_context);
		conn->queue_context = NULL;
	}
}	

/*
 * Disconnect from all peers, discard pending messages, and shut
 * down. Has no effect if not started.
 */
void
msgb_shutdown(void)
{
	int i;

	/* Don't free the wait event set, it was passed in by caller */
	wait_set = NULL;

	for (i = 0; i < max_conns; i++)
		msgb_remove_destination_by_index(i);

	conns = NULL;

	MemoryContextDelete(msgbuf_context);
	msgbuf_context = NULL;
}

static void
msgb_atexit(int code, Datum arg)
{
	msgb_shutdown();
}

/*
 * Pg 10 doesn't offer any interface to remove sockets from a wait event set.
 * So if a socket dies we must trash the wait event set and rebuild it.
 */
static void
msgb_recreate_wait_event_set(void)
{
	int i;

	wait_set = msgb_recreate_wait_event_set_hook(wait_set, max_conns);

	for (i = 0; i < max_conns; i++)
	{
		MsgbConnection *conn = &conns[i];
		conn->wait_set_index = -1;

		if (conn->pgconn != NULL && PQstatus(conn->pgconn) == CONNECTION_OK)
		{
			/*
			 * The wait-set API doesn't let us get current flags so we
			 * initially select all sockets. PQconsumeInput and PQflush will
			 * let us reset the states.
			 */
			msgb_register_wait_event(conn, WL_SOCKET_WRITEABLE|WL_SOCKET_READABLE);
		}
	}
}
