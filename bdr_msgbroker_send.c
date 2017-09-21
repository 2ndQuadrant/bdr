#include "postgres.h"

#include <sys/time.h>
#include <math.h>

#include "catalog/pg_type.h"

#include "fmgr.h"

#include "miscadmin.h"

#include "nodes/pg_list.h"

#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/proc.h"

#include "libpq-fe.h"

#include "utils/elog.h"
#include "utils/memutils.h"

#include "bdr_worker.h"
#include "bdr_catcache.h"
#include "bdr_msgbroker_send.h"
#include "bdr_msgbroker.h"

typedef struct MsgbMessageBuffer
{
	int msgid;
	enum MsgbSendStatus send_status;
	Size payload_size;
	char payload[FLEXIBLE_ARRAY_MEMBER];
} MsgbMessageBuffer;

/*
 * The connection status is used to check the
 * connection state against our expectations.
 */
typedef enum MsgbConnStatus
{
	MSGB_SEND_CONN_UNUSED,
	MSGB_SEND_CONN_PENDING_START,
	MSGB_SEND_CONN_POLLING,
	MSGB_SEND_CONN_POLLING_DONE,
	/* Conn becomes event-driven now */
	MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM,
	MSGB_SEND_CONN_READY
} MsgbConnStatus;

typedef struct MsgbConnection
{
	MsgbConnStatus conn_status;
	uint32 destination_id;
	char *dsn;
	PGconn *pgconn;
	int wait_set_index;				/* wait-set index or -1 if none assigned */
	int wait_flags;					/* for re-creating wait-event sets */
	int msgb_msgid_counter;			/* next message ID to allocate */
	MemoryContext queue_context;	/* memory context for MsgbMessageBuffer */
	List *send_queue;				/* List of MsgbMessageBuffer */
	time_t next_retry_ts;			/* Don't reconnect until this time */
	int num_retries;				/* number of retries since last good connection */
} MsgbConnection;

#define MSGB_DEFAULT_RECONNECT_DELAY_S 2
#define MSGB_MAX_RECONNECT_DELAY_S 600

static inline bool
ConnIsEventDriven(MsgbConnection *conn)
{
	return conn->conn_status == MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM
		   || conn->conn_status == MSGB_SEND_CONN_READY;
}

msgb_request_recreate_wait_event_set_hook_type msgb_request_recreate_wait_event_set_hook = NULL;

/* The wait event set we maintain all our sockets in */
static WaitEventSet *wait_set = NULL;

/* connection and peer state */
static MsgbConnection *conns = NULL;

static MemoryContext msgbuf_context = NULL;

/*
 * Are any connections pending and not yet registered in the wait-list
 * (sockets not created yet)?
 */
static bool conns_polling = false;
static bool wait_set_recreate_pending = false;

static void msgb_remove_destination_by_index(int index, bool recreate_eventset);
static void msgb_start_connect(MsgbConnection *conn);
static void msgb_continue_async_connect(MsgbConnection *conn);
static void msgb_finish_connect(MsgbConnection *conn);
static void msgb_clear_bad_connection(MsgbConnection *conn);
static int msgb_send_pending(MsgbConnection *conn);
static int msgb_recv_pending(MsgbConnection *conn);
static int msgb_idx_for_destination(uint32 destination, const char *errm_action);
static int msgb_flush_conn(MsgbConnection *conn);
static void msgb_conn_set_wait_flags(MsgbConnection *conn, int flags);

static void msgb_request_recreate_wait_event_set(void);

static void msgb_register_wait_event(MsgbConnection *conn);

static void
msgb_status_invariant(MsgbConnection *conn)
{
	ListCell *lc;
	Assert(conn != NULL);

	switch (conn->conn_status)
	{
		case MSGB_SEND_CONN_UNUSED:
			Assert(conn->destination_id == 0);
			Assert(conn->dsn == NULL);
			Assert(conn->pgconn == NULL);
			Assert(conn->wait_set_index == -1);
			Assert(conn->wait_flags == 0);
			Assert(conn->queue_context == NULL);
			Assert(conn->msgb_msgid_counter == 1);
			Assert(conn->send_queue == NIL);
			break;
		case MSGB_SEND_CONN_PENDING_START:
			Assert(conn->destination_id != 0);
			Assert(conn->dsn != NULL);
			Assert(conn->pgconn == NULL);
			Assert(conn->wait_set_index == -1);
			Assert(conn->queue_context != NULL);
			/* Items can be queued now */
			Assert(conn->msgb_msgid_counter >= 1);
			Assert(list_length(conn->send_queue) >= 0);
			/* Messages can't be in-progress if conn not online */
			foreach (lc, conn->send_queue)
			{
				MsgbMessageBuffer *buf = lfirst(lc);
				Assert(buf->send_status == MSGB_MSGSTATUS_QUEUED);
			}
			/* We must be polling */
			Assert(conns_polling);
			break;
		case MSGB_SEND_CONN_POLLING:
		case MSGB_SEND_CONN_POLLING_DONE:
			Assert(conn->destination_id != 0);
			Assert(conn->dsn != NULL);
			Assert(conn->pgconn != NULL);
			/*
			 * Wait events aren't registered in polling; they can be once
			 * we've confirmed polling is finished.
			 */
			if (conn->conn_status == MSGB_SEND_CONN_POLLING)
				Assert(conn->wait_set_index == -1);
			/*
			 * We might set wait-flags while still polling, in preparation for
			 * going to event driven.
			 */
			Assert(conn->wait_flags >= 0);
			Assert(conn->queue_context != NULL);
			/* Items can be queued now */
			Assert(conn->msgb_msgid_counter >= 1);
			Assert(list_length(conn->send_queue) >= 0);
			/* Messages can't be in-progress if conn establishing */
			foreach (lc, conn->send_queue)
			{
				MsgbMessageBuffer *buf = lfirst(lc);
				Assert(buf->send_status == MSGB_MSGSTATUS_QUEUED);
			}
			/* We must be polling */
			Assert(conns_polling);
			break;
		case MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM:
		case MSGB_SEND_CONN_READY:
			Assert(conn->destination_id != 0);
			Assert(conn->dsn != NULL);
			Assert(conn->pgconn != NULL);
			if (conn->conn_status == MSGB_SEND_CONN_READY)
				Assert(conn->num_retries == 0);
			/*
			 * We must have a defined wait-event index unless we're in the
			 * middle of rebuilding the wait-event set, in which case we may
			 * have deferred a wait-event slot allocation for a newly
			 * event-driven connection. Or if we're still polling, we might not
			 * have got around to setting one up yet.
			 */
			if (!wait_set_recreate_pending && !conns_polling)
				Assert(conn->wait_set_index >= 0);
			/*
			 * An active connection must be waiting on receive and/or send.
			 * Even if there's no wait-event set, this is what we'll register
			 * for when we do create the wait-event set entry and it must be
			 * correct otherwise we'll fall over when we exit polling.
			 *
			 * If the send queue is empty we must be waiting for readable data.
			 * If it's non-empty we must be either have a queue item in-flight
			 * or be waiting for it to become sendable. It's OK if we're also
			 * waiting for the other condition in either state too.
			 */
			Assert(conn->wait_flags & WL_SOCKET_READABLE);
			if (conn->send_queue != NIL && conn->conn_status == MSGB_SEND_CONN_READY)
				Assert(((MsgbMessageBuffer*)linitial(conn->send_queue))->send_status == MSGB_MSGSTATUS_SENDING
					   || conn->wait_flags & WL_SOCKET_WRITEABLE);

			Assert(conn->queue_context != NULL);
			/* Items can be queued now */
			Assert(conn->msgb_msgid_counter >= 1);

			/*
			 * Items in the list can never be delivered since we pop them
			 * immediately.
			 */
			foreach (lc, conn->send_queue)
			{
				MsgbMessageBuffer *buf = lfirst(lc);
				if (conn->conn_status == MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM)
					Assert(buf->send_status == MSGB_MSGSTATUS_QUEUED);
				else
					Assert(buf->send_status == MSGB_MSGSTATUS_QUEUED
						   || buf->send_status == MSGB_MSGSTATUS_SENDING);
			}
			break;
		default:
			Assert(false); /* unhandled state! */
	}
}

/*
 * Start up the message broker's libpq-based message delivery system.
 *
 * We will register all our sockets in a wait-event set supplied by the caller
 * (via the wait set recreate hook), and set it as the active set that will be
 * maintained by future add/remove destination operations.
 */
void
msgb_startup_send(void)
{
	int i;

	/*
	 * Wait event sets lack support for removing or replacing socket,
	 * so we must be able to re-create it.
	 */
	if (msgb_request_recreate_wait_event_set_hook == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("caller must install msgb_request_recreate_wait_event_set_hook")));

	if (msgbuf_context != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("msgbroker already running")));

	msgb_request_recreate_wait_event_set();

	msgbuf_context = AllocSetContextCreate(TopMemoryContext,
										   "msgbroker context",
										   ALLOCSET_DEFAULT_SIZES);

	conns = MemoryContextAlloc(msgbuf_context,
							   sizeof(MsgbConnection) * msgb_max_peers);


	memset(conns, 0, sizeof(MsgbConnection) * msgb_max_peers);

	for (i = 0; i < msgb_max_peers; i++)
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
 */
static void
msgb_clear_bad_connection(MsgbConnection *conn)
{
	ListCell   *lc;
	struct timeval tv_now;

	if (conn->pgconn)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("connection to peer %u went down: %s",
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
	{
		msgb_request_recreate_wait_event_set();
		/*
		 * The recreate may not be immediate, but we can safely abandon
		 * the wait-set entry knowing the whole set will get recreated.
		 */
		conn->wait_set_index = -1;
	}

	conn->wait_flags = 0;

	/* Reconnect please */
	conn->conn_status = MSGB_SEND_CONN_PENDING_START;

	/*. .. after a delay */
	gettimeofday(&tv_now, NULL);
	conn->next_retry_ts = tv_now.tv_sec
		+ Min(MSGB_DEFAULT_RECONNECT_DELAY_S * (time_t)pow(2,conn->num_retries),
			  MSGB_MAX_RECONNECT_DELAY_S);

	/* remember to reconnect */
	conns_polling = true;

	msgb_status_invariant(conn);
}

/*
 * Tell libpq to start connecting to a node. Most of the actual
 * work is done in PQconnectPoll in msgb_continue_async_connect(...)
 */
static void
msgb_start_connect(MsgbConnection *conn)
{
	int status;
	struct timeval tv_now;

	Assert(conn->conn_status == MSGB_SEND_CONN_PENDING_START);
	msgb_status_invariant(conn);

	gettimeofday(&tv_now, NULL);
	if (tv_now.tv_sec < conn->next_retry_ts)
	{
		elog(DEBUG3, "broker not reconnecting to peer %u yet due to backoff delay",
			conn->destination_id);
		return;
	}

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
	conn->conn_status = MSGB_SEND_CONN_POLLING;
	msgb_status_invariant(conn);
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
	int pollstatus;

	Assert(conn->conn_status == MSGB_SEND_CONN_POLLING);
	msgb_status_invariant(conn);

 	pollstatus = PQconnectPoll(conn->pgconn);

	switch (pollstatus)
	{
		case PGRES_POLLING_OK:
			conn->conn_status = MSGB_SEND_CONN_POLLING_DONE;
			elog(bdr_debug_level, "%u connected to peer %u, backend pid is %d",
				 bdr_get_local_nodeid(), conn->destination_id,
				 PQbackendPID(conn->pgconn));
			msgb_finish_connect(conn);
			break;
		case PGRES_POLLING_READING:
		case PGRES_POLLING_WRITING:
			/* nothing to do but wait until socket readable/writeable again */
			break;
		case PGRES_POLLING_FAILED:
			if (PQconnectionNeedsPassword(conn->pgconn))
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PASSWORD),
						 errmsg("%u connection to node %u needs password, but none was supplied: %s",
						 		bdr_get_local_nodeid(), conn->destination_id,
								PQerrorMessage(conn->pgconn))));
			else
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("%u connecting to %u failed: %s",
								bdr_get_local_nodeid(), conn->destination_id,
								PQerrorMessage(conn->pgconn))));
			msgb_clear_bad_connection(conn);	
			break;
	}

	/* We can exit in almost any state here */
	Assert(conn->conn_status != MSGB_SEND_CONN_UNUSED);

	msgb_status_invariant(conn);
}

/*
 * Establish our shared memory queue connection between our backend on the peer
 * and the peer's manager worker by calling a SQL-callable function on the
 * backend.
 */
static void
msgb_peer_connect(MsgbConnection *conn)
{
	int ret;
	const int nParams = 3;
	Oid paramTypes[3] = { OIDOID, OIDOID, OIDOID };
	const char * paramValues[3];
	char destination_id[30];
	char origin_id[30];
	char last_message_id[30];

	Assert(conn->conn_status == MSGB_SEND_CONN_POLLING_DONE);
	msgb_status_invariant(conn);

	snprintf(origin_id, 30, "%u", bdr_get_local_nodeid());
	paramValues[0] = origin_id;
	snprintf(destination_id, 30, "%u", conn->destination_id);
	paramValues[1] = destination_id;
	/*
	 * The msgid counter is the *next* message-id and we want to send the id of
	 * the last message sent, or 0 if none sent by this broker instance yet.
	 */
	snprintf(last_message_id, 30, "%u", conn->msgb_msgid_counter - 1);
	paramValues[2] = last_message_id;

	ret = PQsendQueryParams(conn->pgconn, "SELECT * FROM "MSGB_SCHEMA".msgb_connect($1, $2, $3)", nParams,
							paramTypes, paramValues, NULL, NULL, 0);

	if (ret)
	{
		int flags;
		/*
		 * We've sent a query that won't follow the normal response path
		 * since there's no corresponding send_queue entry, so we can't
		 * go straight to MSGB_SEND_CONN_READY yet. We'll handle this in
		 * msgb_process_result and set it there.
		 */
		conn->conn_status = MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM;
		/* always wants read, maybe more write too */
		flags = msgb_flush_conn(conn);
		msgb_conn_set_wait_flags(conn, flags);
	}
	else 
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to establish remote shared memory connection to peer %u: %s",
						conn->destination_id, PQerrorMessage(conn->pgconn))));
		/* Assume bad connection */
		msgb_clear_bad_connection(conn);
	}

	msgb_status_invariant(conn);
}

/*
 * Once a connection is fully established we can switch from polling to wait
 * events for handling the connection.
 */
static void
msgb_finish_connect(MsgbConnection *conn)
{
	Assert(conn->conn_status = MSGB_SEND_CONN_POLLING_DONE);
	msgb_status_invariant(conn);

	if (PQsetnonblocking(conn->pgconn, 1) != 0)
	{
		/* shouldn't happen */
		elog(WARNING, "failed to put connection in non-blocking mode: %s",
					  PQerrorMessage(conn->pgconn));
		msgb_clear_bad_connection(conn);
	}
	else
	{
		conn->wait_flags = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
		msgb_register_wait_event(conn);

		/*
		 * Now we have to attach to the shmem memory queue on the other end and
		 * ensure it's ready to process messages, so dispatch the query for that.
		 *
		 * It won't finish immediately, but we'll switch to wait-event based
		 * processing and consume the result when we're woken on the socket.
		 */
		msgb_peer_connect(conn);
	}

	/*
	 * Since the connection can break we can be in almost any state here,
	 * but we can't be back to polling.
	 *
	 * It should be MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM if we've
	 * succeeded.
	 */
	Assert(conn->conn_status != MSGB_SEND_CONN_POLLING_DONE
		   && conn->conn_status != MSGB_SEND_CONN_POLLING
		   && conn->conn_status != MSGB_SEND_CONN_UNUSED);

	msgb_status_invariant(conn);
}

/*
 * (re)register a connection in the wait-events array.
 *
 * Does not change connection state, the caller is responsible for managing
 * that if needed.
 */
static void
msgb_register_wait_event(MsgbConnection *conn)
{
	Assert(ConnIsEventDriven(conn) || conn->conn_status == MSGB_SEND_CONN_POLLING_DONE);

	msgb_status_invariant(conn);

	/*
	 * Register the connection in our wait event set.
	 *
	 * We'll stop polling the connection when we see a wait_set_index for it.
	 *
	 * If we're currently busy doing event processing and have asked for the
	 * wait-event set to be re-created there's no point doing this.
	 */
	if (!wait_set_recreate_pending)
	{
		conn->wait_set_index = AddWaitEventToSet(wait_set,
												 conn->wait_flags,
												 PQsocket(conn->pgconn),
												 NULL, (void*)conn);

		/* AddWaitEventToSet Will elog(...) on failure */
		Assert(conn->wait_set_index != -1);
	}

	msgb_status_invariant(conn);
}

/*
 * Flush pending data from libpq, if any, and reset connection state on error.
 *
 * Returns WL_SOCKET_WRITEABLE|WL_SOCKET_READABLE if more data is still pending to be sent,
 * since we might also have to consume input to let the server read from its own recv
 * buffer and free up ours. This will be ignored by a caller that's still polling, but
 * is needed if the connection is event-driven.
 *
 * If there's nothing more to send, return WL_SOCKET_READABLE on the assumption we
 * expect a reply.
 */
static int
msgb_flush_conn(MsgbConnection *conn)
{
	int ret;

	/* Cannot flush unused conn */
	Assert(conn->conn_status != MSGB_SEND_CONN_UNUSED);
	Assert(conn->destination_id != 0);

	msgb_status_invariant(conn);

	if (conn->pgconn == NULL)
	{
		/*
		 * Can't flush broken connection. We flush lots of places where we
		 * might have just detected a broken connection so this is OK if
		 * we're set to poll (so we reconnect).
		 */
		Assert(conns_polling == true);
		Assert(conn->conn_status == MSGB_SEND_CONN_PENDING_START);
		ret = 0;
	}
	else
	{
		ret = PQflush(conn->pgconn);
		if (ret == -1)
		{
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("failed to flush output to %u: %s",
							conn->destination_id, PQerrorMessage(conn->pgconn))));
			/* Assume bad connection */
			msgb_clear_bad_connection(conn);
			ret = 0;
		}
		else if (ret == 1)
			ret = WL_SOCKET_WRITEABLE|WL_SOCKET_READABLE;
		else
			ret = WL_SOCKET_READABLE;
	}

	msgb_status_invariant(conn);
	return ret;
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
	PGresult *res;
	MsgbMessageBuffer *buf;
	bool more_pending;

	Assert(conn->destination_id != 0);
	/* don't call with broken connection */
	Assert(conn->pgconn != NULL);

	/* Async result processing only happens in event driven mode */
	Assert(ConnIsEventDriven(conn));
	msgb_status_invariant(conn);

	res = PQgetResult(conn->pgconn);

	if (res == NULL)
	{
		/*
		 * We already handled the results in a prior call, this call just
		 * confirmed that there's nothing left to do.
		 */
		more_pending = false;
	}
	else if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("while getting result from node %u: %s",
				 		conn->destination_id, PQerrorMessage(conn->pgconn))));
		PQclear(res);
		msgb_clear_bad_connection(conn);
		more_pending = false;
	}
	else
	{
		/* Our SQL functions return void */
		Assert(PQntuples(res) == 1);
		Assert(PQnfields(res) == 1);
		Assert(PQftype(res, 0) == VOIDOID);

		if (conn->conn_status == MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM)
		{
			/*
			 * We're seeing the result of msgb_finish_connect's call to
			 * msgb_peer_connect. If it's all OK, the connection is
			 * now ready for normal message delivery.
			 */
			conn->conn_status = MSGB_SEND_CONN_READY;
			Assert(conns_polling || conn->wait_set_index != -1);
			/* Successful connection, reset backoff counter */
			conn->num_retries = 0;
			elog(bdr_debug_level, "connection to %u is now ready",
				 conn->destination_id);
		}
		else
		{
			Assert(conn->conn_status == MSGB_SEND_CONN_READY);

			/* Message delivered */
			buf = linitial(conn->send_queue);
			Assert(buf->send_status = MSGB_MSGSTATUS_SENDING);
			conn->send_queue = list_delete_first(conn->send_queue);
			elog(bdr_debug_level, "%u delivered msgbroker message #%u for %u (%d pending)",
				bdr_get_local_nodeid(), buf->msgid, conn->destination_id,
				list_length(conn->send_queue));
			pfree(buf);
		}

		PQclear(res);

		/* PQgetResult didn't return NULL, might be more results */
		more_pending = true;

		/*
		 * Is there another to deliver? If so, try to as soon as the socket
		 * is writeable. Otherwise we can just consume notices etc until
		 * someone gives us something else to deliver.
		 */
		if (conn->send_queue == NIL)
			msgb_conn_set_wait_flags(conn, WL_SOCKET_READABLE);
		else
			msgb_conn_set_wait_flags(conn, WL_SOCKET_READABLE|WL_SOCKET_WRITEABLE);
	}

	msgb_status_invariant(conn);
	return more_pending;
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
	bool wait_flags;

	Assert(ConnIsEventDriven(conn));
	msgb_status_invariant(conn);

	if (PQconsumeInput(conn->pgconn) == 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("while receiving from node %u: %s",
				 		conn->destination_id, PQerrorMessage(conn->pgconn))));
		msgb_clear_bad_connection(conn);
		wait_flags = 0;
	}
	else
	{
		bool need_recv_more = true;

		/*
		 * Keep reading until we'd block, we've read eveything
		 * we need to, or the connection breaks.
		 */
		while (need_recv_more && !PQisBusy(conn->pgconn))
			need_recv_more = msgb_process_result(conn);

		/*
		 * Always marks socket want-read, maybe write too, unless the
		 * connection broke during the above.
		 */
		wait_flags = msgb_flush_conn(conn);
	}

	Assert(conn->conn_status != MSGB_SEND_CONN_UNUSED);
	msgb_status_invariant(conn);
	return wait_flags;
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
	Oid paramTypes[3] = { OIDOID, OIDOID, BYTEAOID };
	const char * paramValues[3];
	char destination_id[30];
	char msgid_str[30];
	size_t escapedLen;
	int ret;
	int wait_flags;

	const char * const msg_send_query = "SELECT * FROM "MSGB_SCHEMA".msgb_deliver_message($1, $2, $3)";

	Assert(conn->conn_status == MSGB_SEND_CONN_READY);
	msgb_status_invariant(conn);

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
	if (ret)
	{
		buf->send_status = MSGB_MSGSTATUS_SENDING;
		/* always wants read, maybe more write too */
		wait_flags = msgb_flush_conn(conn);
	}
	else
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to dispatch message to %u: %s",
						conn->destination_id, PQerrorMessage(conn->pgconn))));
		/* Assume bad connection */
		msgb_clear_bad_connection(conn);
		wait_flags = 0;
	}

	Assert(conn->conn_status != MSGB_SEND_CONN_UNUSED);
	msgb_status_invariant(conn);
	return wait_flags;
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
	int wait_flags;

	msgb_status_invariant(conn);

	if (conn->send_queue == NIL)
	{
		/*
		 * We only pop messages off the queue once confirmed
		 * delivered, so there must be nothing pending. We still
		 * want to hear about it if the server asynchronously
		 * notifies us of something so keep reading.
		 */
		wait_flags = WL_SOCKET_READABLE;
	}
	else if (conn->conn_status == MSGB_SEND_CONN_READY)
	{
		MsgbMessageBuffer *buf = linitial(conn->send_queue);
		if (buf->send_status == MSGB_MSGSTATUS_SENDING)
			wait_flags = msgb_flush_conn(conn);
		else if (buf->send_status == MSGB_MSGSTATUS_QUEUED)
			wait_flags = msgb_send_next(conn);
		else
			ereport(ERROR, (errmsg_internal("unexpected send status %d", buf->send_status)));
	}
	else if (conn->conn_status == MSGB_SEND_CONN_CONNECTING_REMOTE_SHMEM)
	{
		/* Still doing our initial setup query, so don't look at the queue yet. */
		wait_flags = msgb_flush_conn(conn);
	}

	Assert(conn->conn_status != MSGB_SEND_CONN_UNUSED);
	msgb_status_invariant(conn);
	return wait_flags;
}

static void
msgb_conn_set_wait_flags(MsgbConnection *conn, int flags)
{
	conn->wait_flags = flags;

	if (wait_set != NULL && ConnIsEventDriven(conn))
	{
		if (conn->wait_set_index == -1)
			msgb_register_wait_event(conn);
		else
			ModifyWaitEvent(wait_set, conn->wait_set_index,
				flags, NULL);
	}
}

/*
 * TODO: set up an errcontext here
 *
 * Handle message processing on ready sockets. Queue new messages where they're
 * sendable, flush the libpq buffer where writeable, read from the socket and
 * determine message send outcomes where readable.
 */
static void
msgb_service_connections_events(WaitEvent *occurred_events, int nevents)
{
	int i;

	/*
	 * Service connections for which wait events flagged
	 * activity.
	 */
	for (i = 0; i < nevents; i++)
	{
		WaitEvent const	   *e = &occurred_events[i];
		MsgbConnection	   *conn = NULL;
		int					i;
		int					new_wait_flags = 0;

		/* Find the connection for this wait event */
		for (i = 0; i < msgb_max_peers; i++)
		{
			if (conns[i].destination_id != 0 && conns[i].wait_set_index == e->pos)
			{
				conn = &conns[i];
				/* Sanity check only */
 				Assert(conn == e->user_data);
				break;
			}
		}

		if (conn == NULL)
		{
			/*
			 * It's OK not to find a connection; the wait event may be for
			 * somebody else, a latch set, or whatever.
			 */
			continue;
		}

		msgb_status_invariant(conn);

		new_wait_flags |= msgb_recv_pending(conn);
		new_wait_flags |= msgb_send_pending(conn);

		msgb_conn_set_wait_flags(conn, new_wait_flags);

		CHECK_FOR_INTERRUPTS();

		msgb_status_invariant(conn);
	}
}

/*
 * Service connections that need polling because they aren't ready to switch
 * over to wait events yet. This is mainly required for connections that are in
 * libpq's async handshake process.
 */
static void
msgb_service_connections_polling(void)
{
	bool new_conns_polling = false;
	int i;

	/*
	 * Do connection maintenance that cannot be done via
	 * wait events, like connection establishment.
	 */
	if (conns_polling)
	{
		for (i = 0; i < msgb_max_peers; i++)
		{
			MsgbConnection * const conn = &conns[i];

			msgb_status_invariant(conn);

			/*
			 * connection removal is done immediately, so
			 * can't be any maintenance tasks here.
			 */
			if (conn->conn_status == MSGB_SEND_CONN_UNUSED)
				continue;

			/*
			 * Start a new or replacement connection and start polling until
			 * completed.
			 */
			if (conn->conn_status == MSGB_SEND_CONN_PENDING_START)
			{
				msgb_start_connect(conn);
				new_conns_polling = true;
				/*
				 * Deliberately fall through to start polling if conn was
				 * started OK.
				 */
			}

			if (conn->conn_status == MSGB_SEND_CONN_POLLING)
			{
				switch (PQstatus(conn->pgconn))
				{
					case CONNECTION_OK:
						conn->conn_status = MSGB_SEND_CONN_POLLING_DONE;
						elog(bdr_debug_level, "finished connect %d for %u", i, conn->destination_id);
						msgb_finish_connect(conn);
						break;
						
					case CONNECTION_BAD:
						/*
						 * failed, must ensure wait event cleared and reconnect
						 * next time around.
						 */
						msgb_clear_bad_connection(conn);
						/*
						 * msgb_clear_bad_connection set conns_polling, but
						 * we'd clobber it if we didn't remember it here since
						 * we already read it above and won't re-read it.
						 */
						new_conns_polling = true;
						break;

					default:
						/*
						 * All other states are async connect progress states
						 * where we must continue to PQconnectPoll(...)
						 */
						elog(bdr_debug_level, "continuing connect %d for %u state %d",
							 i, conn->destination_id, PQstatus(conn->pgconn));
						msgb_continue_async_connect(conn);
						/*
						 * The conn could've switched to async events now but
						 * it's harmless to poll again, and simpler.
						 */
						new_conns_polling = true;
						break;
				}
			}

			msgb_status_invariant(conn);

			/*
			 * If we're polling we examine connections that we think are active
			 * too, because we might lose events while re-creating a wait-event
			 * set.
			 */
			if (ConnIsEventDriven(conn))
			{
				int new_wait_flags = 0;
				new_wait_flags |= msgb_recv_pending(conn);
				new_wait_flags |= msgb_send_pending(conn);
				msgb_conn_set_wait_flags(conn, new_wait_flags);
			}

			CHECK_FOR_INTERRUPTS();

			msgb_status_invariant(conn);
		}
	}

	/*
	 * Do we have to keep looping over the whole conns array or can we go back
	 * to servicing connections on wait events?
	 */
	conns_polling = new_conns_polling;
}

/*
 * Handle connection maintenance, both wait-event driven activity
 * and polling for connections that are still being set up.
 */
void
msgb_service_connections_send(WaitEvent *occurred_events, int nevents,
							  long *max_next_wait_ms)
{

	if (nevents == 0 && !conns_polling)
		return;

	msgb_service_connections_events(occurred_events, nevents);
	msgb_service_connections_polling();

	/* Don't let the host proc sleep too long if polling */
	if (conns_polling)
		*max_next_wait_ms = Min(*max_next_wait_ms, 200L);
}

/*
 * Remove a registered destination and discard any messages that are queued for
 * it.
 */
void
msgb_remove_send_peer(uint32 destination_id)
{
	int idx = msgb_idx_for_destination(destination_id, "remove");
	if (idx >= 0)
		msgb_remove_destination_by_index(idx, true);
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

	idx = msgb_idx_for_destination(destination, "enqueue message for");
	if (idx < 0)
		return -1;

	conn = &conns[idx];

	msgb_status_invariant(conn);

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

	elog(bdr_debug_level, "%u enqueued msgbroker message #%u for %u",
		bdr_get_local_nodeid(), msg->msgid, destination);

	/* Make sure we service the connection properly */
	msgb_conn_set_wait_flags(conn, conn->wait_flags|WL_SOCKET_WRITEABLE);
	Assert(ConnIsEventDriven(conn) || conns_polling);
	elog(LOG, "XXX set wait flags to %d after enqueue", conn->wait_flags);

	msgb_status_invariant(conn);

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

	msgb_status_invariant(conn);

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
msgb_remove_destination_by_index(int index, bool recreate_eventset)
{
	MsgbConnection *conn = &conns[index];

	msgb_status_invariant(conn);

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

	conn->wait_flags = 0;

	if (conn->wait_set_index != -1)
	{
		conn->wait_set_index = -1;

		if (recreate_eventset)
		/*
		 * There's no API in 9.6 or Pg10 to remove a socket being waited
		 * on from a wait set. See
		 * http://www.postgresql.org/search/?m=1&q=CAMsr%2BYG8zjxu6WfAAA-i33PQ17jvgSO7_CfSh9cncg_kRQ2NDw%40mail.gmail.com
		 * So we must work around it by dropping and re-creating the wait event set. This'll scan the connections
		 * array and re-create it with only known valid sockets.
		 *
		 * If we're currently in event processing it'll be re-created when
		 * we exit the event loop.
		 */
		msgb_request_recreate_wait_event_set();
	}

	Assert(conn->wait_set_index == -1);

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

	conn->msgb_msgid_counter = 1;

	conn->conn_status = MSGB_SEND_CONN_UNUSED;

	msgb_status_invariant(conn);
}	

void
msgb_shutdown_send(void)
{
	int i;

	if (conns != NULL)
	{
		for (i = 0; i < msgb_max_peers; i++)
			msgb_remove_destination_by_index(i, false);

		conns = NULL;
	}

	/* Re-create the event set without any of our events in it */
	msgb_request_recreate_wait_event_set();

	/* Don't free the wait event set, it was passed in by caller */
	wait_set = NULL;

	if (msgbuf_context != NULL)
	{
		MemoryContextDelete(msgbuf_context);
		msgbuf_context = NULL;
	}
}

void
msgb_add_send_peer(uint32 destination_id, const char *dsn)
{
	int i;
	MsgbConnection *conn = NULL;
	
	for (i = 0; i < msgb_max_peers; i++)
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
				 errdetail("All %d message broker slots already in use", msgb_max_peers)));

	msgb_status_invariant(conn);

	conn->conn_status = MSGB_SEND_CONN_PENDING_START;
	conn->pgconn = NULL;
	conn->destination_id = destination_id;
	conn->dsn = MemoryContextStrdup(msgbuf_context, dsn);
	conn->wait_set_index = -1;
	Assert(conn->queue_context == NULL);
	Assert(conn->send_queue == NIL);

	conn->queue_context = AllocSetContextCreate(TopMemoryContext,
										   "msgbroker send queue",
										   ALLOCSET_DEFAULT_SIZES);

	conns_polling = true;

	/* Skip any pending sleep, so we start work on connecting immediately */
	SetLatch(&MyProc->procLatch);

	msgb_status_invariant(conn);
}

void
msgb_alter_send_peer(uint32 peer_id, const char *new_dsn)
{
	int idx = msgb_idx_for_destination(peer_id, "alter");
	if (idx >= 0)
	{
		MsgbConnection * const conn = &conns[idx];

		msgb_status_invariant(conn);

		if (conn->dsn)
			pfree(conn->dsn);
		conn->dsn = MemoryContextStrdup(msgbuf_context, new_dsn);

		msgb_clear_bad_connection(conn);
	}
}

static int
msgb_idx_for_destination(uint32 destination_id, const char *errm_action)
{
	int i, found = -1;

	for (i = 0; i < msgb_max_peers; i++)
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
 * Pg 10 doesn't offer any interface to remove sockets from a wait event set.
 * So if a socket dies we must trash the wait event set and rebuild it.
 *
 * A wait set usually can't be recreated as soon as we find out we want to,
 * since we might be in the middle of event loop processing. So we ask whatever
 * is driving our event loop to re-create the set next time it's out of the
 * loop and tell us about it here.
 */
void
msgb_wait_event_set_recreated(WaitEventSet *new_wait_set)
{
	int i;

	/* Don't do anything if the broker is shut down */
	if (conns == NULL)
		return;

	wait_set = new_wait_set;

	for (i = 0; i < msgb_max_peers; i++)
	{
		MsgbConnection *conn = &conns[i];
		msgb_status_invariant(conn);

		conn->wait_set_index = -1;

		if (conn->pgconn != NULL && PQstatus(conn->pgconn) == CONNECTION_OK)
			msgb_register_wait_event(conn);

		msgb_status_invariant(conn);
	}

	wait_set_recreate_pending = false;
}

static void
msgb_request_recreate_wait_event_set(void)
{
	wait_set_recreate_pending = true;
	if (msgb_request_recreate_wait_event_set_hook)
		(*msgb_request_recreate_wait_event_set_hook)(wait_set);
}

int
msgb_get_wait_event_space_needed(void)
{
	return msgb_max_peers;
}
