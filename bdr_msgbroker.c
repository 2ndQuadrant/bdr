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
 */
#include "postgres.h"

#include "storage/latch.h"
#include "storage/ipc.h"

#include "utils/elog.h"
#include "utils/memutils.h"

#include "libpq-fe.h"

#include "bdr_msgbroker.h"

typedef struct MsgbConnection
{
	uint32 destination_id;
	char *dsn;
	PGconn *pgconn;
	int wait_set_index;
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
static void msgb_register_wait_event(MsgbConnection *conn);
static void msgb_clear_bad_connection(MsgbConnection *conn);
static int msgb_send_pending(MsgbConnection *conn);
static int msgb_recv_pending(MsgbConnection *conn);

inline void ensure_received_hook(void)
{
	if (msgb_received_hook == NULL)
		ereport(ERROR, (errmsg_internal("no message hook is registered")));
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

}

static void
msgb_clear_bad_connection(MsgbConnection *conn)
{
	/*
	 * Clean up the old connection, destroy any wait
	 * event, and schedule it for reconnection.
	 *
	 * TODO: should update a random backoff timer here
	 * TODO: emit error to logs or to user
	 */
	if (conn->pgconn)
	{
		PQfinish(conn->pgconn);
		conn->pgconn = NULL;
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
 * Once a connection is fully established we can switch from polling to wait
 * events for handling the connection.
 */
static void
msgb_finish_connect(MsgbConnection *conn)
{
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

	msgb_register_wait_event(conn);
}

static void
msgb_register_wait_event(MsgbConnection *conn)
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
											 WL_SOCKET_WRITEABLE|WL_SOCKET_READABLE, 
											 PQsocket(conn->pgconn),
											 NULL, (void*)conn);

	/* AddWaitEventToSet Will elog(...) on failure */
	Assert(conn->wait_set_index != -1);
}

static int
msgb_recv_pending(MsgbConnection *conn)
{
	bool need_recv_more = true;
	PQconsumeInput(conn->pgconn);

	/*
	 * TODO: test PQisBusy(...) and consume
	 * results if available
	 */
	need_recv_more = false; /*XXX*/
	return need_recv_more ? WL_SOCKET_READABLE : 0;
}

static int
msgb_send_pending(MsgbConnection *conn)
{
	bool need_send_more = true;

	/*
	 * TODO: send pending queries
	 */

	need_send_more = false; /*XXX*/
	return need_send_more ? WL_SOCKET_WRITEABLE : 0;
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

	conns_polling = true;
}

void
msgb_remove_destination(uint32 destination_id)
{
	int i;
	MsgbConnection *conn = NULL;

	ensure_received_hook();
	for (i = 0; i < max_conns; i++)
	{
		if (conns[i].destination_id == destination_id)
		{
			conn = &conns[i];
			break;
		}
	}

	if (conn == NULL)
	{
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("attempt to remove destination id %d that is not active",
				 				 destination_id)));
		return;
	}

	msgb_remove_destination_by_index(i);
}

int
msgb_queue_message(uint32 destination, const char * payload, uint32 payload_size)
{
	ensure_received_hook();
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not implemented")));
}

int
msgb_message_status(int msgid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not implemented")));
}

static void
msgb_remove_destination_by_index(int index)
{
	MsgbConnection *conn = &conns[index];

	conn->destination_id = 0;

	if (conn->dsn != NULL)
	{
		pfree(conn->dsn);
		conn->dsn = NULL;
	}

	if (conn->pgconn != NULL)
	{
		/* PQfinish is used by async conns too */
		PQfinish(conn->pgconn);
		conn->pgconn = NULL;
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
			msgb_register_wait_event(conn);
	}
}
