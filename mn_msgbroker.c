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
 * The message broker doesn't remember anything across crashes/restarts and has
 * no persistent state.  It doesn't guarantee message reliability, we overlay a
 * distributed consensus protocol on top for that. See bdr_consensus.c.
 *
 * It does try to redeliver messages after connection loss, so it's possible
 * for a message to be delivered more than once. Recipients should disregard
 * messages with a message_id less than or equal to the last message received.
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

#include "nodes/pg_list.h"

#include "storage/latch.h"
#include "storage/ipc.h"

#include "utils/elog.h"
#include "utils/memutils.h"

#include "mn_msgbroker.h"
#include "mn_msgbroker_receive.h"
#include "mn_msgbroker_send.h"

static bool atexit_registered = false;

static void msgb_atexit(int code, Datum arg);

void
msgb_startup(uint32 local_node_id, mn_request_waitevents_fn request_waitevents,
			 msgb_receive_cb receive_cb, Size recv_queue_size)
{
	if (!atexit_registered)
		before_shmem_exit(msgb_atexit, (Datum)0);

	msgb_startup_send(local_node_id, request_waitevents);
	msgb_startup_receive(local_node_id, recv_queue_size, receive_cb);
	elog(DEBUG1, "BDR msgbroker: started");
}

void
msgb_wakeup(WaitEvent *occurred_events, int nevents,
			long *max_next_wait_ms)
{
	msgb_wakeup_send(occurred_events, nevents,
								  max_next_wait_ms);
	msgb_wakeup_receive();
}

void
msgb_add_peer(uint32 peer_id, const char *dsn)
{
	msgb_add_send_peer(peer_id, dsn);
	elog(DEBUG1, "BDR msgbroker: added peer %u with dsn '%s'", peer_id, dsn);
}

void
msgb_remove_peer(uint32 peer_id)
{
	msgb_remove_send_peer(peer_id);
	elog(DEBUG1, "BDR msgbroker: peer %u removed", peer_id);
}

void
msgb_update_peer(uint32 peer_id, const char *dsn)
{
	msgb_update_send_peer(peer_id, dsn);
}

/*
 * Disconnect from all peers, discard pending messages, and shut
 * down. Has no effect if not started.
 */
void
msgb_shutdown(void)
{
	msgb_shutdown_send();
	msgb_shutdown_receive();
	elog(DEBUG1, "BDR msgbroker: stopped");
}

static void
msgb_atexit(int code, Datum arg)
{
	msgb_shutdown();
}
