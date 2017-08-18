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

/*
 * Trying to totally isolate the message broker from BDR would probably be
 * silly over-engineering, so let it do things like look up its local node-id
 * directly. It's easy enough to abstract out if we decide to split this off
 * into its own extension for re-use later.
 */
#include "bdr_worker.h"
#include "bdr_catcache.h"

#include "bdr_msgbroker_receive.h"
#include "bdr_msgbroker_send.h"
#include "bdr_msgbroker.h"

static bool atexit_registered = false;

static void msgb_atexit(int code, Datum arg);

int msgb_max_peers;

void
msgb_startup(int max_connections, Size recv_queue_size)
{
	if (!atexit_registered)
		on_proc_exit(msgb_atexit, (Datum)0);

	msgb_max_peers = max_connections;

	if (msgb_max_peers <= 0)
		ereport(ERROR,
				(errmsg_internal("max conns must be positive")));

	msgb_startup_send();
	msgb_startup_receive(recv_queue_size);
	elog(bdr_debug_level, "BDR msgbroker: started (max %d conns)",
		 max_connections);
}

void
msgb_service_connections(WaitEvent *occurred_events, int nevents)
{
	msgb_service_connections_send(occurred_events, nevents);
	msgb_service_connections_receive();
}

void
msgb_add_peer(uint32 peer_id, const char *dsn)
{
	msgb_add_receive_peer(peer_id);
	msgb_add_send_peer(peer_id, dsn);
	elog(bdr_debug_level, "BDR msgbroker: added peer %u with dsn '%s'", peer_id, dsn);
}

void
msgb_alter_peer(uint32 peer_id, const char *new_dsn)
{
	msgb_alter_send_peer(peer_id, new_dsn);
	elog(bdr_debug_level, "BDR msgbroker: peer %u dsn changed to '%s'", peer_id, new_dsn);
}

void
msgb_remove_peer(uint32 peer_id)
{
	msgb_remove_receive_peer(peer_id);
	msgb_remove_send_peer(peer_id);
	elog(bdr_debug_level, "BDR msgbroker: peer %u removed", peer_id);
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
	elog(bdr_debug_level, "BDR msgbroker: stopped");
}

void
msgb_shmem_init(int max_local_nodes)
{
	msgb_shmem_init_receive(max_local_nodes);
}

static void
msgb_atexit(int code, Datum arg)
{
	msgb_shutdown();
}
