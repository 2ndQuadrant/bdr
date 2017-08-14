/*-------------------------------------------------------------------------
 *
 * bdr_consensus.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_consensus.c
 *
 * consensus negotiation using unreliable messaging
 *-------------------------------------------------------------------------
 *
 * The message broker provides a message transport, which this consensus module
 * uses to achieve reliable majority or all-nodes consensus on cluster state
 * transitions.
 */
#include "postgres.h"

#include "storage/latch.h"
#include "storage/ipc.h"

#include "bdr_msgbroker_receive.h"
#include "bdr_msgbroker_send.h"
#include "bdr_msgbroker.h"
#include "bdr_consensus.h"

/*
 * Size of shmem memory queues used for worker comms. Low load, can be small
 * ring buffers.
 */
#define CONSENSUS_MSG_QUEUE_SIZE 512

consensus_message_committed_hooktype consensus_message_committed_hook = NULL;

void
consensus_add_node(uint32 nodeid, const char *dsn)
{
	msgb_add_peer(nodeid, dsn);

	elog(WARNING, "not implemented");
}

void
consensus_alter_node(uint32 nodeid, const char *new_dsn)
{
	msgb_remove_peer(nodeid);
	msgb_add_peer(nodeid, new_dsn);

	elog(WARNING, "not implemented");
}

void
consensus_remove_node(uint32 nodeid)
{
	msgb_remove_peer(nodeid);

	elog(WARNING, "not implemented");
}

static void
consensus_received_msgb_message(uint32 origin, const char *payload, Size payload_size)
{
	/*
	 * Must unpack payload to get bdr message type per docs, and act on it
	 * based on consensus protocol.
	 */
	elog(WARNING, "not implemented");
}

void
consensus_startup(uint32 my_nodeid, const char * journal_schema,
	const char * journal_relname, int max_nodes)
{
	msgb_received_hook = consensus_received_msgb_message;
	msgb_startup(max_nodes, CONSENSUS_MSG_QUEUE_SIZE);
	elog(WARNING, "not implemented");
}

/*
 * Try to progress message exchange in response to a socket becoming
 * ready or our latch being set.
 *
 * Pass NULL as occurred_events and 0 as nevents if there are no wait events,
 * such as when a latch set triggers a queue pump.
 */
void
consensus_pump(struct WaitEvent *occurred_events, int nevents)
{
	msgb_service_connections(occurred_events, nevents);
}

void
consensus_shutdown(void)
{
	msgb_shutdown();
	elog(WARNING, "not implemented");
}

uint64
consensus_enqueue_messages(struct ConsensusMessage *messages,
					 int nmessages)
{
	elog(WARNING, "not implemented");
	return 0;
}

/*
enum ConsensusMessageStatus {
	CONSENSUS_MESSAGE_IN_PROGRESS,
	CONSENSUS_MESSAGE_ACCEPTED,
	CONSENSUS_MESSAGE_FAILED
};
*/

enum ConsensusMessageStatus
messages_status(uint64 handle)
{
	elog(WARNING, "not implemented");
	return CONSENSUS_MESSAGE_FAILED;
}

void
consensus_messages_applied(struct ConsensusMessage *upto_incl_message)
{
	elog(WARNING, "not implemented");
}

void
consensus_messages_max_id(uint32 *max_applied, int32 *max_applyable)
{
	elog(WARNING, "not implemented");
}

struct ConsensusMessage*
consensus_get_message(uint32 message_id)
{
	elog(WARNING, "not implemented");
	return NULL;
}
