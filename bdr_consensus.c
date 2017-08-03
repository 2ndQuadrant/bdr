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

#include "bdr_msgbroker.h"
#include "bdr_consensus.h"

consensus_message_committed_hooktype consensus_message_committed_hook = NULL;

void
consensus_add_node(uint32 nodeid, const char *dsn)
{
	msgb_add_destination(nodeid, dsn);
}

void
consensus_alter_node(uint32 nodeid, const char *new_dsn)
{
	msgb_remove_destination(nodeid);
	msgb_add_destination(nodeid, new_dsn);
}

void consensus_remove_node(uint32 nodeid)
{
	msgb_remove_destination(nodeid);
}

void
consensus_startup(uint32 my_nodeid, Name journal_schema,
	Name journal_relname, int max_nodes)
{
	msgb_startup(max_nodes);
}

void
consensus_pump(void)
{
}

void
consensus_shutdown(void)
{
}

uint64
consensus_enqueue_messages(struct ConsensusMessage *messages,
					 int nmessages)
{
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
}

void
consensus_messages_applied(struct ConsensusMessage *upto_incl_message)
{
}

void
consensus_messages_max_id(uint32 *max_applied, int32 *max_applyable)
{
}

struct ConsensusMessage*
consensus_get_message(uint32 message_id)
{
}
