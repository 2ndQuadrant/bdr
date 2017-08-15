/*-------------------------------------------------------------------------
 *
 * bdr_messaging.c
 * 		BDR message handling using consensus manager and message broker
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_messaging.c
 *
 * This is the BDR side of the messaging implementation. It uses the
 * consensus manager and the underlying message broker, both of which
 * are fairly BDR-indendent, to implement BDR state management and
 * inter-node messaging.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_messaging.h"

static bool bdr_msgs_receive(ConsensusMessage *msg);
static void bdr_msgs_enqueue(ConsensusMessage *msgs, int nmessages);
static bool bdr_msgs_prepare(ConsensusMessage *msg, int nmessages);
static void bdr_msgs_commit(ConsensusMessage *msg, int nmessages);
static void bdr_msgs_rollback(void);

void
bdr_start_consensus(int bdr_max_nodes)
{
	List	   *subs;
	ListCell   *lc;

    consensus_messages_receive_hook = bdr_msgs_receive;
	consensus_messages_prepare_hook = bdr_msgs_prepare;
	consensus_messages_commit_hook = bdr_msgs_commit;
	consensus_messages_rollback_hook = bdr_msgs_rollback;

	StartTransactionCommand();

	consensus_begin_startup(bdr_get_local_nodeid(), BDR_SCHEMA_NAME,
					  BDR_MSGJOURNAL_REL_NAME, bdr_max_nodes);

	subs = bdr_get_node_subscriptions(bdr_get_local_nodeid());

	foreach (lc, subs)
	{
		PGLogicalSubscription *sub = lfirst(lc);
		consensus_add_node(sub->target->id, sub->target_if->dsn);
	}

	consensus_finish_startup();

	CommitTransactionCommand();
}

void
bdr_shutdown_consensus(void)
{
    consensus_shutdown();
}

void
bdr_msgs_enqueue(ConsensusMessage *msgs, int nmessages)
{
	/*
	 * Enqueue the messages and don't bother waiting for results. We'll see
	 * them in the handler callbacks anyway.
	 *
	 * TODO: can we cut the status interface as wholly pointless?
	 */
	(void) consensus_enqueue_messages(msgs, nmessages);
}

static bool
bdr_msgs_receive(ConsensusMessage *msg)
{
	/* note, we receive our own messages too */
	elog(LOG, "XXX RECEIVE FROM %u", msg->sender_nodeid);

    /* TODO: can nack messages here */
    return true;
}

static bool
bdr_msgs_prepare(ConsensusMessage *msg, int nmessages)
{
	elog(LOG, "XXX PREPARE"); /* TODO */

    /* TODO: here's where we apply in-transaction state changes like insert nodes */

    /* TODO: can nack messages here too */
    return true;
}

static void
bdr_msgs_commit(ConsensusMessage *msg, int nmessages)
{
	elog(LOG, "XXX COMMIT"); /* TODO */

    /* TODO: here's where we allow state changes to take effect */
}

/* TODO add message-id ranges rejected */
static void
bdr_msgs_rollback(void)
{
	elog(LOG, "XXX ROLLBACK"); /* TODO */

    /* TODO: here's where we wind back any temporary state changes */
}
