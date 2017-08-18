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

#include "storage/latch.h"

#include "pglogical_worker.h"
#include "pglogical_plugins.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_msgbroker.h"
#include "bdr_msgbroker_send.h"
#include "bdr_messaging.h"

static bool bdr_msgs_receive(ConsensusMessage *msg);
static bool bdr_msgs_prepare(List *messages);
static void bdr_msgs_commit(List *messages);
static void bdr_msgs_rollback(void);
static void bdr_request_recreate_wait_event_set_hook(WaitEventSet *old_set, int required_entries);

void
bdr_start_consensus(int bdr_max_nodes)
{
	List	   *subs;
	ListCell   *lc;

	Assert(MyPGLogicalWorker != NULL
		   && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER);

	consensus_messages_receive_hook = bdr_msgs_receive;
	consensus_messages_prepare_hook = bdr_msgs_prepare;
	consensus_messages_commit_hook = bdr_msgs_commit;
	consensus_messages_rollback_hook = bdr_msgs_rollback;
	msgb_request_recreate_wait_event_set_hook = bdr_request_recreate_wait_event_set_hook;

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
bdr_msgs_begin_enqueue(void)
{
	if (MyPGLogicalWorker != NULL && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER)
		consensus_begin_enqueue();
	else
		elog(ERROR, "not implemented");
}

/*
 * Enqueue a message for processing on other peers.
 *
 * Returns a handle that can be used to determine when the final message in the
 * set is finalized and what the outcome was. Use bdr_msg_get_outcome(...)
 * to look up the status.
 */
uint64
bdr_msgs_enqueue(BdrMessage *message)
{
	if (MyPGLogicalWorker != NULL && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER)
	{
		/*
		 * No need for IPC to enqueue this message, we're on the manager
		 * already.
		 */
		return consensus_enqueue_message((const char*)message, BdrMessageSize(message));
	}
	else
	{
		/*
		 * TODO support messaging from other peers.
		 *
		 * We need to get the message to the manager worker to submit, then get the
		 * result back.
		 *
		 * One way is to use paired shm memory queues to exchange the message
		 * between bdr workers, normal backends requesting ddl locks, etc and the
		 * manager.
		 *
		 * Alternately we can use the journal table. We can't easily just have
		 * the consensus manager insert the row from the current backend; it'd
		 * still have to read it from the manager to send it on the wire via
		 * the message broker. The row would be uncommitted and we'd have to do
		 * a dirty read, worry about the initating proc dying (and telling
		 * peers about it), etc. The IPC requirement just moves from
		 * worker<->consensus to consensus<->broker, so it doesn't seem
		 * like a win.
		 *
		 * Either way, TODO.
		 */
		elog(ERROR, "not implemented");
	}
}

uint64
bdr_msgs_finish_enqueue(void)
{
	if (MyPGLogicalWorker != NULL && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER)
		return consensus_finish_enqueue();
	else
		elog(ERROR, "not implemented");
}

/*
 * Given a handle from bdr_msgs_enqueue, look up whether
 * a message is committed or not.
 */
ConsensusMessageStatus bdr_msg_get_outcome(uint64 msg_handle)
{
	/*
	 * TODO: For now only works in manager worker, see comments
	 * in bdr_consensus.c prelude for how to change that.
	 */
	Assert(MyPGLogicalWorker != NULL && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER);

	return consensus_messages_status(msg_handle);
}

static bool
bdr_msgs_receive(ConsensusMessage *msg)
{
	/* note, we receive our own messages too */
	elog(LOG, "XXX RECEIVE FROM %u, my id is %u",
		msg->sender_nodeid, bdr_get_local_nodeid());

    /* TODO: can nack messages here */
    return true;
}

static bool
bdr_msgs_prepare(List *messages)
{
	elog(LOG, "XXX PREPARE"); /* TODO */

    /* TODO: here's where we apply in-transaction state changes like insert nodes */

    /* TODO: can nack messages here too */
    return true;
}

static void
bdr_msgs_commit(List *messages)
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

void
bdr_messaging_wait_event(struct WaitEvent *events, int nevents)
{
	consensus_pump(events, nevents);
}

void
bdr_messaging_wait_event_set_recreated(struct WaitEventSet *new_set)
{
	if (!bdr_is_active_db())
		return;

	msgb_wait_event_set_recreated(new_set);
}

static void
bdr_request_recreate_wait_event_set_hook(WaitEventSet *old_set, int required_entries)
{
	/* TODO: pass required_entries */
	pglogical_manager_recreate_wait_event_set();
}
