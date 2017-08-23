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

static bool bdr_proposals_receive(ConsensusProposal *msg);
static bool bdr_proposals_prepare(List *messages);
static void bdr_proposals_commit(List *messages);
static void bdr_proposals_rollback(void);
static void bdr_request_recreate_wait_event_set_hook(WaitEventSet *old_set);

void
bdr_start_consensus(int bdr_max_nodes)
{
	List	   *subs;
	ListCell   *lc;
	StringInfoData si;

	Assert(MyPGLogicalWorker != NULL
		   && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER);

	initStringInfo(&si);

	consensus_proposals_receive_hook = bdr_proposals_receive;
	consensus_proposals_prepare_hook = bdr_proposals_prepare;
	consensus_proposals_commit_hook = bdr_proposals_commit;
	consensus_proposals_rollback_hook = bdr_proposals_rollback;
	msgb_request_recreate_wait_event_set_hook = bdr_request_recreate_wait_event_set_hook;

	Assert(!IsTransactionState());
	StartTransactionCommand();

	consensus_begin_startup(bdr_get_local_nodeid(), BDR_SCHEMA_NAME,
					  BDR_MSGJOURNAL_REL_NAME, bdr_max_nodes);

	subs = bdr_get_node_subscriptions(bdr_get_local_nodeid());

	foreach (lc, subs)
	{
		PGLogicalSubscription *sub = lfirst(lc);
		resetStringInfo(&si);
		appendStringInfoString(&si, sub->origin_if->dsn);
		appendStringInfo(&si, " application_name='bdr_msgbroker %i'",
						 bdr_get_local_nodeid());
		Assert(sub->target->id == bdr_get_local_nodeid());
		consensus_add_node(sub->origin->id, sub->origin_if->dsn);
	}

	consensus_finish_startup();

	pfree(si.data);

	CommitTransactionCommand();
}

void
bdr_shutdown_consensus(void)
{
    consensus_shutdown();
}

bool
bdr_msgs_begin_enqueue(void)
{
	if (MyPGLogicalWorker != NULL && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER)
		return consensus_begin_enqueue();
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
		return consensus_enqueue_proposal((const char*)message, BdrMessageSize(message));
	}
	else
	{
		/*
		 * We're not running in the manager. So we need IPC to get the message to the
		 * consensus manager in the pglogical manager worker.
		 *
		 * The submitting backend could be a bgworker or a normal Pg backend.
		 *
		 * To handle this we keep a small pool of shm_mq's (actually, currently just one!)
		 * owned by the BDR plugin's messaging module, and we look it up via static
		 * shmem. (This looks very much like how the message broker's receiver works).
		 * We attach to paired send and receive queues, provided then do our business
		 * and detach.
		 */
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
 * Given a handle from bdr_proposals_enqueue, look up whether
 * a message is committed or not.
 */
ConsensusProposalStatus bdr_msg_get_outcome(uint64 msg_handle)
{
	/*
	 * TODO: For now only works in manager worker, see comments
	 * in bdr_consensus.c prelude for how to change that.
	 */
	Assert(MyPGLogicalWorker != NULL && MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER);

	return consensus_proposals_status(msg_handle);
}

static bool
bdr_proposals_receive(ConsensusProposal *msg)
{
	/* note, we receive our own messages too */
	elog(LOG, "XXX RECEIVE FROM %u, my id is %u",
		msg->sender_nodeid, bdr_get_local_nodeid());

    /* TODO: can nack messages here */
    return true;
}

static bool
bdr_proposals_prepare(List *messages)
{
	elog(LOG, "XXX PREPARE"); /* TODO */

    /* TODO: here's where we apply in-transaction state changes like insert nodes */

    /* TODO: can nack messages here too */
    return true;
}

static void
bdr_proposals_commit(List *messages)
{
	elog(LOG, "XXX COMMIT"); /* TODO */

    /* TODO: here's where we allow state changes to take effect */
}

/* TODO add message-id ranges rejected */
static void
bdr_proposals_rollback(void)
{
	elog(LOG, "XXX ROLLBACK"); /* TODO */

    /* TODO: here's where we wind back any temporary state changes */
}

void
bdr_messaging_wait_event(struct WaitEvent *events, int nevents,
						 long *max_next_wait_ms)
{
	consensus_pump(events, nevents, max_next_wait_ms);
}

void
bdr_messaging_wait_event_set_recreated(struct WaitEventSet *new_set)
{
	if (!bdr_is_active_db())
		return;

	msgb_wait_event_set_recreated(new_set);
}

static void
bdr_request_recreate_wait_event_set_hook(WaitEventSet *old_set)
{
	pglogical_manager_recreate_wait_event_set();
}

int
bdr_get_wait_event_space_needed(void)
{
	return msgb_get_wait_event_space_needed();
}
