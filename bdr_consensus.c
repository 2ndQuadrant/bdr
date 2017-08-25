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
 * The message broker provides a mssage transport, which this consensus module
 * uses to achieve reliable majority or all-nodes consensus on cluster state
 * transitions.
 *
 * The consensus manager runs entirely in one bgworker. If other workers want
 * to enqueue proposals, read them, etc, they must do so via the worker the
 * consensus manager runs in.
 *
 * (TODO: proposal fetch/confirm should be handled in multiple backends since we
 *  can just read proposal bodies from the journal and use a small static shmem
 *  area for the progress info and a lock. Enqueuing is could also work from
 *  any backend but we'd need IPC to send proposals to the worker proc...)
 *
 * The module is used by enqueueing proposals, pumping its state, and handling
 * the results with callbacks. Local state transitions that need to be agreed
 * to by peers should be accomplished by enqueueing proposals then processing
 * them via the consensus system.
 *
 * The flow, where "Node X" may be the same node as "Our node" or some other
 * peer, is:
 *
 * [Node X]   consensus_begin_enqueue
 * [Node X]   consensus_enqueue_proposal
 * [Our node]  consensus_proposals_receive_hook
 * [Our node]  sends NACK or ACK
 * [Our node]  consensus_proposals_receive_hook
 * [Our node]  sends NACK or ACK
 * ...
 * [Node X]   consensus_finish_enqueue
 * [Node X]   sends prepare
 * [Our node]  consensus_proposal_prepare_hook (unless locally aborted already)
 * [Our node]  PREPARE TRANSACTION
 * [Our node]  sends NACK or ACK based on outcome of hook+prepare
 *
 * Then if all nodes ACKed:
 *
 * [Node X]   sends commit
 * [Our node]  COMMIT PREPARED
 * [Our node]  consensus_proposal_commit_hook
 *
 * - or - if one or more nodes NACKed:
 *
 * [Node X]   sends rollback
 * [Our node]  ROLLBACK PREPARED
 * [Our node]  consensus_proposals_rollback_hooktype
 *
 * During startup (including crash recovery) we may discover that we have
 * prepared transactions from the consensus system. If so we must recover them.
 * If they were locally originated we just send a rollback to all nodes. If
 * they were remotely originated we must ask the remote what the outcome should
 * be; if it already sent a commit, we can't rollback locally and vice versa.
 * Then based on the remote's response we either locally commit or rollback as
 * above. If it's a local commit, we commit then read the committed proposals
 * back out of the journal to pass to the hook.
 *
 * FIXME: the current consensus module blindly assumes that all peers immediately
 * OK a proposal and invokes the callback as soon as the broker sends it. This is
 * obviously wrong, it's test skeleton code to exercise the broker and absolutely
 * minimal part/join.
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"

#include "lib/stringinfo.h"

#include "miscadmin.h"

#include "storage/latch.h"
#include "storage/ipc.h"

#include "utils/memutils.h"

#include "bdr_catcache.h"
#include "bdr_msgbroker_receive.h"
#include "bdr_msgbroker_send.h"
#include "bdr_msgbroker.h"
#include "bdr_worker.h"
#include "bdr_consensus.h"

/*
 * Size of shmem memory queues used for worker comms. Low load, can be small
 * ring buffers.
 */
#define CONSENSUS_MSG_QUEUE_SIZE 512

typedef enum ConsensusState {
	CONSENSUS_OFFLINE,
	CONSENSUS_STARTING,
	CONSENSUS_RESOLVING_IN_DOUBT,
	CONSENSUS_ONLINE
} ConsensusState;

/*
 * Knowledge of peers is kept in local memory on the consensus system. It's
 * expected that this will be maintained by the app based on some persistent
 * storage of nodes.
 */
typedef struct ConsensusNode {
	uint32 node_id;
} ConsensusNode;

static ConsensusState consensus_state = CONSENSUS_OFFLINE;

consensus_proposals_receive_hooktype consensus_proposals_receive_hook = NULL;
consensus_proposals_prepare_hooktype consensus_proposals_prepare_hook = NULL;
consensus_proposals_commit_hooktype consensus_proposals_commit_hook = NULL;
consensus_proposals_rollback_hooktype consensus_proposals_rollback_hook = NULL;

static ConsensusNode *consensus_nodes = NULL;

static int consensus_max_nodes = 0;

static ConsensusProposal * consensus_deserialize_proposal(uint32 origin, const char *payload, Size payload_size);
static void consensus_serialize_proposal(StringInfo si, ConsensusProposal *msg);
static void consensus_insert_proposal(ConsensusProposal *proposal);
static void consensus_received_new_proposal(ConsensusProposal *proposal);
static void consensus_prepare_transaction(void);
static void consensus_commit_prepared(void);
static void consensus_rollback_prepared(void);

static uint32 consensus_started_txn = 0;
static List *cur_txn_proposals = NIL;
static MemoryContext cur_txn_context = NULL;

static ConsensusNode *
consensus_find_node_by_id(uint32 node_id, const char * find_reason)
{
	int i;
	ConsensusNode *node = NULL;

	for (i = 0; i < consensus_max_nodes; i++)
		if (consensus_nodes[i].node_id == node_id)
			node = &consensus_nodes[i];

	if (node != NULL)
		return node;
	else
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("node %u not found while %s consensus manager",
				 		node_id, find_reason)));
}

void
consensus_add_node(uint32 node_id, const char *dsn)
{
	ConsensusNode *node = NULL;
	int first_free = -1;
	int i;

	if (node_id == bdr_get_local_nodeid())
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot add own node to consensus manager")));
	}

	if (consensus_state >= CONSENSUS_STARTING)
		msgb_add_peer(node_id, dsn);

	for (i = 0; i < consensus_max_nodes; i++)
	{
		if (consensus_nodes[i].node_id == node_id)
			node = &consensus_nodes[i];
		else if (consensus_nodes[i].node_id == 0 && first_free == -1)
			first_free = i;
	}

	if (node != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("node %u already registered in consensus manager",
				 		node_id)));

	node = &consensus_nodes[first_free];
	node->node_id = node_id;

	/* TODO: handle any in progress proposal */
}

void
consensus_alter_node(uint32 node_id, const char *new_dsn)
{
	if (consensus_state >= CONSENSUS_STARTING)
		msgb_alter_peer(node_id, new_dsn);

	/* TODO: handle any in progress proposal */
}

void
consensus_remove_node(uint32 node_id)
{
	ConsensusNode *node;

	if (consensus_state >= CONSENSUS_STARTING)
		msgb_remove_peer(node_id);

	node = consensus_find_node_by_id(node_id, "removing from");
	node->node_id = 0;

	/* TODO: handle any in progress proposal */
}

/*
 * Insert a proposal into the persistent proposal journal, so we can call
 * bdr_proposals_commit_hook again during crash recovery.
 */
static void
consensus_insert_proposal(ConsensusProposal *proposal)
{
	/* TODO */
}

/*
 * Callback invoked by message broker with the proposal we sent. We must
 * determine whether it's a new negotiation, progress in an existing one,
 * etc and act accordingly.
 */
static void
consensus_received_msgb_message(uint32 origin, const char *payload, Size payload_size)
{
    /*
     * TODO: Check if this is a new proposal, an ack/nack, a prepare request,
     * a commit or rollback, etc. Act accordingly.
     */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		consensus_started_txn = origin;
	}
	else if (consensus_started_txn == 0)
	{
		/* cannot be called within a txn someone else started; internal error */
		elog(ERROR, "attempt to start consensus receive within exiting txn");
	}
	else if (consensus_started_txn != origin)
	{
		/*
		 * The consensus system is processing a request from another node right
		 * now. We don't yet support any blocking/queuing here. Our node has
		 * already accepted the proposal, we can't ERROR on delivery. So we 
		 * must dispatch our own NACK as a reply.
		 */

		/*
		 * TODO: send NACK instead of ignoring proposal
		 */
		elog(WARNING, "ignoring proposal from %u due to existing local consensus transaction from %u",
			 origin, consensus_started_txn);
		return;
	}

	/* TODO: handle 2pc prepare and commit requests from peer too */
	/* by invoking consensus_prepare_transaction, consensus_commit_prepared, consensus_rollback_prepared */

	consensus_received_new_proposal(consensus_deserialize_proposal(origin, payload, payload_size));
    
	/*
     * TODO: For now we're ignoring all the consensus protocol stuff and pretty
     * much bypassing this layer, sending the nearly raw proposals we sent on
     * the wire to receivers. No synchronisation, confirmation all nodes
     * received, unpacking, etc. No proposal type header yet. Consequently the
     * receive side here is similarly basic.
	 *
	 * What we'll do here in the real version is insert each proposal and
	 * call a proposal received hook to ack/nack it, then reply with an ack/nack.
	 *
	 * When we get a commit we'll call the commit callback for all the proposals
	 * together.
	 *
	 * But for now we just treat each proposal as committed as soon as it
	 * arrives.
	 */
	consensus_prepare_transaction();
}

static void
consensus_received_new_proposal(ConsensusProposal *proposal)
{
	MemoryContext old_ctx;

    /*
     * FIXME: a ConsensusProposal is the proposal for, and outcome of, a
     * negotiation. Not every message passed to this function will be a
     * ConsensusProposal. We need a ConsensusMessage that wraps ConsensusProposal
     * with a message type field (ack/nack etc).
     */
	Assert(IsTransactionState() && consensus_started_txn);

	if (consensus_proposals_receive_hook != NULL)
		(*consensus_proposals_receive_hook)(proposal);

	consensus_insert_proposal(proposal);

	old_ctx = MemoryContextSwitchTo(cur_txn_context);
	cur_txn_proposals = lappend(cur_txn_proposals, proposal);
	(void) MemoryContextSwitchTo(old_ctx);
}

static void
consensus_prepare_transaction(void)
{
	(*consensus_proposals_prepare_hook)(cur_txn_proposals);

	/* TODO: 2PC here. Prepare the xact, exchange proposals with peers, and come back when they have confirmed prepare too so we can commit: */

	/*
	 * TODO: this commit should happen in response to 2PC consensus, not
	 * immediately here. If all peers agreed we should commit then send
	 * a commit to peers, and not before.
	 */
	consensus_commit_prepared();
}

static void
consensus_commit_prepared(void)
{
	/*
	 * TODO: do real 2PC here
	 *
	 * This func should be called in response to a remote initiated commit-prepared proposal, or
	 * when we detect that a locally submitted consensus request has been acked by all peers.
	 * (Or later a quorum once we do raft/paxos).
	 */
	CommitTransactionCommand();
	consensus_started_txn = 0;

    /*
     * After commit we must pass the proposals again to the commit hook; if
     * we're doing crash recovery after prepare but before commit it's the only
     * hook we'd call and it needs to know what's being recovered.
	 *
	 * TODO: re-read the proposals from the table
     */
	(*consensus_proposals_commit_hook)(cur_txn_proposals);

	MemoryContextReset(cur_txn_context);
	cur_txn_proposals = NIL;
}

static void
consensus_rollback_prepared(void)
{
	AbortCurrentTransaction();
	consensus_started_txn = 0;

	(*consensus_proposals_rollback_hook)();

	cur_txn_proposals = NIL;
	MemoryContextReset(cur_txn_context);
}

/*
 * Start bringing up the consensus system: init the proposal
 * broker, etc.
 *
 * The caller must now add all the currently known-active
 * peer nodes that participate in the consensus group, then
 * call consensus_finish_startup();
 */
void
consensus_begin_startup(uint32 my_nodeid,
	const char * journal_schema,
	const char * journal_relname,
	int max_nodes)
{
	elog(bdr_debug_level, "BDR consensus system starting up");

	if (consensus_state != CONSENSUS_OFFLINE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("consensus system already running")));

	if (consensus_proposals_prepare_hook == NULL
		|| consensus_proposals_commit_hook == NULL
		|| consensus_proposals_rollback_hook == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("consensus proposal hooks not properly registered by caller")));

	consensus_nodes = MemoryContextAlloc(TopMemoryContext,
										 sizeof(ConsensusNode) * max_nodes);
	memset(consensus_nodes, 0, sizeof(ConsensusNode) * max_nodes);

	msgb_received_hook = consensus_received_msgb_message;
	msgb_startup(max_nodes, CONSENSUS_MSG_QUEUE_SIZE);

	consensus_state = CONSENSUS_STARTING;

	/*
	 * We need a memory context that lives longer than the current transaction
	 * so we can pass the consensus proposals to the prepared and committed
	 * hooks before freeing them.
	 */
	cur_txn_context = AllocSetContextCreate(TopMemoryContext,
								"consensus transaction",
								ALLOCSET_DEFAULT_SIZES);

	consensus_max_nodes = max_nodes;
	consensus_started_txn = 0;
}

/*
 * consensus_begin_startup has been called and all peers have been added.
 *
 * Resolve any in-doubt proposal exchange and start accepting queue entries.
 */
void
consensus_finish_startup(void)
{
	consensus_state = CONSENSUS_RESOLVING_IN_DOUBT;

	/* TODO actually resolve things */

	consensus_state = CONSENSUS_ONLINE;

	elog(bdr_debug_level, "BDR consensus system started up");
}

/*
 * Try to progress proposal exchange in response to a socket becoming
 * ready or our latch being set.
 *
 * Pass NULL as occurred_events and 0 as nevents if there are no wait events,
 * such as when a latch set triggers a queue pump.
 */
void
consensus_pump(struct WaitEvent *occurred_events, int nevents,
			   long *max_next_wait_ms)
{
	msgb_service_connections(occurred_events, nevents, max_next_wait_ms);
	CHECK_FOR_INTERRUPTS();
}

void
consensus_shutdown(void)
{
	/* TODO: some kind of graceful shutdown of active requests? */

	msgb_shutdown();

	if (cur_txn_context != NULL)
	{
		MemoryContextDelete(cur_txn_context);
		cur_txn_context = NULL;
	}

	cur_txn_proposals = NIL;

	consensus_state = CONSENSUS_OFFLINE;
	consensus_max_nodes = 0;
	if (consensus_nodes != NULL)
	{
		pfree(consensus_nodes);
		consensus_nodes = NULL;
	}

	consensus_started_txn = 0;
}


/*
 * Given a serialized proposal in wire format, unpack it into a
 * ConsensusProposal.
 */
static ConsensusProposal *
consensus_deserialize_proposal(uint32 origin, const char *payload, Size payload_size)
{
	/*
	 * FIXME, see consensus_serialize_proposal
	 */
	ConsensusProposal *proposal = MemoryContextAlloc(cur_txn_context, payload_size);
	memcpy(proposal, payload, payload_size);
	return proposal;
}

static void
consensus_serialize_proposal(StringInfo si, ConsensusProposal *msg)
{
	/*
	 * FIXME: We shouldn't send the raw proposal struct
	 * but should instead use the proposal building code
	 * to prepare it properly. This is a hack to get the
	 * minimum functionality in place.
	 *
	 * Proposals should be wrapped in a ConsensusMessage that's packed on send
	 * and unpacked on receive.  We should only send the proposal fields that
	 * aren't part of the ConsensusMessage. The ConsensusMessage should carry
	 * the message type (ack/nack etc) and optional payload
	 */
	appendBinaryStringInfo(si, (const char*)msg, offsetof(ConsensusProposal, payload) + msg->payload_length);
}

/*
 * Start a batch of consensus proposals to be delivered as a unit
 * when consensus_end_batch() is called.
 *
 * If a consensus manager transaction initiated by a remote node is
 * active, returns false. Try again later.
 */
bool
consensus_begin_enqueue(void)
{
	if (IsTransactionState())
	{
		if (consensus_started_txn == bdr_get_local_nodeid())
		{
			/*
			 * This isn't an error condition because another backend could've
			 * started the transaction. Only one can win and start it
			 * successfully if multiple backends try at once. We can't just
			 * block because we'd hold up the whole event loop.
			 *
			 * TODO: some shmem signalling so backends can negotiate this more nicely. Seems like another good reason to move the MQ's into consensus manager.
			 */
			elog(bdr_debug_level, "consensus transaction already open by local node");
			return false;
		}
		else if (consensus_started_txn != 0)
		{
			elog(bdr_debug_level, "consensus transaction already open by %u",
				 consensus_started_txn);
			return false;
		}
		else
			elog(WARNING, "database transaction already open that was not started by consensus manager");
			return false;
	}

	Assert(consensus_started_txn == 0);
	Assert(!IsTransactionState());
	StartTransactionCommand();
	consensus_started_txn = bdr_get_local_nodeid();

	return true;
}

/*
 * Add a proposal to a batch of proposals on the send queue.
 *
 * There must be an open consensus proposal transaction from
 * consensus_begin_enqueue()
 *
 * A node must be prepared for its own proposals to fail and be rolled back,
 * so it should treat them the same way it does remote proposals in receive.
 * Don't do anything final until after the commit hook is called.
 *
 * The returned handle may be used to query the progress of the proposal.
 */
uint64
consensus_enqueue_proposal(const char * payload, Size payload_length)
{
	int nodeidx;
	StringInfoData si;
	ConsensusProposal *msg;
	Size msg_size;

	Assert(IsTransactionState() && consensus_started_txn == bdr_get_local_nodeid());

	/*
	 * TODO: here we should check if our consensus txn is already nack'd
	 * by a peer and if so, reject this submit.
	 */

	/*
	 * TODO: right now, all we do is commit locally then fire and forget to the
	 * broker and trust that all nodes will receive it. No attempt to handle
	 * failures is made.  No attempt to achieve consensus is made. Obviously
	 * that's broken, it's just enough of a skeleton to let us start testing
	 * things out.
	 *
	 * We have to do distributed 2PC on this batch of proposals, getting all
	 * nodes' confirmation that they could all be applied before committing.
	 * (Then later we'll upgrade to Raft or Paxos distributed majority
	 * consensus).
	 */

	msg_size = offsetof(ConsensusProposal, payload) + payload_length;
	msg = MemoryContextAlloc(cur_txn_context, msg_size);
	msg->sender_nodeid = bdr_get_local_nodeid();
	msg->sender_local_msgnum = 0; /* TODO (or caller supplied?) */
	msg->sender_lsn = GetXLogWriteRecPtr();
	msg->sender_timestamp = GetCurrentTimestamp();
	msg->global_proposal_id = 0; /* TODO */
	memcpy(msg->payload, payload, payload_length);
	msg->payload_length = payload_length;

	/* payload and length set by caller */
	initStringInfo(&si);
	consensus_serialize_proposal(&si, msg);

	/* A sending node receives its own proposal immediately. */
	consensus_received_new_proposal(msg);

	for (nodeidx = 0; nodeidx < consensus_max_nodes; nodeidx++)
	{
		ConsensusNode * const node = &consensus_nodes[nodeidx];
		if (node->node_id == 0)
			continue;
		
		/*
		 * TODO: We could possibly have a smarter api here where message
		 * data is shared across all recipients when a message id
		 * dispatched to a list of recipients. But that'd be hard to manage
		 * memory and failure for. THis wastes memory temporarily, but
		 * it's simpler.
		 */
		msgb_queue_message(node->node_id, si.data, si.len);
	}
	resetStringInfo(&si);

	/* TODO: return msg number */
	return 0;
}

/*
 * Finish preparing a set of proposals and submit them as a unit.  This node and
 * all other peers will prepare and commit all, or none, of the proposals.
 *
 * Nodes may receive only a subset if the txn is already aborted on some other
 * nodes.
 *
 * Returns a handle that can be used to query the progress of the submission.
 */
uint64
consensus_finish_enqueue(void)
{
	Assert(IsTransactionState() && consensus_started_txn == bdr_get_local_nodeid());

	/*
	 * All local proposals have been passed through the local receive hook and
	 * have been submitted to the broker for sending to peers. Prepare the
	 * transaction locally.
	 *
	 * This should do the full 2PC dance and commit only once all peers have
	 * acked.
	 *
	 * TODO: send out prepare to peers too
	 */
	consensus_prepare_transaction();

	/* TODO: return last msg number */
	return 0;
}

/*
enum ConsensusProposalStatus {
	CONSENSUS_MESSAGE_IN_PROGRESS,
	CONSENSUS_MESSAGE_ACCEPTED,
	CONSENSUS_MESSAGE_FAILED
};
*/


/*
 * Given the handle of a proposal from when it was proposed for
 * delivery, look up its progress.
 */
enum ConsensusProposalStatus
consensus_proposals_status(uint64 handle)
{
	elog(WARNING, "not implemented");
	return CONSENSUS_MESSAGE_FAILED;
}

/*
 * Tell the consensus system that proposals up to id 'n' are applied and fully
 * acted on.
 *
 * The consensus sytem is also free to truncate them off the bottom of the
 * journal.
 *
 * It is an ERROR to try to confirm past the max applyable proposal per
 * consensus_proposals_max_id.
 */
void
consensus_proposals_applied(uint32 applied_upto)
{
    /* TODO: Needs shmem to coordinate apply progress */
	elog(WARNING, "not implemented");
}

/*
 * Find out how far ahead it's safe to request proposals with
 * consensus_get_proposal and apply them. Also reports the consensus
 * manager's view of the last proposal applied by the system.
 *
 * During startup, call this to find out where to start applying proposals. Then
 * fetch each proposal with consensus_get_proposal, act on it and call
 * consensus_proposals_applied to advance the confirmation counter. When
 * max_applyable is reached, call consensus_proposals_max_id again.
 */
void
consensus_proposals_max_id(uint32 *max_applied, int32 *max_applyable)
{
    /* TODO: Needs shmem to coordinate apply progress */
	elog(WARNING, "not implemented");
}

/*
 * Given a globally proposal id (not a proposal handle), look up the proposal
 * and return it.
 *
 * If the proposal-id is 0, return the next proposal after what was
 * reported as applied to consensus_proposals_applied(...).
 *
 * It is an ERROR to request a proposal greater than the max-applyable
 * proposal, even if later proposals may be committed. (This won't happen
 * yet, but might once Raft/Paxos is added).
 *
 * It is an ERROR to request an uncommitted proposal, including PREPAREd
 * but not committed proposals.
 *
 * It is an ERROR to request an already-applied proposal.
 */
struct ConsensusProposal*
consensus_get_proposal(uint32 proposal_id)
{
    /* TODO: Needs shmem to coordinate apply progress */
    /* TODO: read proposal directly from table */
	elog(WARNING, "not implemented");
	return NULL;
}
