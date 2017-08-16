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
 *
 * The consensus manager runs entirely in one bgworker. If other workers want
 * to enqueue messages, read them, etc, they must do so via the worker the
 * consensus manager runs in.
 *
 * (TODO: message fetch/confirm should be handled in multiple backends since we
 *  can just read message bodies from the journal and use a small static shmem
 *  area for the progress info and a lock. Enqueuing is could also work from
 *  any backend but we'd need IPC to send messages to the worker proc...)
 *
 * The module is used by enqueueing messages, pumping its state, and handling
 * the results with callbacks. Local state transitions that need to be agreed
 * to by peers should be accomplished by enqueueing messages then processing
 * them via the consensus system.
 *
 * The flow, where "Node X" may be the same node as "Our node" or some other
 * peer, is:
 *
 * [Node X]   consensus_enqueue_messages
 * [Our node]  consensus_messages_receive_hook
 * [Our node]  sends NACK or ACK
 * [Our node]  consensus_messages_receive_hook
 * [Our node]  sends NACK or ACK
 * ...
 * [Node X]   sends prepare
 * [Our node]  consensus_message_prepare_hook (unless locally aborted already)
 * [Our node]  PREPARE TRANSACTION
 * [Our node]  sends NACK or ACK based on outcome of hook+prepare
 *
 * Then if all nodes ACKed:
 *
 * [Node X]   sends commit
 * [Our node]  COMMIT PREPARED
 * [Our node]  consensus_message_commit_hook
 *
 * - or - if one or more nodes NACKed:
 *
 * [Node X]   sends rollback
 * [Our node]  ROLLBACK PREPARED
 * [Our node]  consensus_messages_rollback_hooktype
 *
 * During startup (including crash recovery) we may discover that we have
 * prepared transactions from the consensus system. If so we must recover them.
 * If they were locally originated we just send a rollback to all nodes. If
 * they were remotely originated we must ask the remote what the outcome should
 * be; if it already sent a commit, we can't rollback locally and vice versa.
 * Then based on the remote's response we either locally commit or rollback as
 * above. If it's a local commit, we commit then read the committed messages
 * back out of the journal to pass to the hook.
 *
 * FIXME: the current consensus module blindly assumes that all peers immediately
 * OK a message and invokes the callback as soon as the broker sends it. This is
 * obviously wrong, it's test skeleton code to exercise the broker and absolutely
 * minimal part/join.
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"

#include "lib/stringinfo.h"

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

consensus_messages_receive_hooktype consensus_messages_receive_hook = NULL;
consensus_messages_prepare_hooktype consensus_messages_prepare_hook = NULL;
consensus_messages_commit_hooktype consensus_messages_commit_hook = NULL;
consensus_messages_rollback_hooktype consensus_messages_rollback_hook = NULL;

static ConsensusNode *consensus_nodes = NULL;

static int consensus_max_nodes = 0;

static ConsensusMessage * consensus_deserialize_message(uint32 origin, const char *payload, Size payload_size);
static void consensus_serialize_message(StringInfo si, ConsensusMessage *msg);
static void consensus_insert_message(ConsensusMessage *message);
static void consensus_received_msgb_unformatted(ConsensusMessage *message);

static bool consensus_started_txn = false;

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

	/* TODO: handle any in progress message */
}

void
consensus_alter_node(uint32 node_id, const char *new_dsn)
{
	if (consensus_state >= CONSENSUS_STARTING)
		msgb_alter_peer(node_id, new_dsn);

	/* TODO: handle any in progress message */
}

void
consensus_remove_node(uint32 node_id)
{
	ConsensusNode *node;

	if (consensus_state >= CONSENSUS_STARTING)
		msgb_remove_peer(node_id);

	node = consensus_find_node_by_id(node_id, "removing from");
	node->node_id = 0;

	/* TODO: handle any in progress message */
}

/*
 * Insert a message into the persistent message journal, so we can call
 * bdr_messages_commit_hook again during crash recovery.
 */
static void
consensus_insert_message(ConsensusMessage *message)
{
	/* TODO */
}

/*
 * Callback invoked by message broker with the message we sent. We must
 * determine whether it's a new negotiation, progress in an existing one,
 * etc and act accordingly.
 */
static void
consensus_received_msgb_message(uint32 origin, const char *payload, Size payload_size)
{
	consensus_received_msgb_unformatted(consensus_deserialize_message(origin, payload, payload_size));
}

static void
consensus_received_msgb_unformatted(ConsensusMessage *message)
{
    /*
     * FIXME: a ConsensusMessage is the proposal for, and outcome of, a
     * negotiation. Not every message passed to this function will be a
     * ConsensusMessage and we'll need a prefix message type, some unpacking
     * logic for different kinds of message, etc. That way we'll be able
     * to do propose/ack/nack/prepare/rollback exchanges, etc.
     *
     * TODO: For now we're ignoring all the consensus protocol stuff and pretty
     * much bypassing this layer, sending the nearly raw messages we sent on
     * the wire to receivers. No synchronisation, confirmation all nodes
     * received, unpacking, etc. No message type header yet. Consequently the
     * receive side here is similarly basic.
	 *
	 * What we'll do here in the real version is insert each message and
	 * call a message received hook to ack/nack it, then reply with an ack/nack.
	 *
	 * When we get a commit we'll call the commit callback for all the messages
	 * together.
	 *
	 * But for now we just treat each message as committed as soon as it
	 * arrives.
	 */

    /*
     * TODO: Check if this is a new proposal, an ack/nack, a prepare request,
     * a commit or rollback, etc. Act accordingly.
     */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		consensus_started_txn = true;
	}
	else
	{
		/* cannot be called within a txn someone else started */
		Assert(consensus_started_txn);
	}

	if (consensus_messages_receive_hook != NULL)
		(*consensus_messages_receive_hook)(message);

	consensus_insert_message(message);

	(*consensus_messages_prepare_hook)(message, 1);

	/* TODO: 2PC here. Prepare the xact, exchange messages with peers, and come back when they have confirmed prepare too so we can commit: */

	CommitTransactionCommand();
	consensus_started_txn = false;

    /*
     * After commit we must pass the messages again to the commit hook; if
     * we're doing crash recovery after prepare but before commit it's the only
     * hook we'd call and it needs to know what's being recovered.
     */

	(*consensus_messages_commit_hook)(message, 1);
}

/*
 * Start bringing up the consensus system: init the message
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
	if (consensus_state != CONSENSUS_OFFLINE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("consensus system already running")));

	if (consensus_messages_prepare_hook == NULL
		|| consensus_messages_commit_hook == NULL
		|| consensus_messages_rollback_hook == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("consensus message hooks not properly registered by caller")));

	consensus_nodes = MemoryContextAlloc(TopMemoryContext,
										 sizeof(ConsensusNode) * max_nodes);
	memset(consensus_nodes, 0, sizeof(ConsensusNode) * max_nodes);

	msgb_received_hook = consensus_received_msgb_message;
	msgb_startup(max_nodes, CONSENSUS_MSG_QUEUE_SIZE);

	consensus_state = CONSENSUS_STARTING;

	consensus_max_nodes = max_nodes;

	elog(WARNING, "not fully implemented");
}

/*
 * consensus_begin_startup has been called and all peers have been added.
 *
 * Resolve any in-doubt message exchange and start accepting queue entries.
 */
void
consensus_finish_startup(void)
{
	consensus_state = CONSENSUS_RESOLVING_IN_DOUBT;

	/* TODO actually resolve things */

	consensus_state = CONSENSUS_ONLINE;
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
	/* TODO: some kind of graceful shutdown of active requests? */

	msgb_shutdown();

	consensus_state = CONSENSUS_OFFLINE;
	consensus_max_nodes = 0;
	pfree(consensus_nodes);
	consensus_nodes = NULL;
}


/*
 * Given a serialized message in wire format, unpack it into a
 * ConsensusMessage.
 */
static ConsensusMessage *
consensus_deserialize_message(uint32 origin, const char *payload, Size payload_size)
{
	/*
	 * FIXME, see consensus_serialize_message
	 */
	ConsensusMessage *message = palloc(payload_size);
	memcpy(message, payload, payload_size);
	return message;
}

static void
consensus_serialize_message(StringInfo si, ConsensusMessage *msg)
{
	/*
	 * FIXME: We shouldn't send the raw message struct
	 * but should instead use the message building code
	 * to prepare it properly. This is a hack to get the
	 * minimum functionality in place.
	 */
	appendBinaryStringInfo(si, (const char*)msg, offsetof(ConsensusMessage, payload) + msg->payload_length);
}

/*
 * Execute the consensus protocol for a batch of messages as a unit, such that
 * all or none are handled by peers.
 *
 * Returns immediately, before messages are sent let alone before they're
 * received and acted on. Use messages_status to check progress.
 *
 * May not be called from within an existing transaction, since the consensus
 * broker needs to handle a 2PC commit and doesn't want to get mixed up with
 * whatever else is already going on.
 */
uint64
consensus_enqueue_messages(struct ConsensusMessage *messages,
					 int nmessages)
{
	int msgidx, nodeidx;
	StringInfoData si;
	uint32 local_node_id = bdr_get_local_nodeid();
	XLogRecPtr cur_lsn = GetXLogWriteRecPtr();
	TimestampTz cur_ts = GetCurrentTimestamp();

	Assert(!IsTransactionState());

	initStringInfo(&si);

	/*
	 * TODO: right now, all we do is fire and forget to the broker and trust
	 * that all nodes will receive it. No attempt to handle failures is made.
	 * No attempt to achieve consensus is made. Obviously that's broken, it's
	 * just enough of a skeleton to let us start testing things out.
	 *
	 * We have to do distributed 2PC on this batch of messages, getting all
	 * nodes' confirmation that they could all be applied before committing.
	 * (Then later we'll upgrade to Raft or Paxos distributed majority
	 * consensus).
	 */
	for (msgidx = 0; msgidx < nmessages; msgidx++)
	{
		ConsensusMessage *msg = &messages[msgidx];

		msg->sender_nodeid = local_node_id;
		msg->sender_local_msgnum = 0; /* TODO (or caller supplied?) */
		msg->sender_lsn = cur_lsn;
		msg->sender_timestamp = cur_ts;
		msg->global_message_id = 0; /* TODO */

		/* message_type, payload and length set by caller */
		consensus_serialize_message(&si, msg);

		/* A sending node receives its own message immediately. */
		consensus_received_msgb_unformatted(msg);

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
	}

	/* TODO: return msg number */
	return 0;
}

/*
enum ConsensusMessageStatus {
	CONSENSUS_MESSAGE_IN_PROGRESS,
	CONSENSUS_MESSAGE_ACCEPTED,
	CONSENSUS_MESSAGE_FAILED
};
*/


/*
 * Given the handle of a message from when it was proposed for
 * delivery, look up its progress.
 */
enum ConsensusMessageStatus
consensus_messages_status(uint64 handle)
{
	elog(WARNING, "not implemented");
	return CONSENSUS_MESSAGE_FAILED;
}

/*
 * Tell the consensus system that messages up to id 'n' are applied and fully
 * acted on.
 *
 * The consensus sytem is also free to truncate them off the bottom of the
 * journal.
 *
 * It is an ERROR to try to confirm past the max applyable message per
 * consensus_messages_max_id.
 */
void
consensus_messages_applied(uint32 applied_upto)
{
    /* TODO: Needs shmem to coordinate apply progress */
	elog(WARNING, "not implemented");
}

/*
 * Find out how far ahead it's safe to request messages with
 * consensus_get_message and apply them. Also reports the consensus
 * manager's view of the last message applied by the system.
 *
 * During startup, call this to find out where to start applying messages. Then
 * fetch each message with consensus_get_message, act on it and call
 * consensus_messages_applied to advance the confirmation counter. When
 * max_applyable is reached, call consensus_messages_max_id again.
 */
void
consensus_messages_max_id(uint32 *max_applied, int32 *max_applyable)
{
    /* TODO: Needs shmem to coordinate apply progress */
	elog(WARNING, "not implemented");
}

/*
 * Given a globally message id (not a proposal handle), look up the message
 * and return it.
 *
 * If the message-id is 0, return the next message after what was
 * reported as applied to consensus_messages_applied(...).
 *
 * It is an ERROR to request a message greater than the max-applyable
 * message, even if later messages may be committed. (This won't happen
 * yet, but might once Raft/Paxos is added).
 *
 * It is an ERROR to request an uncommitted message, including PREPAREd
 * but not committed messages.
 *
 * It is an ERROR to request an already-applied message.
 */
struct ConsensusMessage*
consensus_get_message(uint32 message_id)
{
    /* TODO: Needs shmem to coordinate apply progress */
    /* TODO: read message directly from table */
	elog(WARNING, "not implemented");
	return NULL;
}
