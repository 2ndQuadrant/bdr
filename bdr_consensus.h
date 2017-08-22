#ifndef BDR_CONSENSUS_H
#define BDR_CONSENSUS_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"

struct WaitEvent;

extern void consensus_add_node(uint32 nodeid, const char *dsn);

extern void consensus_alter_node(uint32 nodeid, const char *new_dsn);

extern void consensus_remove_node(uint32 nodeid);

extern void consensus_begin_startup(uint32 my_nodeid, const char * journal_schema,
	const char * journal_relname, int max_nodes);

void consensus_finish_startup(void);

extern void consensus_pump(struct WaitEvent *occurred_events, int nevents,
			   long *max_next_wait_ms);

extern void consensus_shutdown(void);

typedef struct ConsensusMessage {
	uint32 sender_nodeid;
	uint32 sender_local_msgnum;
	XLogRecPtr sender_lsn;
	TimestampTz sender_timestamp;
	uint32 global_message_id;
	uint16 message_type;
	Size payload_length;
	char payload[FLEXIBLE_ARRAY_MEMBER];
} ConsensusMessage;

/*
 * Start a batch of consensus messages to be delivered as a unit
 * when consensus_end_batch() is called.
 *
 * This will start a local database transaction. No transaction may already be
 * open, either a consensus transaction initiated by the local node or
 * a non-consensus-manager-related local DB transaction.
 *
 * If a consensus manager transaction initiated by a remote node is
 * active, returns false. Try again later.
 */
extern bool consensus_begin_enqueue(void);

/*
 * Add a message to a batch of messages on the send queue.
 *
 * There must be an open consensus message transaction from
 * consensus_begin_enqueue()
 *
 * A node must be prepared for its own messages to fail and be rolled back,
 * so it should treat them the same way it does remote messages in receive.
 * Don't do anything final until after the commit hook is called.
 *
 * The returned handle may be used to query the progress of the message.
 */
extern uint64 consensus_enqueue_message(const char *payload, Size payload_size);

/*
 * Finish preparing a set of messages and submit them as a unit.  This node and
 * all other peers will prepare and commit all, or none, of the messages.
 *
 * Nodes may receive only a subset if the txn is already aborted on some other
 * nodes.
 *
 * Returns a handle that can be used to query the progress of the submission.
 */
extern uint64 consensus_finish_enqueue(void);

typedef enum ConsensusMessageStatus {
	CONSENSUS_MESSAGE_IN_PROGRESS,
	CONSENSUS_MESSAGE_ACCEPTED,
	CONSENSUS_MESSAGE_FAILED
} ConsensusMessageStatus;

extern enum ConsensusMessageStatus consensus_messages_status(uint64 handle);

extern void consensus_messages_applied(uint32 applied_upto);

extern void consensus_messages_max_id(uint32 *max_applied, int32 *max_applyable);

extern struct ConsensusMessage* consensus_get_message(uint32 message_id);

/*
 * This hook is called when a message is received from the broker, before it is
 * inserted. It's also called for locally enqueued messages.
 *
 * The hook is not called for messages processed during crash recovery of
 * a prepared but not yet committed message set.
 *
 * A message may be rejected by this hook, causing it and all messages that are
 * part of the same message set to be nacked by this node. The xact will
 * be rolled back. consensus_messages_rollback_hook is not called, since
 * we never prepared the message set for commit.
 *
 * This hook is mainly for debugging/tracing. It should not have any state
 * side effects outside the transaction.
 *
 * XXX TODO XXX make the message broker support loopback connections via shmem
 * shortcut, and deliver-to-self? Simpler code path.
 */
typedef bool (*consensus_messages_receive_hooktype)(struct ConsensusMessage *message);

extern consensus_messages_receive_hooktype consensus_messages_receive_hook;

/*
 * This hook is called after a set of messages, already inserted into the
 * journal and passed to consensus_messages_receive_hook, is being prepared for
 * commit.  This is phase 1 in a 2PC process and is called with an open
 * transaction and the messages already inserted into the journal.
 *
 * It should return true if the xact should be prepared and an ack sent to
 * peers, or false if it should be rolled back and a nack sent to peers.
 * (A nack is still possible on true return if PREPARE TRANSATION its self
 * fails).
 *
 * The hook must prepare any database work needed to make the messages final
 * and ensure they can be committed when the commit hook is called. If the node
 * needs to reject a message due to some sort of conflict, this is its chance
 * to do so by raising a suitable ERROR.
 *
 * No final and irrevocable actions should be taken since the messages may
 * be rejected by another node and rolled back. Side effects that aren't
 * part of the transaction may be lost anyway, if we crash after prepare
 * and before commit.
 *
 * This hook should try to behave the same whether the messages originated on
 * this node or another node. Try to handle local changes by enqueuing messages
 * then reacting to them when received.
 *
 * It's guaranteed that either consensus_message_commit_hook or
 * consensus_messages_rollback_hook will be called after
 * consensus_message_prepare_hook and before any other prepare hook.
 */
typedef bool (*consensus_messages_prepare_hooktype)(List *messages);

extern consensus_messages_prepare_hooktype consensus_messages_prepare_hook;

/*
 * This hook is called once consensus_message_prepare_hook has returned
 * successfully, all peers have confirmed successful prepare, and the open
 * transaction has been locally committed. It reports completion of phase 2 of
 * the message exchange. The messages are now final on disk along with any
 * related work done by the prepare hook.
 *
 * The committed hook should set in progress any actions required to
 * make the messages take effect on the recipient system.
 *
 * It is possible for invocation of this hook to be skipped if the node crashes
 * after it does a local COMMIT PREPARED of an xact but before the hook is
 * invoked.
 *
 * Note that these messages may be locally or remotely originated. They may be
 * newly prepared in this session or may be recovered from after commit of a
 * previously prepared xact before a crash. Either way they must be treated
 * the same.
 */
typedef void (*consensus_messages_commit_hooktype)(List *messages);

extern consensus_messages_commit_hooktype consensus_messages_commit_hook;

/*
 * This hook is called instead of consensus_message_commit_hook if a peer node
 * rejects phase 1 (including our own node). The transaction from
 * consensus_message_prepare_hook has already been rolled back and phase 2 of
 * the message exchange has failed.
 *
 * It is NOT called for a normal rollback before prepare. Use regular transaction
 * hooks for that.
 *
 * The prepared xact has already rolled back, so the affected messages are
 * no longer known. This hook has mainly diagnostic value.
 *
 * It is possible for invocation of this hook to be skipped if the node crashes
 * after it does a local ROLLBACK PREPARED of an xact but before the hook is
 * invoked.
 *
 * TODO: call it with the message-id and origin node at least, should be able to
 * determine that much?
 */
typedef void (*consensus_messages_rollback_hooktype)(void);

extern consensus_messages_rollback_hooktype consensus_messages_rollback_hook;

#endif
