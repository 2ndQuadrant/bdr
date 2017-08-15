#ifndef BDR_CONSENSUS_H
#define BDR_CONSENSUS_H

struct WaitEvent;

extern void consensus_add_node(uint32 nodeid, const char *dsn);

extern void consensus_alter_node(uint32 nodeid, const char *new_dsn);

extern void consensus_remove_node(uint32 nodeid);

extern void consensus_begin_startup(uint32 my_nodeid, const char * journal_schema,
	const char * journal_relname, int max_nodes);

void consensus_finish_startup(void);

extern void consensus_pump(struct WaitEvent *occurred_events, int nevents);

extern void consensus_shutdown(void);

typedef struct ConsensusMessage {
	uint32 sender_nodeid;
	uint32 sender_local_msgnum;
	uint64 sender_lsn;
	TimestampTz sender_timestamp;
	uint32 global_message_id;
	uint16 message_type;
	Size payload_length;
	char payload[FLEXIBLE_ARRAY_MEMBER];
} ConsensusMessage;

extern uint64 consensus_enqueue_messages(struct ConsensusMessage *messages,
					 int nmessages);

enum ConsensusMessageStatus {
	CONSENSUS_MESSAGE_IN_PROGRESS,
	CONSENSUS_MESSAGE_ACCEPTED,
	CONSENSUS_MESSAGE_FAILED
};

extern enum ConsensusMessageStatus messages_status(uint64 handle);

extern void consensus_messages_applied(struct ConsensusMessage *upto_incl_message);

extern void consensus_messages_max_id(uint32 *max_applied, int32 *max_applyable);

extern struct ConsensusMessage* consensus_get_message(uint32 message_id);

/*
 * This hook is called when a message is received from the broker, before it is
 * inserted.
 *
 * The recipient may ERROR here if it knows it cannot honour the message.
 * Otherwise it may prepare any needed work within the transaction so that
 * the message can be acted on once committed. Or that work can be left
 * until the prepare hook is passed all the messages.
 *
 * This hook is mainly for debugging/tracing. It MUST not have any side effects
 * outside the transaction.
 */
typedef void (*consensus_messages_receive_hooktype)(struct ConsensusMessage *message);

extern consensus_messages_receive_hooktype consensus_messages_receive_hook;

/*
 * This hook is called after a set of messages, already inserted into the
 * journal and passed to consensus_messages_receive_hook, is being prepared for
 * commit.  This is phase 1 in a 2PC process.
 *
 * Called with an open transaction, which will be PREPAREd after this hook
 * returns.
 *
 * The hook must prepare any database work needed to make the messages final
 * and ensure they can be committed when the commit hook is called. If the node
 * needs to reject a message due to some sort of conflict, this is its chance
 * to do so by raising a suitable ERROR.
 *
 * No final and irrevocable actions should be taken since the messages may
 * be rejected by another node and rolled back.
 *
 * It's guaranteed that either consensus_message_commit_hook or
 * consensus_messages_rollback_hook will be called after
 * consensus_message_prepare_hook and before any other prepare hook.
 */
typedef void (*consensus_messages_prepare_hooktype)(struct ConsensusMessage *messages, int nmessages);

extern consensus_messages_prepare_hooktype consensus_message_prepare_hook;

/*
 * This hook is called once consensus_message_prepare_hook has returned
 * successfully, all peers have confirmed successful prepare, and the open
 * transaction has been locally committed. It reports completion of phase 2 of
 * the message exchange. The messages are now final on disk along with any
 * related work done by the prepare hook.
 *
 * The committed hook should set in progress any actions required to
 * make the messages take effect on the recipient system.
 */
typedef void (*consensus_messages_commit_hooktype)();

extern consensus_messages_commit_hooktype consensus_message_commit_hook;

/*
 * This hook is called instead of consensus_message_commit_hook if a peer node
 * rejects phase 1. The transaction from consensus_message_prepare_hook has
 * already been rolled back and phase 2 of the message exchange has failed.
 *
 * The recipient shouldn't generally need to do anything here because it
 * didn't do anything final in the prepare hook, but sometimes reality
 * differs.
 */
typedef void (*consensus_messages_rollback_hooktype)(void);

extern consensus_messages_rollback_hooktype consensus_messages_rollback_hook;

#endif
