#ifndef BDR_MESSAGING_H
#define BDR_MESSAGING_H

#include "bdr_consensus.h"

/*
 * All BDR subsystems use the BdrMessageType for their inter-node
 * communication. Messages get dispatched via the message module,
 * which knows which subsystems to send which messages to.
 *
 * Give subsystems their own ranges to allow for convenient expansion.
 *
 * (This could be decoupled into some kind of subsystem + subid, like
 * rmgr id + infomask flags is, but there's hardly any point).
 */
typedef enum BdrMessageType
{
    /* Just because we don't want anything real at zero */
    BDR_MSG_NOOP = 0,
	/* Pretty much just for testing, add a comment */
	BDR_MSG_COMMENT = 1,

    /* Node part/join management */
    BDR_MSG_NODE_JOIN_REQUEST = 100,
    BDR_MSG_NODE_ID_SEQ_ALLOCATE,
	BDR_MSG_NODE_CATCHUP_READY,
	BDR_MSG_NODE_ACTIVE,

    /* Global DDL locking */
    BDR_MSG_DDL_LOCK_REQUEST = 200,
    BDR_MSG_DDL_LOCK_GRANT,
    BDR_MSG_DDL_LOCK_REJECT,
    BDR_MSG_DDL_LOCK_RELEASE

} BdrMessageType;

/*
 * This is a BDR inter-node message for a given state change or activity.
 *
 * On the wire, you'll see, outer to inner, ignoring transport:
 *
 * message broker message:
 *  ... fields...
 *  payload: serialized ConsensusMessage
 *     ...fields specific to ConsensusMessage ...
 *     payload: serialized BdrMessage
 *       ... fields specific to BdrMessage ...
 *       payload:
 *         ... fields from BdrMessage message type ...
 *
 * but this struct encapsulates all the relevant fields from every level
 * and is what we work with when handling received messages.
 *
 * On the wire, some of these fields are actually in the consensus message in
 * which this message is a payload; they're just copied to/from the bdr message
 * struct for convenience.
 */
typedef struct BdrMessage
{
    /* Globally unique, ordered sequence value for this proposal/agreement */
    int             global_consensus_no;

    /* Node ID that proposed this BDR message */
    uint32          originator_id;

    /*
     * This send timestamp is different to ConsensusMessage.sender_timestamp;
     * that's the time the individual consensus exchange message was sent
     * whereas this is the time the BDR message was proposed to the system.
     */
    TimestampTz     originator_propose_time;

    /* Same here for the LSN */
    XLogRecPtr      originator_propose_lsn;

    /* Did this message require all-nodes consensus or just majority? */
    bool            majority_consensus_ok;

    /* Message type determines meaning of payload */
    BdrMessageType  message_type;

	/* Payload is a separate message struct, determined by message_type, or null */
	void*			message;
} BdrMessage;

#define BdrMessageSize(msg) (offsetof(BdrMessage, payload) + msg->payload_length)

extern void bdr_start_consensus(int max_nodes);
extern void bdr_shutdown_consensus(void);

/*
 * This API wraps bdr_consensus.c and adds IPC to allow submissions
 * from other backends.
 */
extern bool bdr_msgs_begin_enqueue(void);
extern uint64 bdr_msgs_enqueue(BdrMessageType message_type, void* message);
extern uint64 bdr_msgs_finish_enqueue(void);

static inline uint64
bdr_msgs_enqueue_one(BdrMessageType message_type, void *message)
{
	uint64 handle2;
	uint64 handle PG_USED_FOR_ASSERTS_ONLY;
	if (!bdr_msgs_begin_enqueue())
		return 0;
	handle = bdr_msgs_enqueue(message_type, message);
	handle2 = bdr_msgs_finish_enqueue();
	Assert(handle == handle2);
	return handle2;
}

extern ConsensusProposalStatus bdr_msg_get_outcome(uint64 msg_handle);

struct WaitEvent;
extern void bdr_messaging_wait_event(struct WaitEvent *events, int nevents,
									 long *max_next_wait_ms);

struct WaitEventSet;
extern void bdr_messaging_wait_event_set_recreated(struct WaitEventSet *new_set);

extern int bdr_messaging_get_wait_event_space_needed(void);

extern void bdr_messaging_refresh_nodes(void);
extern void bdr_messaging_add_peer(uint32 node_id, const char *dsn,
	bool update_if_found);
extern void bdr_messaging_remove_peer(uint32 node_id);

#endif
