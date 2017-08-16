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

    /* Node part/join management */
    BDR_MSG_NODE_NAME_RESERVE = 100,
    BDR_MSG_NODE_ID_SEQ_ALLOCATE,

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
 *
 * but this is what we actually care about: messages proposed by the current
 * node or peer nodes for BDR to act on to perform state changes to the
 * system.
 *
 * Note that on the wire, some of these fields are actually in the consensus
 * message in which this message is a payload; they're just copied to/from the
 * bdr message struct for convenience.
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

    /* Actual message contents are zero or more payload bytes */
    Size            payload_length;
    char            payload[FLEXIBLE_ARRAY_MEMBER];

} BdrMessage;

extern void bdr_start_consensus(int max_nodes);
extern void bdr_shutdown_consensus(void);

extern uint64 bdr_msgs_enqueue(BdrMessage *msgs, int nmessages);

extern ConsensusMessageStatus bdr_msg_get_outcome(uint64 msg_handle);

struct WaitEvent;
extern void bdr_messaging_wait_event(struct WaitEvent *events, int nevents);

struct WaitEventSet;
extern void bdr_messaging_wait_event_set_recreated(struct WaitEventSet *new_set);

#endif
