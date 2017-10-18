#ifndef BDR_CONSENSUS_H
#define BDR_CONSENSUS_H

#include "mn_consensus.h"
#include "bdr_state.h"

#define MAX_DIGITS_INT32 11
#define MAX_DIGITS_INT64 21

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
	BDR_MSG_NODE_STANDBY_READY,
	BDR_MSG_NODE_SLOT_CREATED,
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
    /* Globally unique, sequence value for this proposal/agreement */
	uint64			global_consensus_no;

    /* Node ID that proposed this BDR message */
    uint32			originator_id;

    /*
     * This send timestamp is different to ConsensusMessage.sender_timestamp;
     * that's the time the individual consensus exchange message was sent
     * whereas this is the time the BDR message was proposed to the system.
     */
    TimestampTz		originator_propose_time;

    /* Did this message require all-nodes consensus or just majority? */
    bool			majority_consensus_ok;

    /* Message type determines meaning of payload */
    BdrMessageType	message_type;

      /* Payload is a separate message struct, determined by message_type, or null */
    void		   *message;
} BdrMessage;

#define BdrMessageSize(msg) (offsetof(BdrMessage, payload) + msg->payload_length)

/*
 * A request to join a peer node.
 *
 * In some contexts not all fields are set; see
 * comments on usages.
 */
typedef struct BdrMsgJoinRequest
{
	const char *nodegroup_name;
	Oid			nodegroup_id;
	const char *joining_node_name;
	Oid			joining_node_id;
	int			joining_node_state;
	const char *joining_node_if_name;
	Oid			joining_node_if_id;
	const char *joining_node_if_dsn;
	const char *joining_node_dbname;
	const char *join_target_node_name;
	Oid			join_target_node_id;
} BdrMsgJoinRequest;

typedef struct BdrMsgSlotCreated
{
	Oid			for_peer_id;	/* Peer expected to use the slot */
	const char *slot_name;		/* duh */
	XLogRecPtr	start_lsn;		/* Initial confirmed_flush_lsn */
} BdrMsgSlotCreated;

extern void wrapInStringInfo(StringInfo si, char *data, Size length);

extern void msg_serialize_proposal(StringInfo out, BdrMessageType message_type, void* message);

extern struct BdrMessage* msg_deserialize_proposal(MNConsensusMessage *in);

extern uint64 bdr_consensus_enqueue_proposal(BdrMessageType message_type, void *message);

extern void bdr_start_consensus(BdrNodeState cur_state);
extern void bdr_shutdown_consensus(void);

void bdr_consensus_refresh_nodes(BdrNodeState cur_state);

extern void bdr_consensus_wait_event_set_recreated(WaitEventSet *new_set);
extern int bdr_consensus_get_wait_event_space_needed(void);

const char *bdr_message_type_to_string(BdrMessageType msgtype);

#endif
