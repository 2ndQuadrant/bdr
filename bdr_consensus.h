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
 */
typedef struct BdrMessage
{
    /* Message type determines meaning of payload */
    BdrMessageType	message_type;

	/* Node which sent this request. */
	uint32		origin_id;
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
	BdrMessage	msg;
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
	BdrMessage	msg;
	Oid			for_peer_id;	/* Peer expected to use the slot */
	const char *slot_name;		/* duh */
	XLogRecPtr	start_lsn;		/* Initial confirmed_flush_lsn */
} BdrMsgSlotCreated;

extern void wrapInStringInfo(StringInfo si, char *data, Size length);

extern void bdr_serialize_message(StringInfo s, BdrMessage *msg);
extern struct BdrMessage *msg_deserialize_message(StringInfo s);

extern void bdr_start_consensus(BdrNodeState cur_state);
extern void bdr_shutdown_consensus(void);
extern void bdr_consensus_refresh_nodes(BdrNodeState cur_state);

extern const char *bdr_message_type_to_string(BdrMessageType msgtype);

extern MNConsensusStatus bdr_consensus_request(BdrMessage *msg);
extern void bdr_consensus_request_enqueue(BdrMessage *msg);
extern MNConsensusResult *bdr_consensus_query(BdrMessage *msg);

#endif
