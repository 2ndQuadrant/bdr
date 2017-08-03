#ifndef BDR_CONSENSUS_H
#define BDR_CONSENSUS_H

extern void consensus_add_node(uint32 nodeid, const char *dsn);

extern void consensus_alter_node(uint32 nodeid, const char *new_dsn);

extern void consensus_remove_node(uint32 nodeid);

extern void consensus_startup(uint32 my_nodeid, Name journal_schema,
	Name journal_relname, int max_nodes);

extern void consensus_pump(void);

extern void consensus_shutdown(void);

struct ConsensusMessage {
	uint32 sender_nodeid;
	uint32 sender_local_msgnum;
	uint64 sender_lsn;
	TimestampTz sender_timestamp;
	uint32 global_message_id;
	uint16 message_type;
	char payload[FLEXIBLE_ARRAY_MEMBER];
};

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

typedef void (*consensus_message_committed_hooktype)(void);

extern consensus_message_committed_hooktype consensus_message_committed_hook;

#endif
