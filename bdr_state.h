#ifndef BDR_STATE_H
#define BDR_STATE_H

#include "datatype/timestamp.h"

#include "lib/stringinfo.h"

/*
 * This enumeration corresponds to the on-disk states in bdr.state_journal.
 * 
 * Values must not be changed once assigned and used in a public release,
 * so the enum is defined sparse.
 */
typedef enum BdrNodeState
{
	/* Never appears, reserved */
	BDR_NODE_STATE_UNUSED = 0,

	/*
	 * BDR node just created. It's not joined to a nodegroup.
	 */
	BDR_NODE_STATE_CREATED = 1,

	/*
	 * Join failure, oops.
	 */
	BDR_NODE_STATE_JOIN_FAILED = 2,

	/*
	 * Join phases are discrete states, so we can follow join progress in
	 * monitoring, make sure we recover after crashes, etc.
	 *
	 * JOIN_START is entered when the remote node definition
	 * and nodegroup definition is copied from a peer and we're ready
	 * to submit the join request.
	 */
	BDR_NODE_STATE_JOIN_START = 1000,

	/*
	 * Join request submitted to remote node. We're waiting for it to approve
	 * the request so we know for sure that we got our node id and name
	 * reserved by the node group.
	 */
	BDR_NODE_STATE_JOIN_WAIT_CONFIRM = 1010,

	/*
	 * The remote node has OK'd our join request, and we need to copy its
	 * nodes entries.
	 */
	BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES = 1020,

	/*
	 * Create replication slot on the join target
	 */
	BDR_NODE_STATE_JOIN_CREATE_TARGET_SLOT = 1025,

	/*
	 * This state is never actually entered. It's a marker for when the
	 * consensus system should be started during join. Anything
	 * prior to it has the consensus system suppressed, anything after it
	 * has it enabled.
	 */
	BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS = 1030,
	
	/*
	 * Create the subscription to the join target node, so we dump its
	 * db.
	 */
	BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET = 1040,

	/*
	 * Wait until the dump of the remote subscription finishes
	 */
	BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE = 1050,

	/*
	 * Create subscriptions to remaining peers. We do this
	 * in catchup phase because they'll be set to fast-forward
	 * only mode.
	 */
	BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS = 1055,

	/*
	 * Look up join target to find the minimum point we must replay past
	 * before we can promote.
	 */
	BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN = 1060,

	/*
	 * Our join request to the join target was successful, and we're in catchup
	 * mode streaming from the join target, waiting to pass the minimum
	 * replay lsn.
	 */
	BDR_NODE_STATE_JOIN_WAIT_CATCHUP = 1070,

	/*
	 * Copy replication set memberships from upstream tables to the
	 * downstream node.
	 */
	BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS = 1080,

	/*
	 * Create replication slots on peer nodes
	 */
	BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS = 1090,

	/*
	 * Wait for local repay to pass safe start point on all peer slots
	 */
	BDR_NODE_STATE_JOIN_WAIT_STANDBY_REPLAY = 1100,

	/*
	 * Tell all our peers that we've finished minimum catchup
	 * and can be promoted.
	 */
	BDR_NODE_STATE_SEND_STANDBY_READY = 1110,

	/*
	 * Potential steady state: standby, waiting for promotion
	 */
	BDR_NODE_STATE_STANDBY = 2000,

	/*
	 * Promoted and starting to go active
	 */
	BDR_NODE_STATE_PROMOTING = 2005,

	/*
	 * We've been promoted and are going full-active. Get a node-id for
	 * global sequences.
	 */
	BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID = 2010,

	/*
	 * We've requested a global sequence ID and obtained a request handle.
	 * Now we're waiting for the ID to be assigned.
	 *
	 * We can go back to BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID on nack
	 * to try again or continue to BDR_NODE_STATE_CREATE_LOCAL_SLOTS.
	 */
	BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID = 2020,

	/*
	 * We've been promoted and are going active.
	 *
	 * Create slots for peers to replay from us.
	 */
	BDR_NODE_STATE_CREATE_LOCAL_SLOTS = 2030,

	/*
	 * Announce that we've gone active (and set our node read/write)
	 *
	 * => BDR_NODE_STATE_ACTIVE
	 */
	BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE = 2040,

	/*
	 * This is a placeholder for the end of the range of join states.
	 * We never enter this state.
	 */
	BDR_NODE_STATE_JOIN_RANGE_END = 2999,


	/*
	 * Nodegroup is fully active as a participating member
	 *
	 * This state is a steady state, it won't transition out
	 * without an external influence from global messages,
	 * SQL functions, etc.
	 */
	BDR_NODE_STATE_ACTIVE = 5000,

	/*
	 * Placeholder for the end of the range of IDs used for active
	 * nodes.
	 */
	BDR_NODE_ACTIVE_RANGE_END = 5999

} BdrNodeState;

/*
 * The in-memory representation of a bdr.state_journal entry
 */
typedef struct BdrStateEntry
{
	uint32			counter;
	BdrNodeState	current;
	BdrNodeState	goal;
	TimestampTz		entered_time;
	/* if this state is associated with a global consensus operation */
	uint64			global_consensus_no;
	/* If this is state is associated with a join operation, the join target id */
	uint32			peer_id;
	/* A separate palloc'd struct of BdrNodeState-specific type, or NULL */
	void		   *extra_data;
} BdrStateEntry;

extern const char * bdr_node_state_name(BdrNodeState state);

extern const char * bdr_node_state_name_abbrev(BdrNodeState state);

extern void state_transition_goal(BdrStateEntry *state, BdrNodeState new_state,
	BdrNodeState new_goal, int64 consensus_no, uint32 peer_id,
	void *extradata);

extern void state_transition(BdrStateEntry *state, BdrNodeState new_state,
	void *extradata);

extern void state_extradata_serialize(StringInfo out, BdrNodeState new_state,
	void *extradata);

extern void* state_extradata_deserialize(StringInfo in, BdrNodeState state);

extern char* state_stringify_extradata(BdrStateEntry *state);

extern void state_get_expected(BdrStateEntry *state, bool for_update,
	bool want_extradata, BdrNodeState expected);

extern void bdr_state_insert_initial(BdrNodeState initial);

extern void bdr_state_dispatch(long *max_next_wait_msecs);

#endif
