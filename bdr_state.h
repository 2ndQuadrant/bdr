#ifndef BDR_STATE_H
#define BDR_STATE_H

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
	BDR_NODE_STATE_JOIN_START = 100,

	/*
	 * Join request submitted to remote node. We're waiting for it to approve
	 * the request so we know for sure that we got our node id and name
	 * reserved by the node group.
	 */
	BDR_NODE_STATE_JOIN_WAIT_CONFIRM,

	/*
	 * The remote node has OK'd our join request, and we need to copy its
	 * nodes entries.
	 */
	BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES,

	/*
	 * This state is never actually entered. It's a marker for when the
	 * consensus system should be started during join. Anything
	 * prior to it has the consensus system suppressed, anything after it
	 * has it enabled.
	 */
	BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS,
	
	/*
	 * Create the subscription to the join target node, so we dump its
	 * db.
	 */
	BDR_NODE_JOIN_SUBSCRIBE_JOIN_TARGET,

	/*
	 * Wait until the dump of the remote subscription finishes
	 */
	BDR_NODE_STATE_WAIT_SUBSCRIBE_COMPLETE,

	/*
	 * Look up join target to find the minimum point we must replay past
	 * before we can promote.
	 */
	BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN,

	/*
	 * Our join request to the join target was successful, and we're in catchup
	 * mode streaming from the join target, waiting to pass the minimum
	 * replay lsn.
	 */
	BDR_NODE_STATE_JOIN_WAIT_CATCHUP,

	/*
	 * Copy replication set memberships from upstream tables to the
	 * downstream node.
	 */
	BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS,

	/*
	 * Create subscriptions to remaining peers. We do this
	 * in catchup phase because they'll be set to fast-forward
	 * only mode.
	 */
	BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS,

	/*
	 * Tell all our peers that we've finished minimum catchup
	 * and can be promoted.
	 */
	BDR_NODE_STATE_SEND_CATCHUP_READY,

	/*
	 * Steady state: standby, waiting for promotion
	 */
	BDR_NODE_STATE_STANDBY = 200,

	/*
	 * We've been promoted and are going full-active. Get a node-id for
	 * global sequences.
	 */
	BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID,

	/*
	 * We've requested a global sequence ID and obtained a request handle.
	 * Now we're waiting for the ID to be assigned.
	 *
	 * We can go back to BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID on nack
	 * to try again or continue to BDR_NODE_STATE_CREATE_SLOTS.
	 */
	BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID,

	/*
	 * We've been promoted and are going active.
	 *
	 * Create slots for peers to replay from us.
	 */
	BDR_NODE_STATE_CREATE_SLOTS,

	/*
	 * Announce that we've gone active (and set our node read/write)
	 *
	 * => BDR_NODE_STATE_ACTIVE
	 */
	BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE,

	/*
	 * This is a placeholder for the end of the range of join states.
	 * We never enter this state.
	 */
	BDR_NODE_STATE_JOIN_RANGE_END = 299,

	/*
	 * Nodegroup is fully active as a participating member
	 */
	BDR_NODE_STATE_ACTIVE = 500

} BdrNodeState;

/*
 * The in-memory representation of a bdr.state_journal entry
 */
typedef struct BdrStateEntry
{
	uint32			counter;
	BdrNodeState	current;
	/* if this state is associated with a global consensus operation */
	uint64			global_consensus_no;
	/* If this is state is associated with a join operation, the join target id */
	uint32			join_target_id;
	/* A separate palloc'd struct of BdrNodeState-specific type, or NULL */
	void		   *extra_data;
} BdrStateEntry;

extern void state_transition(BdrStateEntry *state, BdrNodeState new_state,
	uint32 join_target_id, void *extradata);

extern void state_extradata_serialize(StringInfo out, BdrNodeState new_state,
	void *extradata);

extern void* state_extradata_deserialize(StringInfo in, BdrNodeState state);

extern void state_get_expected(BdrStateEntry *state, bool for_update,
	bool want_extradata, BdrNodeState expected);

extern void state_get_expected_many(BdrStateEntry *state, bool for_update,
	bool want_extradata, int nexpected, BdrNodeState *expected);

extern void bdr_state_insert_initial(BdrNodeState initial);

extern void bdr_state_dispatch(long *max_next_wait_msecs);

#endif
