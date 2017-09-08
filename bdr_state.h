#ifndef BDR_STATE_H
#define BDR_STATE_H

typedef enum BdrNodeState
{
	/* Never appears, reserved */
	BDR_NODE_STATE_UNUSED,

	/*
	 * BDR node just created. It's not joined to a nodegroup.
	 */
	BDR_NODE_STATE_CREATED,

	/*
	 * Nodegroup is fully active as a participating member
	 */
	BDR_NODE_STATE_ACTIVE

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
	/* A separate palloc'd struct of BdrNodeState-specific type, or NULL */
	void		   *extra_data;
} BdrStateEntry;

extern void state_transition(BdrStateEntry *state, BdrNodeState new_state,
	void *extradata);

extern void state_extradata_serialize(StringInfo out, BdrNodeState new_state,
	void *extradata);

extern void* state_extradata_deserialize(StringInfo in, BdrNodeState state);

extern void state_get_expected(BdrStateEntry *state, bool for_update,
	BdrNodeState expected);

extern void state_get_expected_many(BdrStateEntry *state, bool for_update, int nexpected,
	BdrNodeState *expected);

extern void bdr_state_insert_initial(BdrNodeState initial);

#endif
