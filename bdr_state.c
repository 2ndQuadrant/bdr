/*-------------------------------------------------------------------------
 *
 * bdr_state.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_state.c
 *
 * A persistent state machine for BDR node management operations.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "miscadmin.h"

#include "utils/memutils.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_join.h"
#include "bdr_msgformats.h"
#include "bdr_state.h"
#include "bdr_state.h"
#include "bdr_worker.h"

/*
 * TODO: add this to the state transition jump table info?
 */
static bool
state_has_extradata(BdrNodeState new_state)
{
	switch (new_state)
	{
		case BDR_NODE_STATE_CREATED:
		case BDR_NODE_STATE_ACTIVE:
		case BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES:
		case BDR_NODE_JOIN_SUBSCRIBE_JOIN_TARGET:
		case BDR_NODE_STATE_WAIT_SUBSCRIBE_COMPLETE:
		case BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN:
		case BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS:
		case BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS:
		case BDR_NODE_STATE_SEND_CATCHUP_READY:
		case BDR_NODE_STATE_STANDBY:
		case BDR_NODE_STATE_CREATE_SLOTS:
		case BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE:
		case BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID:
			return false;
		case BDR_NODE_STATE_JOIN_START:
		case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
		case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
		case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
		case BDR_NODE_STATE_JOIN_FAILED:
			return true;
		case BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS:
		case BDR_NODE_STATE_JOIN_RANGE_END:
		case BDR_NODE_STATE_UNUSED:
			Assert(false);
			elog(ERROR, "reserved node state %u somehow got used", new_state);
	}
	/* This is deliberately not a default: so we get warnings */
	Assert(false);
	elog(ERROR, "unhandled node state %u", new_state);
}

/*
 * Deserialize state extra data, of a state-specific type identified
 * by 'state' and return a palloc'd struct with the deserialized
 * data.
 *
 * Deserialization functions must keep in mind that state entries
 * could've been serialized by an older BDR version and should
 * add a format version byte.
 */
void*
state_extradata_deserialize(StringInfo in, BdrNodeState state)
{
	BdrNodeState embedded_state;

	/*
	 * States that permit no extra data in our current version.
	 *
	 * In case of downgrade, we probably don't want to ERROR noisly here.
	 */
	if (!state_has_extradata(state))
	{
		elog(bdr_debug_level, "ignoring state extra data for state %u that does not carry extradata",
			 state);
		return NULL;
	}

	Assert(in->len > 4);
	embedded_state = pq_getmsgint(in, 4);
	if (embedded_state != state)
		elog(ERROR, "state info mismatch; serialized state extradata of type %u does not match expected %u",
			 embedded_state, state);

	switch (state)
	{
		case BDR_NODE_STATE_JOIN_START:
		{
			struct ExtraDataJoinStart *extra = palloc(sizeof(struct ExtraDataJoinStart));
			extra->group_name = pq_getmsgstring(in);
			return extra;
		}

		case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
		case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
		{
			struct ExtraDataConsensusWait *extra = palloc(sizeof(struct ExtraDataConsensusWait));
			extra->request_message_handle = pq_getmsgint64(in);
			return extra;
		}

		case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
		{
			struct ExtraDataJoinWaitCatchup *extra = palloc(sizeof(struct ExtraDataJoinWaitCatchup));
			extra->min_catchup_lsn = pq_getmsgint64(in);
			return extra;
		}

		case BDR_NODE_STATE_JOIN_FAILED:
		{
			struct ExtraDataJoinFailure *extra = palloc(sizeof(struct ExtraDataJoinFailure));
			extra->reason = pq_getmsgstring(in);
			return extra;
		}

		default:
			/*
			 * We shouldn't error here, since new states could be added
			 * and we might be a downgrade.
			 */
			elog(WARNING, "ignoring extradata for unrecognised node state %u",
				 embedded_state);
			return NULL;
	}

	Assert(false); /* unreachable */
}

/*
 * Serialize state extra data, of a state-specific type identified by
 * new_state, into the passed StringInfo.
 */
void
state_extradata_serialize(StringInfo out, BdrNodeState new_state,
	void *extradata)
{
	Assert(state_has_extradata(new_state));

	/*
	 * we always inject the state type first. It's a bit of a waste of space
	 * storing it twice, but it lets us detect errors and bail out without
	 * bizarre behaviour.
	 */
	pq_sendint(out, new_state, 4);

	switch (new_state)
	{
		case BDR_NODE_STATE_JOIN_START:
		{
			struct ExtraDataJoinStart *extra = extradata;
			pq_sendstring(out, extra->group_name);
			break;
		}

		case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
		case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
		{
			struct ExtraDataConsensusWait *extra = extradata;
			pq_sendint64(out, extra->request_message_handle);
			break;
		}

		case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
		{
			struct ExtraDataJoinWaitCatchup *extra = extradata;
			pq_sendint64(out, extra->min_catchup_lsn);
			break;
		}

		case BDR_NODE_STATE_JOIN_FAILED:
		{
			struct ExtraDataJoinFailure *extra = extradata;
			pq_sendstring(out, extra->reason);
			break;
		}

		default:
			elog(ERROR, "unhandled node state %u", new_state);
	}
}

/*
 * Validty check for state transitions
 *
 * TODO: make into a state jump table, this will get messy otherwise
 */
static void
state_transition_check(BdrStateEntry *state, BdrNodeState new_state)
{
	/* Not implemented! */
}

/*
 * Dispatcher for local node state transitions. Takes the current state info
 * read from disk, a proposed new state and some state-type-specific extra
 * data. Records a new state journal entry with the state, sequential counter,
 * and serialized extradata if passed.
 *
 * The state struct passed should generally have been be locked for update when
 * read. It MUST be locked if the caller cares about exactly sequential
 * states with no other changes in between.
 *
 * state_transition rechecks the last committed state, and will ERROR if it
 * doesn't match the old state in the state struct passed to it. It doesn't
 * check the counter, as it's presumed that the caller is happy so long as the
 * state value is the same, otherwise the caller would've locked the state
 * table when it read it.
 */
void
state_transition(BdrStateEntry *state, BdrNodeState new_state,
	uint32 join_target_id, void *extradata)
{
	BdrStateEntry cur;

	Assert(state_has_extradata(new_state) != (extradata == NULL));

	state_get_last(&cur, true /* for_update */, false /* no extradata */);
	Assert(cur.counter != 0);

	if (state->counter != 0 && state->counter != cur.counter)
		elog(ERROR, "attempt to push out of sequence state counter; last committed was %u but we have %u",
			 cur.counter, state->counter);

	state_transition_check(state, new_state);

	cur.counter = cur.counter + 1;
	cur.current = new_state;
	cur.global_consensus_no = 0L; /* TODO, accept arg */
	cur.join_target_id = join_target_id;
	cur.extra_data = extradata;

	elog(bdr_debug_level, "node %u state transition #%u, %u => %u",
		 bdr_get_local_nodeid(), cur.counter, state->current, cur.current);

	state_push(&cur);

	/*
	 * We avoid overwriting the caller's state until we *know* the new state
	 * got inserted. It's not committed yet, but it will be along with whatever
	 * our caller is doing in this state change.
	 */
	*state = cur;
}

/*
 * Look up the current state and set 'state' to it if it's one of the listed
 * expected states. ERROR if it doesn't match any of the expected states.
 */
void
state_get_expected_many(BdrStateEntry *state, bool for_update,
	bool with_extradata, int nexpected, BdrNodeState *expected)
{
	int i;
	BdrStateEntry cur;
	state_get_last(&cur, for_update, with_extradata);
	for (i = 0; i < nexpected; i++)
	{
		if (cur.current == expected[i])
		{
			*state = cur;
			return;
		}
	}

	/* TODO: list expected/allowed states here */
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("local BDR node %u in unexpected state %u",
					bdr_get_local_nodeid(), cur.current)));
}

/*
 * Look up the current state and set 'state' to it if it's the listed expected
 * state. ERROR if it doesn't match expected.
 */
void
state_get_expected(BdrStateEntry *state, bool for_update,
	bool with_extradata, BdrNodeState expected)
{
	BdrStateEntry cur;
	state_get_last(&cur, for_update, with_extradata);
	if (cur.current == expected)
	{
		*state = cur;
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("local BDR node %u in unexpected state %u; wanted %u",
					bdr_get_local_nodeid(), cur.current, expected)));
}

/*
 * Initialize the bdr.state_journal with its first entry, on node
 * creation.
 */
void
bdr_state_insert_initial(BdrNodeState initial)
{
	BdrStateEntry	state_initial;

	state_initial.counter = 1;
	state_initial.current = initial;
	state_initial.global_consensus_no = 0L;
	state_initial.join_target_id = 0;
	state_initial.extra_data = NULL;

	/*
	 * No need to look for states here, we'll ERROR with a pkey
	 * violation.
	 */
	state_push(&state_initial);
}

/*
 * Dispatch to state-specific handlers to continue whatever
 * work we're doing.
 *
 * TODO: should really be a jump table
 */
void
bdr_state_dispatch(long *max_next_wait_msecs)
{
	bool txn_started = false;
	BdrStateEntry cur;

	if (!IsTransactionState())
	{
		txn_started = true;
		StartTransactionCommand();
	}
	state_get_last(&cur, false /* no lock */, false /* no extradata */);
	if (txn_started)
		CommitTransactionCommand();

	switch (cur.current)
	{
		/*
		 * Steady states that don't budge without
		 * external influence.
		 */
		case BDR_NODE_STATE_CREATED:
		case BDR_NODE_STATE_ACTIVE:
		case BDR_NODE_STATE_JOIN_FAILED:
			return;

		/*
		 * Transitional states during the node join process.
		 */
		case BDR_NODE_STATE_JOIN_START:
		case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
		case BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES:
		case BDR_NODE_JOIN_SUBSCRIBE_JOIN_TARGET:
		case BDR_NODE_STATE_WAIT_SUBSCRIBE_COMPLETE:
		case BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN:
		case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
		case BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS:
		case BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS:
		case BDR_NODE_STATE_SEND_CATCHUP_READY:
		case BDR_NODE_STATE_STANDBY:
		case BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID:
		case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
		case BDR_NODE_STATE_CREATE_SLOTS:
		case BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE:
			bdr_join_continue(cur.current, max_next_wait_msecs);
			return;

		case BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS:
		case BDR_NODE_STATE_JOIN_RANGE_END:
		case BDR_NODE_STATE_UNUSED:
			Assert(false);
			elog(ERROR, "reserved node state %u somehow got used", cur.current);
	}
}
