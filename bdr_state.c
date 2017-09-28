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
 *
 * This state machine is invoked by the manager during event processing,
 * startup, etc. It looks up the bdr.state_journal table to find the most
 * recent state recorded, and based on that invokes a callback for whatever BDR
 * subsystem is interested in making changes to node state. A state transition
 * occurs when a state callback succeeds and sets the next state by inserting
 * a new bdr.state_journal row.
 *
 * It's mainly used for node part and join, where we need to perform complex
 * multi-step operations in a reliable, crash-safe, recoverable way.
 *
 * The current state can move out of one of the steady states like
 * BDR_NODE_STATE_CREATED, BDR_NODE_STATE_ACTIVE, etc in response to SQL
 * function calls that directly change the node state. 
 *
 * Global consensus messages can also change the state once the consensus
 * system is up; we just do a state transition in a gobal consensus prepare
 * handler. This lets global consensus messages respond to requests that
 * cannot be completed within the scope of one commit (so long as they
 * can do enough to promise to succeed within the first prepare).
 *
 * Arbitrary extra data may be added to any given state, so the handler callback
 * for it has the information it needs.
 *
 * The bdr.state_journal may be inspected with the bdr.state_journal_details
 * view to get human readable state names, decoded extradata fields, etc.
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
		case BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET:
		case BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE:
		case BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN:
		case BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS:
		case BDR_NODE_STATE_SEND_CATCHUP_READY:
		case BDR_NODE_STATE_STANDBY:
		case BDR_NODE_STATE_CREATE_SLOTS:
		case BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE:
		case BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID:
		case BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING:
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
		case BDR_NODE_ACTIVE_RANGE_END:
			Assert(false);
			elog(ERROR, "reserved node state %u somehow got used", new_state);
	}
	/* This is deliberately not a default: so we get warnings */
	Assert(false);
	elog(ERROR, "unhandled node state %u", new_state);
}

/* Sometimes I hate C */
const char *
bdr_node_state_name(BdrNodeState state)
{
	switch (state)
	{
		case BDR_NODE_STATE_CREATED:
			return CppAsString2(BDR_NODE_STATE_CREATED);
		case BDR_NODE_STATE_ACTIVE:
			return CppAsString2(BDR_NODE_STATE_ACTIVE);
		case BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES:
			return CppAsString2(BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES);
		case BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET:
			return CppAsString2(BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET);
		case BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE:
			return CppAsString2(BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE);
		case BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN:
			return CppAsString2(BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN);
		case BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS:
			return CppAsString2(BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS);
		case BDR_NODE_STATE_SEND_CATCHUP_READY:
			return CppAsString2(BDR_NODE_STATE_SEND_CATCHUP_READY);
		case BDR_NODE_STATE_STANDBY:
			return CppAsString2(BDR_NODE_STATE_STANDBY);
		case BDR_NODE_STATE_CREATE_SLOTS:
			return CppAsString2(BDR_NODE_STATE_CREATE_SLOTS);
		case BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE:
			return CppAsString2(BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE);
		case BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID:
			return CppAsString2(BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID);
		case BDR_NODE_STATE_JOIN_START:
			return CppAsString2(BDR_NODE_STATE_JOIN_START);
		case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
			return CppAsString2(BDR_NODE_STATE_JOIN_WAIT_CONFIRM);
		case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
			return CppAsString2(BDR_NODE_STATE_JOIN_WAIT_CATCHUP);
		case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
			return CppAsString2(BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID);
		case BDR_NODE_STATE_JOIN_FAILED:
			return CppAsString2(BDR_NODE_STATE_JOIN_FAILED);
		case BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS:
			return CppAsString2(BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS);
		case BDR_NODE_STATE_JOIN_RANGE_END:
			return CppAsString2(BDR_NODE_STATE_JOIN_RANGE_END);
		case BDR_NODE_STATE_UNUSED:
			return CppAsString2(BDR_NODE_STATE_UNUSED);
		case BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING:
			return CppAsString2(BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING);
		case BDR_NODE_ACTIVE_RANGE_END:
			return CppAsString2(BDR_NODE_ACTIVE_RANGE_END);
	}
	Assert(false);
	elog(ERROR, "unhandled node state %u", state);
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
 * For debugging/tracing purposes, convert a state's extradata to human
 * readable form. Returns a palloc'd string in the current memory context.
 *
 * Keep in mind that this could possibly have to deal with unrecognised
 * states after a downgrade, etc.
 */
char*
state_stringify_extradata(BdrStateEntry *state)
{
	StringInfoData si;

	if (state->extra_data == NULL)
		return NULL;

	initStringInfo(&si);

	switch (state->current)
	{
		case BDR_NODE_STATE_JOIN_START:
		{
			struct ExtraDataJoinStart *extra = state->extra_data;
			appendStringInfoString(&si, "node group name: ");
			if (extra->group_name != NULL)
				appendStringInfoString(&si, extra->group_name);
			else
				appendStringInfoString(&si, "(NULL)");
			break;
		}

		case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
		case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
		{
			struct ExtraDataConsensusWait *extra = state->extra_data;
			appendStringInfo(&si, "global consensus message handle: "UINT64_FORMAT,
				extra->request_message_handle);
			break;
		}

		case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
		{
			struct ExtraDataJoinWaitCatchup *extra = state->extra_data;
			appendStringInfo(&si, "minimum catchup lsn: %X/%08X",
				(uint32)(extra->min_catchup_lsn>>32),
				(uint32)extra->min_catchup_lsn);
			break;
		}

		case BDR_NODE_STATE_JOIN_FAILED:
		{
			struct ExtraDataJoinFailure *extra = state->extra_data;
			appendStringInfoString(&si, "reason: ");
			if (extra->reason != NULL)
				appendStringInfoString(&si, extra->reason);
			else
				appendStringInfoString(&si, "(NULL)");
			break;
		}

		default:
			appendStringInfo(&si, "unhandled state %d",
				state->current);
			break;
	}
	return si.data;
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
	uint32 peer_id, void *extradata)
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
	cur.peer_id = peer_id;
	cur.extra_data = extradata;

	elog(bdr_debug_level, "node %u state transition #%u, %s => %s",
		 bdr_get_local_nodeid(), cur.counter,
		 bdr_node_state_name(state->current),
		 bdr_node_state_name(cur.current));

	state_push(&cur);

	/*
	 * We avoid overwriting the caller's state until we *know* the new state
	 * got inserted. It's not committed yet, but it will be along with whatever
	 * our caller is doing in this state change.
	 */
	*state = cur;
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
	state_initial.peer_id = 0;
	state_initial.extra_data = NULL;
	/* state->entered_time will be set by state_push */

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
		case BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET:
		case BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE:
		case BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN:
		case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
		case BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS:
		case BDR_NODE_STATE_SEND_CATCHUP_READY:
		case BDR_NODE_STATE_STANDBY:
		case BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID:
		case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
		case BDR_NODE_STATE_CREATE_SLOTS:
		case BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE:
			bdr_join_continue(cur.current, max_next_wait_msecs);
			return;

		/*
		 * Temporary states for active nodes
		 */
		case BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING:
			bdr_join_create_peer_slot();
			break;

		/*
		 * States that should never appear
		 */
		case BDR_NODE_STATE_JOIN_CAN_START_CONSENSUS:
		case BDR_NODE_STATE_JOIN_RANGE_END:
		case BDR_NODE_ACTIVE_RANGE_END:
		case BDR_NODE_STATE_UNUSED:
			Assert(false);
			elog(ERROR, "reserved node state %u somehow got used", cur.current);
	}
}
