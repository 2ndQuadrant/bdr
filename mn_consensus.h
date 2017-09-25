#ifndef MN_CONSENSUS_H
#define MN_CONSENSUS_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "storage/latch.h"

#include "mn_msgbroker.h"

typedef struct MNConsensusProposal
{
	Size		payload_length;
	char	   *payload;
} MNConsensusProposal;

typedef struct MNConsensusProposalRequest {
	uint32		sender_nodeid;
	uint64		sender_local_msgnum;
	XLogRecPtr	sender_lsn;
	TimestampTz	sender_timestamp;
	uint64		global_proposal_id;
	MNConsensusProposal	*proposal;
} MNConsensusProposalRequest;

typedef enum MNConsensusStatus {
	MNCONSENSUS_IN_PROGRESS,
	MNCONSENSUS_ACCEPTED,
	MNCONSENSUS_FAILED
} MNConsensusStatus;

/*
 * This hook is called when a proposal is received from the message broker or
 * is localy submitted, before it is inserted.
 *
 * The hook is not called for proposals processed during crash recovery of
 * a prepared but not yet committed proposal set.
 *
 * A proposal may be rejected by this callback, causing it and all proposals
 * that are part of the same proposal set to be nacked by this node. The xact
 * will be rolled back. consensus_proposals_rollback_cb is not called, since
 * we never prepared the proposal set for commit.
 *
 * This hook is mainly for debugging/tracing. It should not have any state
 * side effects outside the transaction.
 */
typedef bool (*consensus_proposal_receive_cb)(struct MNConsensusProposalRequest *request);

/*
 * This hook is called after a set of proposals, already inserted into the
 * journal and passed to consensus_proposals_receive_hook, is being prepared for
 * commit.  This is phase 1 in a 2PC process and is called with an open
 * transaction and the proposals already inserted into the journal.
 *
 * It should return true if the xact should be prepared and an ack sent to
 * peers, or false if it should be rolled back and a nack sent to peers.
 * (A nack is still possible on true return if PREPARE TRANSATION its self
 * fails).
 *
 * The hook must prepare any database work needed to make the proposals final
 * and ensure they can be committed when the commit hook is called. If the node
 * needs to reject a proposal due to some sort of conflict, this is its chance
 * to do so by raising a suitable ERROR.
 *
 * No final and irrevocable actions should be taken since the proposals may
 * be rejected by another node and rolled back. Side effects that aren't
 * part of the transaction may be lost anyway, if we crash after prepare
 * and before commit.
 *
 * This hook should try to behave the same whether the proposals originated on
 * this node or another node. Try to handle local changes by enqueuing proposals
 * then reacting to them when received.
 *
 * It's guaranteed that either consensus_proposal_commit_hook or
 * consensus_proposals_rollback_hook will be called after
 * consensus_proposal_prepare_hook and before any other prepare hook.
 */
typedef bool (*consensus_proposals_prepare_cb)(List *requests);

/*
 * This hook is called once consensus_proposal_prepare_hook has returned
 * successfully, all peers have confirmed successful prepare, and the open
 * transaction has been locally committed. It reports completion of phase 2 of
 * the proposal exchange. The proposals are now final on disk along with any
 * related work done by the prepare hook.
 *
 * The committed hook should set in progress any actions required to
 * make the proposals take effect on the recipient system.
 *
 * It is possible for invocation of this hook to be skipped if the node crashes
 * after it does a local COMMIT PREPARED of an xact but before the hook is
 * invoked.
 *
 * Note that these proposals may be locally or remotely originated. They may be
 * newly prepared in this session or may be recovered from after commit of a
 * previously prepared xact before a crash. Either way they must be treated
 * the same.
 */
typedef void (*consensus_proposals_commit_cb)(List *requests);

/*
 * This hook is called instead of consensus_proposal_commit_hook if a peer node
 * rejects phase 1 (including our own node). The transaction from
 * consensus_proposal_prepare_hook has already been rolled back and phase 2 of
 * the proposal exchange has failed.
 *
 * It is NOT called for a normal rollback before prepare. Use regular transaction
 * hooks for that.
 *
 * The prepared xact has already rolled back, so the affected proposals are
 * no longer known. This hook has mainly diagnostic value.
 *
 * It is possible for invocation of this hook to be skipped if the node crashes
 * after it does a local ROLLBACK PREPARED of an xact but before the hook is
 * invoked.
 *
 * TODO: call it with the proposal-id and origin node at least, should be able to
 * determine that much?
 */
typedef void (*consensus_proposals_rollback_cb)(void);

typedef struct MNConsensusCallbacks
{
	consensus_proposal_receive_cb		proposal_receive;
	consensus_proposals_prepare_cb		proposals_prepare;
	consensus_proposals_commit_cb		proposals_commit;
	consensus_proposals_rollback_cb		proposals_rollback;
	mn_request_waitevents_fn			request_waitevents;
} MNConsensusCallbacks;

/* Server part of consensus. */
extern void mn_consensus_start(uint32 local_node_id, const char *journal_schema,
				   const char *journal_relation, MNConsensusCallbacks *cbs);
extern void mn_consensus_shutdown(void);
extern void mn_consensus_wakeup(struct WaitEvent *events, int nevents,
								long *max_next_wait_ms);

extern void mn_consensus_add_node(uint32 nodeid, const char *dsn,
								  bool update_if_exists);
extern void mn_consensus_remove_node(uint32 nodeid);

/* Consensus client API */
extern bool mn_consensus_begin_enqueue(void);
extern uint64 mn_consensus_enqueue(MNConsensusProposal *proposal);
extern uint64 mn_consensus_finish_enqueue(void);

static inline uint64
mn_consensus_enqueue_one(MNConsensusProposal *proposal)
{
	uint64 handle2;
	uint64 handle PG_USED_FOR_ASSERTS_ONLY;
	if (!mn_consensus_begin_enqueue())
		return 0;
	handle = mn_consensus_enqueue(proposal);
	handle2 = mn_consensus_finish_enqueue();
	Assert(handle == handle2);
	return handle2;
}

extern MNConsensusStatus mn_consensus_status(uint64 msg_handle);

extern void mn_serialize_consensus_proposal(MNConsensusProposal *proposal,
								StringInfo s);
extern MNConsensusProposal *mn_deserialize_consensus_proposal(const char *data, Size len);
extern void mn_serialize_consensus_proposal_req(MNConsensusProposalRequest *req,
									StringInfo s);
extern MNConsensusProposalRequest *mn_deserialize_consensus_proposal_req(const char *data, Size len);

extern uint32 mn_consensus_active_nodeid(void);

static inline void
wrapInStringInfo(StringInfo si, char *data, Size length)
{
	si->data = data;
	si->len = length;
	si->maxlen = -1;
	si->cursor = 0;
}

#endif		/* MN_CONSENSUS_H */
