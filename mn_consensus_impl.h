#ifndef MN_CONSENSUS_IMPL_H
#define MN_CONSENSUS_IMPL_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"

#include "mn_consensus.h"

struct WaitEvent;

extern void consensus_add_or_update_node(uint32 nodeid, const char *dsn,
				  bool update_if_exists);
extern void consensus_remove_node(uint32 nodeid);

extern void consensus_startup(uint32 local_node_id, const char *journal_schema,
				  const char *journal_relation, MNConsensusCallbacks *cbs);
extern void consensus_shutdown(void);
extern void consensus_wakeup(struct WaitEvent *occurred_events, int nevents,
				 long *max_next_wait_ms);

extern bool consensus_begin_enqueue(void);
extern uint64 consensus_enqueue_proposal(MNConsensusProposal *proposal);
extern uint64 consensus_finish_enqueue(void);

extern enum MNConsensusStatus consensus_proposals_status(uint64 handle);

extern uint32 consensus_active_nodeid(void);

#endif		/* MN_CONSENSUS_IMPL_H */
