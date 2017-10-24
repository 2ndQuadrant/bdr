#ifndef MN_CONSENSUS_IMPL_H
#define MN_CONSENSUS_IMPL_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"

#include "mn_consensus.h"

struct RaftServer;
typedef struct RaftServer RaftServer;

extern RaftServer *raft_new_server(uint32 node_id,
				consensus_execute_request_cb execute_request_cb,
				consensus_execute_query_cb execute_query_cb);
extern void raft_received_message(RaftServer *server, uint32 origin,
					  MNConsensusMessageKind msgkind,
					  StringInfo message);
extern void raft_wakeup(RaftServer *server, int msec_elapsed, long *max_next_wait_ms);
extern bool raft_add_node(RaftServer *server, uint32 node_id);
extern void raft_remove_node(RaftServer *server, uint32 node_id);

extern uint32 raft_get_leader(RaftServer *server);

extern void raft_request(RaftServer *server, MNConsensusRequest *req);
extern void raft_query(RaftServer *server, MNConsensusQuery *query);

#endif		/* MN_CONSENSUS_IMPL_H */
