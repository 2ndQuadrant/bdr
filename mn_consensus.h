#ifndef MN_CONSENSUS_H
#define MN_CONSENSUS_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "storage/latch.h"

#include "mn_msgbroker.h"

/*
 * TODO : these should be split between consensus "connection pool" and Raft.
 */
typedef enum MNConsensusMessageKind
{
	/* Request/response. */
	MNCONSENSUS_MSG_KIND_REQUEST,
	MNCONSENSUS_MSG_KIND_RESPONSE,
	MNCONSENSUS_MSG_KIND_QUERY,
	MNCONSENSUS_MSG_KIND_RESULT,

	/* Raft */
	MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE,
	MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE_RES,
	MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES,
	MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES_RES
} MNConsensusMessageKind;

typedef struct MNConsensusRequest
{
	/* Unique request id. */
	uint64		req_id;

	/* Node which sent this request. */
	uint32		origin_id;

	/* Payload of the request */
	Size		payload_length;
	char	   *payload;
} MNConsensusRequest;

typedef enum MNConsensusStatus
{
	MNCONSENSUS_IN_PROGRESS,				/* Request is in progress, only
											   returned by nonblocking
											   enqueue. */
	MNCONSENSUS_NO_LEADER,					/* There is no leader, retry
											   later. */
	MNCONSENSUS_EXECUTED,					/* Executed - this really means
											   it was commited in Raft terms
											   and applied on leader). */
	MNCONSENSUS_FAILED						/* Failed execution - commited in
											   Raft terms but apply has failed
											   on leader. */
} MNConsensusStatus;

typedef struct MNConsensusResponse
{
	/* Unique request id. */
	uint64		req_id;

	/* Indicates success or failure. */
	MNConsensusStatus	status;

	/* Optional payload. */
	Size		payload_length;
	char	   *payload;
} MNConsensusResponse;

/*
 * Currently query and result are same as request and response. Difference is
 * only in behavior.
 */
typedef MNConsensusRequest MNConsensusQuery;
typedef MNConsensusResponse MNConsensusResult;

/* Execute the request on the local node. */
typedef void (*consensus_execute_request_cb)(MNConsensusRequest *request, MNConsensusResponse *res);
typedef bool (*consensus_execute_query_cb)(MNConsensusQuery *query, MNConsensusResult *res);

typedef struct MNConsensusCallbacks
{
	consensus_execute_request_cb		execute_request;
	consensus_execute_query_cb			execute_query;
} MNConsensusCallbacks;

/* Server part of consensus. */
extern void mn_consensus_start(uint32 local_node_id, const char *journal_schema,
				   const char *journal_relation, MNConsensusCallbacks *cbs);
extern void mn_consensus_shutdown(void);
extern void mn_consensus_wakeup(struct WaitEvent *events, int nevents,
								long *max_next_wait_ms);

extern bool mn_consensus_want_waitevent_rebuild(void);
extern int mn_consensus_wait_event_count(void);
extern void mn_consensus_add_events(WaitEventSet *weset, int nevents);

extern void mn_consensus_add_or_update_node(uint32 nodeid, const char *dsn,
											bool update_if_exists);
extern void mn_consensus_remove_node(uint32 nodeid);

extern void mn_consensus_serialize_request(StringInfo s, MNConsensusRequest *req);
extern void mn_consensus_deserialize_request(StringInfo s, MNConsensusRequest *req);

extern void mn_consensus_serialize_response(StringInfo s, MNConsensusResponse *res);
extern void mn_consensus_deserialize_response(StringInfo s, MNConsensusResponse *res);

extern void mn_consensus_serialize_result(StringInfo s, MNConsensusResult *res);
extern void mn_consensus_deserialize_result(StringInfo s, MNConsensusResult *res);

extern void mn_consensus_serialize_query(StringInfo s, MNConsensusQuery *query);
extern void mn_consensus_deserialize_query(StringInfo s, MNConsensusQuery *query);

extern void mn_consensus_send_remote_message(uint32 target, MNConsensusMessageKind msgkind,
								 const char *data, Size len);
extern void mn_consensus_send_message(uint32 target, MNConsensusMessageKind msgkind,
						  const char *data, Size len);

extern void mn_consensus_send_response(uint64 node_id, MNConsensusResponse *res);
extern void mn_consensus_send_result(uint64 node_id, MNConsensusResult *res);

static inline void
wrapInStringInfo(StringInfo si, char *data, Size length)
{
	si->data = data;
	si->len = length;
	si->maxlen = -1;
	si->cursor = 0;
}

extern char *mn_consensus_status_to_str(MNConsensusStatus status);
extern MNConsensusStatus mn_consensus_status_from_str(const char *status);

/* Client API */
extern MNConsensusStatus mn_consensus_request(uint32 origin_id, char *data, Size len);
extern void mn_consensus_request_enqueue(uint32 origin_id, char *data, Size len);
extern MNConsensusResult *mn_consensus_query(uint32 origin_id, char *data, Size len);

#endif		/* MN_CONSENSUS_H */
