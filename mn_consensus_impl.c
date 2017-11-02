/*-------------------------------------------------------------------------
 *
 * mn_consensus_impl.c
 * 		Implementation of consensus using Raft algorithm.
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_consensus.c
 *
 *-------------------------------------------------------------------------
 *
 */
#include "postgres.h"

#include <sys/time.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "miscadmin.h"

#include "storage/latch.h"
#include "storage/ipc.h"

#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timeout.h"

#include "mn_msgbroker.h"
#include "mn_msgbroker_receive.h"
#include "mn_msgbroker_send.h"
#include "mn_consensus.h"
#include "mn_consensus_impl.h"

#define RAFT_TIMEOUT 10000				/* 10s */

#define MIN(A, B) ((B)<(A)?(B):(A))

typedef enum RaftState {
	RAFT_FOLLOWER,
	RAFT_CANDIDATE,
	RAFT_LEADER
} RaftState;

/*
 * Knowledge of peers is kept in local memory on the consensus system. It's
 * expected that this will be maintained by the app based on some persistent
 * storage of nodes.
 */
typedef struct RaftNode {
	uint32		node_id;
	uint64		next_index;
	uint64		match_index;
	bool		voted_for_me;
} RaftNode;

struct RaftServer {
	uint32			node_id;
	RaftState		state;
	uint32			leader;
	uint32			voted_for;
	uint64			current_term;
	uint64			last_log_index;
	uint64			last_log_term;
	uint64			commit_index;
	uint64			apply_index;
	int				nnodes;
	RaftNode	   *nodes;
	consensus_execute_request_cb	execute_request_cb;
	consensus_execute_query_cb		execute_query_cb;
	int				time_elapsed;
};

typedef struct RaftMsgRequestVote {
	uint64		term;
	uint32		candidate_id;
	uint64		last_log_index;
	uint64		last_log_term;
} RaftMsgRequestVote;

typedef struct RaftMsgRequestVoteResponse {
	uint64		term;
	bool		vote_granted;
} RaftMsgRequestVoteResponse;

typedef struct RaftLogEntry {
	uint64		index;
	uint64		term;
	MNConsensusRequest	req;
} RaftLogEntry;

typedef struct RaftMsgAppendEntries {
	uint64		term;
	uint64		prev_log_index;
	uint64		prev_log_term;
	int			leader_commit;
	List	   *entries;
} RaftMsgAppendEntries;

typedef struct RaftMsgAppendEntriesResponse {
	uint64		current_index;
	uint64		term;
	bool		success;
} RaftMsgAppendEntriesResponse;

typedef struct RaftServerPersistentState {
	uint32		node_id;
	uint64		current_term;
	uint64		apply_index;
	uint32		voted_for;
} RaftServerPersistentState;

#define CATALOG_RAFT_LOG_RELATION	"global_consensus_journal"
#define CATALOG_RAFT_LOG_INDEX		"global_consensus_journal_pkey"
#define	Natts_raft_log		3
#define	Anum_raft_log_index	1
#define	Anum_raft_log_term	2
#define Anum_raft_log_req	3

#define CATALOG_RAFT_STATE_RELATION	"local_consensus_state"
#define	Natts_raft_state				4
#define	Anum_raft_state_node_id			1
#define	Anum_raft_state_current_term	2
#define	Anum_raft_state_apply_index		3
#define	Anum_raft_state_voted_for		4

static void raft_set_term(RaftServer *server, uint64 term);
static void raft_set_state(RaftServer *server, RaftState state);
static void raft_send_requestvote(RaftServer *server, RaftNode *target);
static void raft_send_appendentries(RaftServer *server, RaftNode *target);
static void raft_send_appendentries_all(RaftServer *server);


static void raft_serialize_request_vote(StringInfo s, RaftMsgRequestVote *reqv);
static void raft_serialize_request_vote_res(StringInfo s, RaftMsgRequestVoteResponse *res);
static void raft_serialize_log_entry(StringInfo s, RaftLogEntry *entry);
static void raft_serialize_append_entries(StringInfo s, RaftMsgAppendEntries *appe);
static void raft_serialize_append_entries_res(StringInfo s, RaftMsgAppendEntriesResponse *res);
static void raft_deserialize_log_entry(StringInfo s, RaftLogEntry *entry);
static void raft_deserialize_append_entries(StringInfo s, RaftMsgAppendEntries *appe);

/*
 * This is similar to PG_TRY() but tailored to bgworker use.
 * The main difference vs PG_TRY() is this fully recovers the state, no need
 * to RETRHOW the error (if you want to do that use PG_TRY()). Because of this
 * it can't be used in SQL functions.
 *
 * The error recovery code is based on postgres.c
 */
#define CONSENSUS_TRY() \
	do { \
		sigjmp_buf *save_exception_stack = PG_exception_stack; \
		ErrorContextCallback *save_context_stack = error_context_stack; \
		sigjmp_buf local_sigjmp_buf; \
		if (sigsetjmp(local_sigjmp_buf, 1) == 0)\
		{ \
			PG_exception_stack = &local_sigjmp_buf;

#define CONSENSUS_END_TRY() \
		} \
		else \
		{ \
			error_context_stack = NULL; \
			HOLD_INTERRUPTS(); \
			disable_all_timeouts (false); \
			QueryCancelPending = false; \
			EmitErrorReport(); \
			AbortCurrentTransaction(); \
			MemoryContextSwitchTo(TopMemoryContext); \
			FlushErrorState(); \
			RESUME_INTERRUPTS(); \
		} \
		PG_exception_stack = save_exception_stack; \
		error_context_stack = save_context_stack; \
	} while (0);

static inline Oid
get_raft_relid(const char *table)
{
	Oid			nspoid;
	Oid			reloid;

	nspoid = get_namespace_oid("bdr", false);

	reloid = get_relname_relid(table, nspoid);

	if (reloid == InvalidOid)
		elog(ERROR, "cache lookup failed for relation %s.%s",
			 "bdr", table);

	return reloid;
}

static Oid
get_raft_state_relid(RaftServer *server)
{
	static Oid relid = InvalidOid;

	if (relid == InvalidOid)
		relid = get_raft_relid(CATALOG_RAFT_STATE_RELATION);

	return relid;
}

static Oid
get_raft_log_relid(RaftServer *server)
{
	static Oid relid = InvalidOid;

	if (relid == InvalidOid)
		relid = get_raft_relid(CATALOG_RAFT_LOG_RELATION);

	return relid;
}

static Oid
get_raft_log_idxid(RaftServer *server)
{
	static Oid idxid = InvalidOid;

	if (idxid == InvalidOid)
		idxid = get_raft_relid(CATALOG_RAFT_LOG_INDEX);

	return idxid;
}

static RaftLogEntry *
raft_log_entry_from_tuple(HeapTuple tup, TupleDesc desc, bool metadata_only)
{
	RaftLogEntry   *entry = palloc(sizeof(RaftLogEntry));
	RaftLogEntry   *entrytup;
	Datum			d;
	bool			isnull;
	bytea		   *req_data;
	StringInfoData	s;

	entrytup = (RaftLogEntry *) GETSTRUCT(tup);

	entry->index = entrytup->index;
	entry->term = entrytup->term;

	/* If caller is only interested in metadata we are done. */
	if (metadata_only)
	{
		memset(&entry->req, 0, sizeof(MNConsensusRequest));
		return entry;
	}

	/* Otherwise get the request as well. */
	d = fastgetattr(tup, Anum_raft_log_req, desc, &isnull);
	req_data = DatumGetByteaPCopy(d);
	wrapInStringInfo(&s, VARDATA_ANY(req_data), VARSIZE_ANY(req_data));
	mn_consensus_deserialize_request(&s, &entry->req);

	return entry;
}

static RaftLogEntry *
raft_get_log_entry(RaftServer *server, uint64 index)
{
	Relation		log;
	Relation		idx;
	HeapTuple		tup;
	SysScanDesc		scan;
	ScanKeyData		skey;
	RaftLogEntry   *entry;

	if (index == 0)
		return NULL;

	ScanKeyInit(&skey,
				Anum_raft_log_index,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(index));

	log = heap_open(get_raft_log_relid(server), ShareRowExclusiveLock);
	idx = index_open(get_raft_log_idxid(server), ShareRowExclusiveLock);

	scan = systable_beginscan_ordered(log, idx, NULL, 1, &skey);

    tup = systable_getnext_ordered(scan, ForwardScanDirection);
	if (HeapTupleIsValid(tup))
		entry = raft_log_entry_from_tuple(tup, RelationGetDescr(log), false);
	else
		entry = NULL;

	systable_endscan_ordered(scan);
	index_close(idx, NoLock);
	heap_close(log, NoLock);

	return entry;
}

static RaftLogEntry *
raft_get_last_log_entry(RaftServer *server)
{
	Relation		log;
	Relation		idx;
	HeapTuple		tup;
	SysScanDesc		scan;
	RaftLogEntry   *entry;

	log = heap_open(get_raft_log_relid(server), ShareRowExclusiveLock);
	idx = index_open(get_raft_log_idxid(server), ShareRowExclusiveLock);

	scan = systable_beginscan_ordered(log, idx, NULL, 0, NULL);

    tup = systable_getnext_ordered(scan, BackwardScanDirection);
	if (HeapTupleIsValid(tup))
		entry = raft_log_entry_from_tuple(tup, RelationGetDescr(log), false);
	else
		entry = NULL;

	systable_endscan_ordered(scan);
	index_close(idx, NoLock);
	heap_close(log, NoLock);

	return entry;
}

static List *
raft_get_log_entries(RaftServer *server, uint64 index)
{
	Relation		log;
	Relation		idx;
	HeapTuple		tup;
	TupleDesc		desc;
	SysScanDesc		scan;
	ScanKeyData		skey;
	List		   *entries = NIL;

	if (index == 0)
		return NIL;

	ScanKeyInit(&skey,
				Anum_raft_log_index,
				BTEqualStrategyNumber, F_INT8GE,
				Int64GetDatum(index));

	log = heap_open(get_raft_log_relid(server), ShareRowExclusiveLock);
	idx = index_open(get_raft_log_idxid(server), ShareRowExclusiveLock);

	desc = RelationGetDescr(log);

	scan = systable_beginscan_ordered(log, idx, NULL, 1, &skey);

    while ((tup = systable_getnext_ordered(scan, ForwardScanDirection)) != NULL)
		entries = lappend(entries, raft_log_entry_from_tuple(tup, desc, false));

	systable_endscan_ordered(scan);
	index_close(idx, NoLock);
	heap_close(log, NoLock);

	return entries;
}

static void
raft_delete_log_entries(RaftServer *server, uint64 index)
{
	Relation		log;
	Relation		idx;
	HeapTuple		tup;
	SysScanDesc		scan;
	ScanKeyData		skey;

	if (index == 0)
		return;

	ScanKeyInit(&skey,
				Anum_raft_log_index,
				BTEqualStrategyNumber, F_INT8GE,
				Int64GetDatum(index));

	log = heap_open(get_raft_log_relid(server), ShareRowExclusiveLock);
	idx = index_open(get_raft_log_idxid(server), ShareRowExclusiveLock);

	scan = systable_beginscan_ordered(log, idx, NULL, 1, &skey);

    while ((tup = systable_getnext_ordered(scan, ForwardScanDirection)) != NULL)
	{
		CatalogTupleDelete(log, &tup->t_self);
	}

	systable_endscan_ordered(scan);
	index_close(idx, NoLock);
	heap_close(log, NoLock);

	/* TODO update last_log_term. */
	server->last_log_index = index - 1;
}

static void
raft_insert_log_entry(RaftServer *server, RaftLogEntry *entry)
{
	Relation		log;
	Relation		idx;
	HeapTuple		tup;
	bytea		   *req_data;
	StringInfoData	s;
	Datum			values[Natts_raft_log];
	bool			nulls[Natts_raft_log];

	log = heap_open(get_raft_log_relid(server), ShareRowExclusiveLock);
	idx = index_open(get_raft_log_idxid(server), ShareRowExclusiveLock);

	if (entry->index == 0)
	{
		SysScanDesc		scan;

		scan = systable_beginscan_ordered(log, idx, NULL, 0, NULL);
		tup = systable_getnext_ordered(scan, BackwardScanDirection);
		if (HeapTupleIsValid(tup))
		{
			RaftLogEntry   *oldentry = raft_log_entry_from_tuple(tup,
																 RelationGetDescr(log),
																 true);
			entry->index = 1 + oldentry->index;
		}
		else
			entry->index = 1;

		systable_endscan_ordered(scan);
	}

	/* No NULLs. */
	memset(nulls, 0, sizeof(nulls));

	/* Raft info. */
	values[Anum_raft_log_index - 1] = DatumGetUInt64(entry->index);
	values[Anum_raft_log_term - 1] = DatumGetUInt64(entry->term);

	/* Serialize the request to bytea. */
	initStringInfo(&s);
	mn_consensus_serialize_request(&s, &entry->req);
	req_data = palloc(VARHDRSZ + s.len);
	SET_VARSIZE(req_data, VARHDRSZ + s.len);
	memcpy(VARDATA(req_data), s.data, s.len);
	values[Anum_raft_log_req - 1] = PointerGetDatum(req_data);

	/* And insert the tuple tothe catalog. */
	tup = heap_form_tuple(RelationGetDescr(log), values, nulls);
	CatalogTupleInsert(log, tup);

	index_close(idx, NoLock);
	heap_close(log, NoLock);

	server->last_log_index = entry->index;
	server->last_log_term = entry->term;
}

static void
raft_persist_server_state(RaftServer *server)
{
	Relation		staterel;
	HeapTuple		oldtup;
	HeapTuple		newtup;
	SysScanDesc		scan;
	ScanKeyData		skey;
	Datum			values[Natts_raft_state];
	bool			nulls[Natts_raft_state];
	bool			replace[Natts_raft_state];

	staterel = heap_open(get_raft_state_relid(server), ShareRowExclusiveLock);

	memset(nulls, 0, sizeof(nulls));
	memset(replace, 1, sizeof(nulls));

	/* Raft info. */
	values[Anum_raft_state_node_id - 1] = DatumGetUInt32(server->node_id);
	values[Anum_raft_state_current_term - 1] = DatumGetUInt64(server->current_term);
	values[Anum_raft_state_apply_index - 1] = DatumGetUInt64(server->apply_index);
	if (server->voted_for)
		values[Anum_raft_state_voted_for - 1] = DatumGetUInt32(server->voted_for);
	else
		values[Anum_raft_state_voted_for - 1] = DatumGetUInt32(InvalidOid);

	newtup = heap_form_tuple(RelationGetDescr(staterel), values, nulls);

	ScanKeyInit(&skey,
				Anum_raft_state_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(server->node_id));
	scan = systable_beginscan(staterel, InvalidOid, false, NULL, 1, &skey);
    oldtup = systable_getnext(scan);
	if (HeapTupleIsValid(oldtup))
		CatalogTupleUpdate(staterel, &oldtup->t_self, newtup);
	else
		CatalogTupleInsert(staterel, newtup);

	systable_endscan(scan);
	heap_close(staterel, NoLock);
}

static void
raft_restore_server_state(RaftServer *server)
{
	Relation		staterel;
	HeapTuple		tup;
	SysScanDesc		scan;
	ScanKeyData		skey;

	staterel = heap_open(get_raft_state_relid(server), ShareRowExclusiveLock);

	ScanKeyInit(&skey,
				Anum_raft_state_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(server->node_id));
	scan = systable_beginscan(staterel, InvalidOid, false, NULL, 1, &skey);
    tup = systable_getnext(scan);

	if (HeapTupleIsValid(tup))
	{
		RaftLogEntry   *entry;
		RaftServerPersistentState  *statetup;

		statetup = (RaftServerPersistentState *) GETSTRUCT(tup);
		server->current_term = statetup->current_term;
		server->apply_index = statetup->apply_index;
		server->voted_for = statetup->voted_for;

		entry = raft_get_last_log_entry(server);
		if (entry)
		{
			server->last_log_index = entry->index;
			server->last_log_term = entry->term;
		}
	}

	systable_endscan(scan);
	heap_close(staterel, NoLock);
}

static bool
is_majority(const int nnodes, const int nvotes)
{
	/* Corrupt vote. */
    if (nnodes < nvotes)
        return false;

    return nnodes / 2 + 1 <= nvotes;
}

static void
raft_check_votes(RaftServer *server)
{
	int		i;
	int		nvotes = 0;

	Assert(server->state == RAFT_CANDIDATE);

	for (i = 0; i < server->nnodes; i++)
	{
		if (server->nodes[i].voted_for_me)
			nvotes++;
	}

	if (is_majority(server->nnodes, nvotes))
		raft_set_state(server, RAFT_LEADER);
}

static void
raft_set_term(RaftServer *server, uint64 term)
{
	server->current_term = term;
	server->voted_for = 0;

	StartTransactionCommand();
	raft_persist_server_state(server);
	CommitTransactionCommand();
}

static void
raft_set_state(RaftServer *server, RaftState state)
{
	int		i;

	server->state = state;

	switch (state)
	{
		case RAFT_FOLLOWER:
			break;
		case RAFT_CANDIDATE:
		{
			server->leader = 0;
			server->current_term++;

		    for (i = 0; i < server->nnodes; i++)
		    {
				if (server->node_id == server->nodes[i].node_id)
				{
					server->nodes[i].voted_for_me = true;
				    continue;
				}

				server->nodes[i].voted_for_me = false;
			}

			server->voted_for = server->node_id;
			server->time_elapsed = random() % RAFT_TIMEOUT;

			StartTransactionCommand();
			raft_persist_server_state(server);
			CommitTransactionCommand();

			raft_check_votes(server);

			/*
			 * Can't do it in above loop because we need to be sure our
			 * state is persisted before sending request.
			 */
		    for (i = 0; i < server->nnodes; i++)
			{
				if (server->node_id == server->nodes[i].node_id)
				    continue;
				raft_send_requestvote(server, &server->nodes[i]);
			}

			break;
		}
		case RAFT_LEADER:
		{
			server->leader = server->node_id;

		    for (i = 0; i < server->nnodes; i++)
		    {
				RaftNode *node;

				if (server->node_id == server->nodes[i].node_id)
				    continue;

				node = &server->nodes[i];
				node->next_index = server->last_log_index + 1;
				node->match_index = 0;
			}

			if (server->nnodes == 1)
				server->commit_index = server->last_log_index;

			StartTransactionCommand();
			raft_persist_server_state(server);
			CommitTransactionCommand();

			raft_send_appendentries_all(server);

			break;
		}

	}
}

static void
raft_send_requestvote(RaftServer *server, RaftNode *target)
{
    RaftMsgRequestVote	reqv;
	StringInfoData	s;

    reqv.term = server->current_term;
    reqv.candidate_id = server->node_id;
    reqv.last_log_index = server->last_log_index;
    reqv.last_log_term = server->last_log_term;

	initStringInfo(&s);
	raft_serialize_request_vote(&s, &reqv);
	mn_consensus_send_remote_message(target->node_id,
									 MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE,
									 s.data, s.len);
	pfree(s.data);
}

static void
raft_recv_requestvote(RaftServer *server, RaftNode *target,
					  RaftMsgRequestVote *req)
{
	RaftMsgRequestVoteResponse	res;
	RaftLogEntry   *entry;
	StringInfoData	s;

	if (server->current_term < req->term)
	{
		raft_set_term(server, req->term);
		raft_set_state(server, RAFT_FOLLOWER);
	}

	StartTransactionCommand();

	/* Raft leader vote decition. */
	if (server->current_term > req->term)
	{
		res.vote_granted = false;
	}
	else if (server->voted_for != 0 && server->voted_for != req->candidate_id)
	{
		res.vote_granted = false;
	}
	else if (server->last_log_index == 0)
	{
		res.vote_granted = true;
	}
	else if ((entry = raft_get_log_entry(server, server->last_log_index - 1)))
	{
		if (entry->term < req->term)
			res.vote_granted = true;
		else if (entry->term == req->term &&
			server->last_log_index <= req->last_log_index)
			res.vote_granted = true;
		else
			res.vote_granted = false;
	}
	else
	{
		res.vote_granted = false;
	}

	if (res.vote_granted)
	{
		server->voted_for = target->node_id;
		server->leader = 0;
		server->time_elapsed = 0;
	}

	raft_persist_server_state(server);

	CommitTransactionCommand();

    res.term = server->current_term;

	initStringInfo(&s);
	raft_serialize_request_vote_res(&s, &res);
	mn_consensus_send_remote_message(target->node_id,
									 MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE_RES,
									 s.data, s.len);
	pfree(s.data);
}

static void
raft_recv_requestvote_response(RaftServer *server, RaftNode *target,
							   RaftMsgRequestVoteResponse *res)
{
	if (server->state != RAFT_CANDIDATE)
		return;

	if (server->current_term < res->term)
	{
		raft_set_term(server, res->term);
		raft_set_state(server, RAFT_FOLLOWER);
		return;
	}

	if (server->current_term != res->term)
		return;

	if (res->vote_granted)
	{
		target->voted_for_me = true;
		raft_check_votes(server);
	}
}


static void
raft_send_appendentries(RaftServer *server, RaftNode *target)
{
    RaftMsgAppendEntries	appe = {0};
	StringInfoData			s;

    appe.term = server->current_term;
    appe.leader_commit = server->commit_index;

	StartTransactionCommand();

	if (target->next_index > 1)
	{
		appe.entries = raft_get_log_entries(server, target->next_index - 1);

		if (list_length(appe.entries) > 0)
		{
			appe.prev_log_index = target->next_index - 1;
			appe.prev_log_term = ((RaftLogEntry *) linitial(appe.entries))->term;
			appe.entries = list_delete_first(appe.entries);
		}
	}
	else
		appe.entries = raft_get_log_entries(server, target->next_index);

	initStringInfo(&s);
	raft_serialize_append_entries(&s, &appe);
	mn_consensus_send_remote_message(target->node_id,
									 MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES,
									 s.data, s.len);

	CommitTransactionCommand();
}

static void
raft_send_appendentries_all(RaftServer *server)
{
	int		i;

	server->time_elapsed = 0;

	for (i = 0; i < server->nnodes; i++)
	{
		if (server->node_id == server->nodes[i].node_id)
			continue;
		raft_send_appendentries(server, &server->nodes[i]);
	}
}

static void
raft_recv_appendentries(RaftServer *server, RaftNode *target,
						RaftMsgAppendEntries *appe)
{
	ListCell	   *lc;
	StringInfoData	s;
	RaftMsgAppendEntriesResponse	res;

	server->time_elapsed = 0;

	res.success = false;
	res.term = server->current_term;

	if ((server->state == RAFT_CANDIDATE &&
		 server->current_term == appe->term) ||
		server->current_term < appe->term)
	{
		raft_set_term(server, appe->term);
		raft_set_state(server, RAFT_FOLLOWER);
	}
	else if (server->current_term > appe->term)
	{
		res.current_index = server->last_log_index;
		goto reply;
	}

	StartTransactionCommand();

	if (appe->prev_log_index > 0)
	{
		RaftLogEntry	*entry;

		if (server->last_log_index < appe->prev_log_index)
		{
			res.current_index = server->last_log_index;
			goto commitreply;
		}

		if (!(entry = raft_get_log_entry(server, server->last_log_index - 1)))
		{
			res.current_index = server->last_log_index;
			goto commitreply;
		}

		if (entry->term != appe->prev_log_term)
		{
			raft_delete_log_entries(server, appe->prev_log_index);
			res.current_index = appe->prev_log_index - 1;
			goto commitreply;
		}
	}

	res.current_index = appe->prev_log_index;

	/* Remove conflicting entries */
	foreach (lc, appe->entries)
    {
		RaftLogEntry	*newentry = lfirst(lc);
		RaftLogEntry	*oldentry;

		oldentry = raft_get_log_entry(server, newentry->index);
        res.current_index = newentry->index;

		if (!oldentry)
		{
			break;
		}
		else if (oldentry && oldentry->term != newentry->term &&
			server->commit_index < newentry->index)
        {
			raft_delete_log_entries(server, newentry->index);
            break;
        }
    }

	/* Add remaining entries. */
	for (; lc != NULL; lc = lnext(lc))
	{
		RaftLogEntry	*entry = lfirst(lc);

		raft_insert_log_entry(server, entry);

        res.current_index = entry->index;
    }

    if (server->commit_index < appe->leader_commit)
    {
		server->commit_index = MIN(appe->leader_commit,
								   server->last_log_index);
    }

    server->leader = target->node_id;

	raft_persist_server_state(server);

    res.success = true;

commitreply:
	CommitTransactionCommand();

reply:
	initStringInfo(&s);
	raft_serialize_append_entries_res(&s, &res);
	mn_consensus_send_remote_message(target->node_id,
									 MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES_RES,
									 s.data, s.len);
	pfree(s.data);
}

static void
raft_recv_appendentries_response(RaftServer *server, RaftNode *target,
								 RaftMsgAppendEntriesResponse *res)
{
	RaftLogEntry	   *entry;

	if (server->state != RAFT_LEADER)
		return;

	if (server->current_term < res->term)
	{
		raft_set_term(server, res->term);
		raft_set_state(server, RAFT_FOLLOWER);
		return;
	}

	if (server->current_term != res->term)
		return;

	/*
	 * Append entries failed because the target node didn't have all the
	 * previous log, retry with older data.
	 */
	if (!res->success)
	{
		if (res->current_index < target->next_index - 1)
			target->next_index = MIN(res->current_index + 1, server->last_log_index);
		else
			target->next_index--;

		raft_send_appendentries(server, target);

		return;
	}

	target->next_index = res->current_index + 1;
	target->match_index = res->current_index;

	StartTransactionCommand();

	if ((entry = raft_get_log_entry(server, res->current_index)) != NULL)
	{
		/*
		 * Try to move the commit point.
		 * TODO: we need better logic, this might wait a lot on busy server
		 * where nodes lag randdmly.
		 */
		if (server->commit_index < entry->index &&
			server->current_term == entry->term)
		{
            int		i;
			int		ncaughtup = 1;

			for (i = 0; i < server->nnodes; i++)
            {
				if (res->current_index <= server->nodes[i].match_index)
					ncaughtup++;
            }

			if (is_majority(server->nnodes, ncaughtup))
			{
				server->commit_index = entry->index;
				raft_persist_server_state(server);
			}
        }
    }

	CommitTransactionCommand();

	/* If there is more to be send, do it immediately. */
	if (res->current_index < server->last_log_index)
		raft_send_appendentries(server, target);
}

static void
raft_apply_entries(RaftServer *server)
{
	List		   *entries;
	ListCell	   *lc;
	MemoryContext	oldctx = CurrentMemoryContext;
	MemoryContext	tmpctx;

	tmpctx = AllocSetContextCreate(TopMemoryContext,
								   "raft_entris_ctx",
								   ALLOCSET_DEFAULT_SIZES);
	/* Fetch entries. */
	StartTransactionCommand();
	MemoryContextSwitchTo(tmpctx);
	entries = raft_get_log_entries(server, server->apply_index + 1);
	CommitTransactionCommand();

	/* Apply all found entries. */
	foreach (lc, entries)
	{
		RaftLogEntry   *entry = lfirst(lc);
		MNConsensusResponse	res;

		res.req_id = entry->req.req_id;
		res.status = MNCONSENSUS_FAILED;
		res.payload_length = 0;
		res.payload = NULL;

		/* Try to apply entry. */
		CONSENSUS_TRY()
		{
			server->execute_request_cb(&entry->req, &res);
		}
		CONSENSUS_END_TRY()

		/* Send response for applied/failed entry. */
		mn_consensus_send_response(entry->req.origin_id, &res);

		server->apply_index = entry->index;
	}

	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(tmpctx);
}

static RaftNode *
raft_node_by_id(RaftServer *server, uint32 node_id, bool missing_ok)
{
	int		i;

    for (i = 0; i < server->nnodes; i++)
    {
        if (server->nodes[i].node_id == node_id)
			return &server->nodes[i];
    }

	if (!missing_ok)
		elog(ERROR, "could not find consensus node %u", node_id);

	return NULL;
}

void
raft_received_message(RaftServer *server, uint32 origin,
					  MNConsensusMessageKind msgkind,
					  StringInfo message)
{
	RaftNode	   *node;

	node = raft_node_by_id(server, origin, false);

	switch (msgkind)
	{
		case MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE:
		{
		    RaftMsgRequestVote	reqv;

			reqv.term = pq_getmsgint64(message);
			reqv.candidate_id = pq_getmsgint(message, 4);
			reqv.last_log_index = pq_getmsgint64(message);
			reqv.last_log_term = pq_getmsgint64(message);

			raft_recv_requestvote(server, node, &reqv);

			break;
		}
		case MN_CONSENSUS_MSG_RAFT_REQUEST_VOTE_RES:
		{
			RaftMsgRequestVoteResponse	res;

			res.term = pq_getmsgint64(message);
			res.vote_granted = pq_getmsgbyte(message);

			raft_recv_requestvote_response(server, node, &res);

			break;
		}
		case MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES:
		{
		    RaftMsgAppendEntries	appe;

			raft_deserialize_append_entries(message, &appe);

			raft_recv_appendentries(server, node, &appe);

			break;
		}
		case MN_CONSENSUS_MSG_RAFT_APPEND_ENTRIES_RES:
		{
			RaftMsgAppendEntriesResponse	res;

			res.current_index = pq_getmsgint64(message);
			res.term = pq_getmsgint64(message);
			res.success = pq_getmsgbyte(message);

			raft_recv_appendentries_response(server, node, &res);

			break;
		}
		default:
			elog(ERROR, "unexpected message kind %u", msgkind);
	}
}

void
raft_wakeup(RaftServer *server, int msec_elapsed, long *max_next_wait_ms)
{
    server->time_elapsed += msec_elapsed;

	if (server->nnodes == 1 && server->state != RAFT_LEADER)
		raft_set_state(server, RAFT_LEADER);

	if (server->time_elapsed >= RAFT_TIMEOUT)
	{
		if (server->state == RAFT_LEADER)
            raft_send_appendentries_all(server);
		else
			raft_set_state(server, RAFT_CANDIDATE);
    }

    if (server->apply_index < server->commit_index)
		raft_apply_entries(server);

	if (RAFT_TIMEOUT > server->time_elapsed)
		*max_next_wait_ms = RAFT_TIMEOUT - server->time_elapsed;
	else
		*max_next_wait_ms = 1000; /* 1s minimum timeout */
}

bool
raft_add_node(RaftServer *server, uint32 node_id)
{
	int		i;

	/* If node already exist skip. */
    for (i = 0; i < server->nnodes; i++)
    {
        if (server->nodes[i].node_id == node_id)
			return false;
	}

	/* Otherwise add the node. */
	if (server->nnodes++ > 0)
		server->nodes = repalloc(server->nodes,
								 MAXALIGN(sizeof(RaftNode)) * server->nnodes);
	else
		server->nodes = palloc(MAXALIGN(sizeof(RaftNode)) * server->nnodes);

	server->nodes[server->nnodes-1].node_id = node_id;

	return true;
}

void
raft_remove_node(RaftServer *server, uint32 node_id)
{
	int				i;
	int				j;
	int				found = 0;
	RaftNode	   *oldnodes = server->nodes;

	for (i = 0; i < server->nnodes; i++)
    {
        if (oldnodes[i].node_id == node_id)
			found ++;
	}

	if (!found)
		return;

	server->nodes = palloc0(MAXALIGN(sizeof(RaftNode)) * server->nnodes - found);

	j = 0;
    for (i = 0; i < server->nnodes; i++)
    {
        if (oldnodes[i].node_id == node_id)
			continue;

		memcpy(&server->nodes[j++], &server->nodes[i], sizeof(RaftNode));
    }

	server->nnodes -= found;

    free(oldnodes);
}

uint32
raft_get_leader(RaftServer *server)
{
	return server->leader;
}

RaftServer *
raft_new_server(uint32 node_id,
				consensus_execute_request_cb execute_request_cb,
				consensus_execute_query_cb execute_query_cb)
{
	RaftServer	   *server = palloc0(sizeof(RaftServer));

	server->state = RAFT_FOLLOWER;

	server->execute_request_cb = execute_request_cb;
	server->execute_query_cb = execute_query_cb;

	raft_add_node(server, node_id);

	server->node_id = node_id;

	raft_restore_server_state(server);

	return server;
}

void
raft_request(RaftServer *server, MNConsensusRequest *req)
{
	RaftLogEntry	entry;

	if (server->state != RAFT_LEADER)
	{
		MNConsensusResponse	res;

		res.req_id = req->req_id;
		res.status = MNCONSENSUS_NO_LEADER;
		res.payload_length = 0;
		res.payload = NULL;
		mn_consensus_send_response(req->origin_id, &res);
	}

 	entry.index = 0; /* Autofilled. */
	entry.term = server->current_term;
	memcpy(&entry.req, req, sizeof(MNConsensusRequest));

	StartTransactionCommand();
	raft_insert_log_entry(server, &entry);

	/* If there is only one node the change has been committed. */
	if (server->nnodes == 1)
	{
		server->commit_index = server->last_log_index;
		raft_persist_server_state(server);
		CommitTransactionCommand();
	}
	else
	{
		int		i;

		CommitTransactionCommand();

		/* Aggresively send the new entry to the nodes that are caught up. */
		for (i = 0; i < server->nnodes; i++)
		{
			if (server->node_id == server->nodes[i].node_id)
				continue;

			if (server->nodes[i].next_index == server->last_log_index)
				raft_send_appendentries(server, &server->nodes[i]);
		}
	}
}

void
raft_query(RaftServer *server, MNConsensusQuery *query)
{
	MNConsensusResult	res;

	res.req_id = query->req_id;
	res.status = MNCONSENSUS_FAILED;
	res.payload_length = 0;
	res.payload = NULL;

	if (server->state != RAFT_LEADER)
	{
		res.status = MNCONSENSUS_NO_LEADER;
		return;
	}

	server->execute_query_cb(query, &res);

	mn_consensus_send_result(query->origin_id, &res);
}

static void
raft_serialize_request_vote(StringInfo s, RaftMsgRequestVote *reqv)
{
	pq_sendint64(s, reqv->term);
	pq_sendint(s, reqv->candidate_id,4 );
	pq_sendint64(s, reqv->last_log_index);
	pq_sendint64(s, reqv->last_log_term);
}

static void
raft_serialize_request_vote_res(StringInfo s, RaftMsgRequestVoteResponse *res)
{
	pq_sendint64(s, res->term);
	pq_sendbyte(s, res->vote_granted);
}

static void
raft_serialize_log_entry(StringInfo s, RaftLogEntry *entry)
{
	pq_sendint64(s, entry->index);
	pq_sendint64(s, entry->term);
	mn_consensus_serialize_request(s, &entry->req);
}

static void
raft_serialize_append_entries(StringInfo s, RaftMsgAppendEntries *appe)
{
	ListCell   *lc;

	pq_sendint64(s, appe->term);
	pq_sendint64(s, appe->prev_log_index);
	pq_sendint64(s, appe->prev_log_term);
	pq_sendint64(s, appe->leader_commit);
	pq_sendint(s, list_length(appe->entries), 4);

	foreach (lc, appe->entries)
	{
		RaftLogEntry   *entry = lfirst(lc);
		raft_serialize_log_entry(s, entry);
	}
}

static void
raft_serialize_append_entries_res(StringInfo s, RaftMsgAppendEntriesResponse *res)
{
	pq_sendint64(s, res->current_index);
	pq_sendint64(s, res->term);
	pq_sendbyte(s, res->success);
}

static void
raft_deserialize_log_entry(StringInfo s, RaftLogEntry *entry)
{
	entry->index = pq_getmsgint64(s);
	entry->term = pq_getmsgint64(s);
	mn_consensus_deserialize_request(s, &entry->req);
}

static void
raft_deserialize_append_entries(StringInfo s, RaftMsgAppendEntries *appe)
{
	int		i;
	int		nentries;

	appe->term = pq_getmsgint64(s);
	appe->prev_log_index = pq_getmsgint64(s);
	appe->prev_log_term = pq_getmsgint64(s);
	appe->leader_commit = pq_getmsgint64(s);
	appe->entries = NIL;

	nentries = pq_getmsgint(s, 4);

	for (i = 0; i < nentries; i++)
	{
		RaftLogEntry   *entry = palloc(sizeof(RaftLogEntry));

		raft_deserialize_log_entry(s, entry);
		appe->entries = lappend(appe->entries, entry);
	}
}
