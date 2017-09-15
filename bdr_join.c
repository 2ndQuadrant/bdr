/*-------------------------------------------------------------------------
 *
 * bdr_join.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_join.c
 *
 * Logic for consistently joining a BDR node to an existing node group
 *-------------------------------------------------------------------------
 *
 * bdr_join manages node parts and joins using a simple state machine for the
 * node's participation and join join.
 */
#include "postgres.h"

#include "access/xact.h"

#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "fmgr.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "pgstat.h"

#include "replication/logicalfuncs.h"
#include "replication/logical.h"
#include "replication/origin.h"
#include "replication/slot.h"

#include "storage/proc.h"

#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/memutils.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"
#include "pglogical_sync.h"
#include "pglogical_worker.h"
#include "pglogical_plugins.h"

#include "bdr_catcache.h"
#include "bdr_functions.h"
#include "bdr_join.h"
#include "bdr_manager.h"
#include "bdr_messaging.h"
#include "bdr_msgformats.h"
#include "bdr_state.h"
#include "bdr_worker.h"

/*
 * Local non-persistent state for join. Persistent state is in the state
 * journal entries.
 */
struct BdrJoinProgress
{
	/*
	 * Non-replication libpq connection to the remote node, used
	 * to clone catalog entries, make remote function calls, etc.
	 */
	PGconn *conn;

	/* Memory used for join state */
	MemoryContext mctx;

	/* There's a query in-flight on 'conn' */
	bool query_result_pending;

	/* Our cached info about the join target */
	BdrNodeInfo *target;

	/*
	 * Position of our wait event for the connection in the
	 * manager's wait event array or -1 for unregistered
	 */
	int wait_event_pos;

	/*
	 * The wait-event set that wait_event_pos is part of.
	 */
	WaitEventSet *wait_set;
};

struct BdrJoinProgress join = { NULL, NULL, false, NULL, -1, NULL};

static void bdr_join_create_slot(BdrNodeInfo *local, BdrNodeInfo *remote);
static void bdr_create_subscription(BdrNodeInfo *local, BdrNodeInfo *remote, int apply_delay_ms, bool for_join);
static void bdr_join_wait_event_set_register(void);
static void backend_sleep_conn(int millis, PGconn *conn);

/*
 * Start an async connection to the remote peer
 */
static PGconn*
bdr_join_begin_connect_remote(BdrNodeInfo *local,
	const char * remote_node_dsn)
{
	const char *connkeys[3] = {"dbname", "application_name", NULL};
	const char *connvalues[3];
	char appname[NAMEDATALEN];
	PGconn *conn;

	snprintf(appname, NAMEDATALEN, "bdr join: %s",
		local->pgl_node->name);
	appname[NAMEDATALEN-1] = '\0';

	connvalues[0] = remote_node_dsn;
	connvalues[1] = appname;
	connvalues[2] = NULL;

	conn = PQconnectStartParams(connkeys, connvalues, true);

	if (PQstatus(conn) == CONNECTION_BAD)
		elog(ERROR, "failed to allocate PGconn: out of memory?");

	return conn;
}

static void
bdr_join_reset_connection(void)
{
	if (join.conn != NULL)
		PQfinish(join.conn);
	join.conn = NULL;
	join.wait_event_pos = -1;
	join.query_result_pending = false;
	/*
	 * We can't remove our wait event from the set, so...
	 */
	pglogical_manager_recreate_wait_event_set();
}

/*
 * Begin or continue asynchronous connection process for the join target node.
 * Returns true if the conn is ready to use. Call reapeatedly until it
 * succeeds.
 */
static bool
bdr_join_maintain_conn(BdrNodeInfo *local, uint32 target_id)
{
	MemoryContext old_ctx;

	Assert(join.target == NULL || join.target->bdr_node->node_id == target_id);
	if (join.target == NULL)
	{
		old_ctx = MemoryContextSwitchTo(join.mctx);
		join.target = bdr_get_node_info(target_id, false);
		(void) MemoryContextSwitchTo(old_ctx);
	}

	if (PQstatus(join.conn) == CONNECTION_OK)
	{
		/*
		 * We consume input here because we want to clear any notices, etc,
		 * that might be on the connection, notice when it breaks etc, even if
		 * we're in a join phase that doesn't use the connection right now.
		 * 
		 * This lets us update the wait-event state appropriately too.
		 */
		if (!PQconsumeInput(join.conn))
			bdr_join_reset_connection();
	}

	if (join.conn != NULL && PQstatus(join.conn) == CONNECTION_BAD)
	{
		ereport(ERROR,
				(errmsg("connection to peer broke"),
				 errdetail("libpq: %s", PQerrorMessage(join.conn))));
		bdr_join_reset_connection();
	}

	if (join.conn == NULL)
	{
		bdr_join_reset_connection();
		join.conn = bdr_join_begin_connect_remote(local,
			join.target->pgl_interface->dsn);
	}

	/*
	 * Continue async connect
	 */
	if (join.conn != NULL
		&& PQstatus(join.conn) != CONNECTION_OK
		&& PQstatus(join.conn) != CONNECTION_BAD)
	{

		switch (PQconnectPoll(join.conn))
		{
			case PGRES_POLLING_FAILED:
				ereport(WARNING,
						(errmsg("failed to connect to remote BDR node %s",
								join.target->pgl_node->name),
						 errdetail("libpq: %s", PQerrorMessage(join.conn))));
				bdr_join_reset_connection();
				/* TODO: rate limit reconnections */
				break;

			case PGRES_POLLING_OK:
				Assert(PQstatus(join.conn) == CONNECTION_OK);
				bdr_join_wait_event_set_register();
				break;
				
			default:
				/*
				 * We just polled, so no point doing it again, we'll recheck
				 * next time we loop.
				 */
				Assert(join.wait_event_pos == -1);
				break;
		}
	}

	return PQstatus(join.conn) == CONNECTION_OK;
}

/*
 * Synchronously connect to a peer.
 */
PGconn*
bdr_join_connect_remote(BdrNodeInfo *local, const char * remote_node_dsn)
{
	PGconn *conn;
	int ret;

	Assert(!is_bdr_manager());

	conn = bdr_join_begin_connect_remote(local, remote_node_dsn);
	for (;;)
	{
		ret = PQconnectPoll(conn);
		switch (ret)
		{
			case PGRES_POLLING_OK:
				Assert(PQstatus(conn) == CONNECTION_OK);
				return conn;
			case PGRES_POLLING_READING:
			case PGRES_POLLING_WRITING:
				/* we just polled, sleep and try again */
				backend_sleep_conn(2500L /* millis */, conn);
				break;
			case PGRES_POLLING_FAILED:
				ereport(ERROR,
						(errmsg("failed to connect to remote BDR node"),
						 errdetail("libpq: %s", PQerrorMessage(conn))));

		}
	}

	Assert(false); /* unreachable */
}

void
bdr_finish_connect_remote(PGconn *conn)
{
	PQfinish(conn);
}

/*
 * Submit an async node-join request to the peer node. This
 * doesn't wait to check that the remote actually processed
 * the query.
 */
static void
bdr_join_submit_request(const char * node_group_name)
{
	Oid paramTypes[8] = {TEXTOID, TEXTOID, OIDOID, INT4OID, TEXTOID, OIDOID, TEXTOID, TEXTOID};
	const char *paramValues[8];
	char my_node_id[MAX_DIGITS_INT32];
	char my_node_initial_state[MAX_DIGITS_INT32];
	char my_node_if_id[MAX_DIGITS_INT32];
	int ret;
	BdrNodeInfo *local = bdr_get_cached_local_node_info();

	Assert(local->pgl_interface != NULL);
	Assert(!join.query_result_pending);

	paramValues[0] = node_group_name;
	paramValues[1] = local->pgl_node->name;
	snprintf(my_node_id, MAX_DIGITS_INT32, "%u",
		local->bdr_node->node_id);
	paramValues[2] = my_node_id;
	snprintf(my_node_initial_state, MAX_DIGITS_INT32, "%d",
		local->bdr_node->local_state);
	paramValues[3] = my_node_initial_state;
	paramValues[4] = local->pgl_interface->name;
	snprintf(my_node_if_id, MAX_DIGITS_INT32, "%u",
		local->pgl_interface->id);
	paramValues[5] = my_node_if_id;
	paramValues[6] = local->pgl_interface->dsn;
	paramValues[7] = get_database_name(MyDatabaseId);
	ret = PQsendQueryParams(join.conn,
							"SELECT bdr.internal_submit_join_request($1, $2, $3, $4, $5, $6, $7, $8)",
							8, paramTypes, paramValues, NULL, NULL, 0);

	if (!ret)
	{
		ereport(WARNING,
				(errmsg("failed to submit join request on join target - couldn't send query"),
				 errdetail("libpq: %s", PQerrorMessage(join.conn))));
		bdr_join_reset_connection();
	}

	join.query_result_pending = true;
}

/*
 * Return true if there's a result ready to read on the current
 * join.conn
 *
 * It's legal to call this when we're not actually expecting a result, e.g.
 * due to the connection being reset.
 *
 * This must be called in conjunction with bdr_join_maintain_conn to
 * consume input, establish/continue/fix connections, etc.
 */
static bool
check_for_query_result(void)
{
	if (!join.query_result_pending)
		return false;

	return (PQstatus(join.conn) == CONNECTION_OK && !PQisBusy(join.conn));
}

/*
 * Sleep safely in the backend. Basically pg_sleep + wait for socket read.
 */
static void
backend_sleep_conn(int millis, PGconn *conn)
{
	int latchret = WaitLatchOrSocket(&MyProc->procLatch,
		WL_TIMEOUT|WL_LATCH_SET|WL_POSTMASTER_DEATH|WL_SOCKET_READABLE,
		PQsocket(conn), millis, PG_WAIT_EXTENSION);

	ResetLatch(&MyProc->procLatch);

	if (latchret & WL_POSTMASTER_DEATH)
		proc_exit(0);

	CHECK_FOR_INTERRUPTS();
}

/*
 * Get the reply to the query from bdr_join_submit_request, with
 * a handle we can use to track progress of our consensus message
 * on the join target.
 *
 * Returns 0 until result successfully read.
 */
static uint64
bdr_join_submit_get_result(void)
{
	uint64 handle = 0;

	if (check_for_query_result())
	{
		PGresult *res = PQgetResult(join.conn);
		const char * val;

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			const char *msg = pstrdup(PQresultErrorMessage(res));
			PQclear(res);
			ereport(ERROR,
					(errmsg("failed to submit join request on join target"),
					 errdetail("libpq: %s", msg)));
		}

		val = PQgetvalue(res, 0, 0);
		if (sscanf(val, UINT64_FORMAT, &handle) != 1)
			elog(ERROR, "could not parse consensus message handle from remote");

		PQclear(res);

		/*
		 * We know there's only one result set, so this really shouldn't
		 * block for long. It's a bit naughty to check it here without
		 * testing if we'll block first, but it really should be safe...
		 */
		res = PQgetResult(join.conn);
		Assert(res == NULL);

		join.query_result_pending = false;
	}

	return handle;
}

/*
 * Entrypoint for BDR_NODE_STATE_JOIN_START handling.
 *
 * Asynchronously connect to the remote peer, submit a
 * join request query, want wait for the query result.
 *
 * This just submits a join request and gets a completion
 * handle for the remote's consensus processing then
 * transitions to BDR_NODE_STATE_JOIN_WAIT_CONFIRM.
 *
 * This function will be called repeatedly until it transitions
 * the system to the next state.
 */
static void
bdr_join_continue_join_start(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	uint64 handle = 0;
	ExtraDataJoinStart *extra;

	Assert(cur_state->current == BDR_NODE_STATE_JOIN_START);
	extra = cur_state->extra_data;

	if (!join.query_result_pending)
	{
		/* send join request query to server */
		bdr_join_submit_request(extra->group_name);
		join.query_result_pending = true;
	}

	/* wait for server to reply with progress handle for join */
	if (check_for_query_result())
	{
		handle = bdr_join_submit_get_result();

		if (handle != 0)
		{
			/* join request submitted successfully */
			ExtraDataConsensusWait new_extra;
			new_extra.request_message_handle = handle;
			state_transition(cur_state, BDR_NODE_STATE_JOIN_WAIT_CONFIRM,
				cur_state->join_target_id, &new_extra);
		}
	}
}


/*
 * Handle a remote request to join by creating the remote node entry.
 *
 * At this point the proposal isn't committed yet, and we're in a
 * consensus manager transaction that'll prepare it if we succeed.
 */
void
bdr_join_handle_join_proposal(BdrMessage *msg)
{
	BdrMsgJoinRequest *req = msg->message;
	BdrNodeGroup *local_nodegroup;
	BdrNode bnode;
	PGLogicalNode pnode;
	PGlogicalInterface pnodeif;
	BdrStateEntry cur_state;

	Assert(is_bdr_manager());
	Assert(msg->message_type == BDR_MSG_NODE_JOIN_REQUEST);
	state_get_expected(&cur_state, true, true, BDR_NODE_STATE_ACTIVE);

	local_nodegroup = bdr_get_nodegroup_by_name(req->nodegroup_name, false);
	if (req->nodegroup_id != 0 && local_nodegroup->id != req->nodegroup_id)
		elog(ERROR, "expected nodegroup %s to have id %u but local nodegroup id is %u",
			 req->nodegroup_name, req->nodegroup_id, local_nodegroup->id);

	if (req->joining_node_id == 0)
		elog(ERROR, "joining node id must be nonzero");

	if (req->joining_node_id == bdr_get_local_nodeid())
	{
		/*
		 * Join requests are not received by the node being joined, because
		 * it's not yet part of the consensus system. So this shouldn't happen.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("node %u received its own join request", req->joining_node_id)));
	}

	pnode.id = req->joining_node_id;
	pnode.name = (char*)req->joining_node_name;

	bnode.node_id = req->joining_node_id;
	bnode.node_group_id = local_nodegroup->id;
	/*
	 * TODO: should set local state to joining
	 */
	bnode.local_state = req->joining_node_state;
	bnode.seq_id = 0;
	bnode.dbname = req->joining_node_dbname;

	pnodeif.id = req->joining_node_if_id;
	pnodeif.name = req->joining_node_if_name;
	pnodeif.nodeid = pnode.id;
	pnodeif.dsn = req->joining_node_if_dsn;

	/*
	 * TODO: should treat node as join-confirmed if we're an active
	 * node ourselves.
	 */
	bnode.confirmed_our_join = false;

	/* TODO: do upserts here in case pgl node or even bdr node exists already */
	create_node(&pnode);
	create_node_interface(&pnodeif);
	bdr_node_create(&bnode);

	/*
	 * TODO: move addition of the peer node from prepare
	 * phase to accept phase callback
	 */
	bdr_messaging_add_peer(bnode.node_id, pnodeif.dsn, false);

	/*
	 * If this node is the join target, the joining peer will need a
	 * replication slot to use for catchup mode subscription. Create one now.
	 *
	 * Otherwise we'll delay slot creation until the peer is entering catchup
	 * standby mode so there's less cleanup to do in case of failure, less resource
	 * retention to worry about, etc.
	 *
	 * Unfortunately we cannot create a logical replication slot in an xact
	 * that's done writes, let alone a prepared xact. And we must do this reliably,
	 * so we don't want to wait for the after-commit callback. To solve that,
	 * transition the node's local state and action the slot creation once
	 * we've committed. This'll call bdr_join_create_peer_slot() to do the
	 * creation.
	 */
	if (req->join_target_node_id == bdr_get_local_nodeid())
		state_transition(&cur_state, BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING,
			req->joining_node_id, NULL);

	/*
	 * We don't subscribe to the node yet, that only happens once it goes
	 * active.
	 */
}

/*
 * Respond to a BDR_MSG_NODE_JOIN_REQUEST request's
 * BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING state by creating a slot for the
 * peer and returning to BDR_NODE_STATE_ACTIVE.
 */
void
bdr_join_create_peer_slot(void)
{
	BdrStateEntry cur_state;
	BdrNodeInfo *local, *remote;

	Assert(!IsTransactionState());
	StartTransactionCommand();
	state_get_expected(&cur_state, true, true,
		BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING);

	local = bdr_get_cached_local_node_info();
	remote = bdr_get_node_info(cur_state.join_target_id, false);
	bdr_join_create_slot(local, remote);

	state_transition(&cur_state, BDR_NODE_STATE_ACTIVE, 0, NULL);

	/*
	 * TODO: should set local state for peer node entry.
	 */

	/*
	 * TODO: write a catchup-confirmation message
	 * here, so the peer can tally join confirmations
	 */
	CommitTransactionCommand();
}

/*
 * Send a query to find out whether the join proposal
 * consensus request was accepted by the remote peer yet.
 */
static void
bdr_join_submit_outcome_request(uint64 handle)
{
	Oid paramTypes[1] = {TEXTOID};
	const char *paramValues[1];
	char handle_txt[MAX_DIGITS_INT64];
	int ret;
	BdrNodeInfo *local = bdr_get_cached_local_node_info();

	Assert(local->pgl_interface != NULL);
	Assert(!join.query_result_pending);

	snprintf(handle_txt, MAX_DIGITS_INT64, UINT64_FORMAT, handle);
	paramValues[0] = handle_txt;

	ret = PQsendQueryParams(join.conn,
							"SELECT bdr.consensus_message_outcome($1)",
							1, paramTypes, paramValues, NULL, NULL, 0);

	if (!ret)
	{
		ereport(WARNING,
				(errmsg("failed to submit consensus message outcome request - couldn't send query"),
				 errdetail("libpq: %s", PQerrorMessage(join.conn))));
		bdr_join_reset_connection();
	}

	join.query_result_pending = true;
}

/*
 * Get the reply to the query from bdr_join_submit_request, with
 * a handle we can use to track progress of our consensus message
 * on the join target.
 *
 * Returns 0 until result successfully read.
 */
static ConsensusProposalStatus
bdr_join_submit_outcome_get_result(void)
{
	ConsensusProposalStatus outcome;
	Assert(sizeof(ConsensusProposalStatus) == sizeof(int));

	Assert(join.query_result_pending);

	if (check_for_query_result())
	{
		PGresult *res = PQgetResult(join.conn);
		const char *val;

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			const char *msg = pstrdup(PQresultErrorMessage(res));
			PQclear(res);
			ereport(ERROR,
					(errmsg("failed to submit join request outcome query on join target"),
					 errdetail("libpq: %s", msg)));
		}

		val = PQgetvalue(res, 0, 0);
		if (sscanf(val, "%d", (int*)(&outcome)) != 1)
			elog(ERROR, "could not parse consensus message outcome from remote");

		PQclear(res);

		/*
		 * We know there's only one result set, so this really shouldn't
		 * block for long. It's a bit naughty to check it here without
		 * testing if we'll block first, but it really should be safe...
		 */
		res = PQgetResult(join.conn);
		Assert(res == NULL);

		join.query_result_pending = false;
	}

	return outcome;
}

/*
 * Continue the join process in BDR_NODE_STATE_JOIN_WAIT_CONFIRM state.
 *
 * We've submitted a join request, got a handle for it, and now we're waiting
 * for the outcome.
 *
 * Much like bdr_join_continue_submit, this asynchronously fires a request to
 * the join target to ask it the outcome of our request, then waits for a
 * reply. It repeats until we get a conclusive success or failure.
 */
static void
bdr_join_continue_wait_confirm(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	ExtraDataConsensusWait *extra;
	ConsensusProposalStatus outcome;
	Assert(cur_state->current == BDR_NODE_STATE_JOIN_WAIT_CONFIRM);
	extra = cur_state->extra_data;

	if (!join.query_result_pending)
	{
		/* send join request query to server */
		bdr_join_submit_outcome_request(extra->request_message_handle);
		join.query_result_pending = true;
	}

	/* wait for server to reply with progress handle for join */
	if (check_for_query_result())
	{
		outcome = bdr_join_submit_outcome_get_result();

		switch (outcome)
		{
			case CONSENSUS_MESSAGE_ACCEPTED:
			{
				/* join request submitted successfully */
				state_transition(cur_state, BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES,
					cur_state->join_target_id, NULL);
				break;
			}
			case CONSENSUS_MESSAGE_FAILED:
			{
				/*
				 * TODO: we should be able to transition back to
				 * BDR_NODE_STATE_JOIN_START if the request fails to achieve
				 * consensus, and retry. This will be important once we support
				 * majority consensus.
				 */
				ExtraDataJoinFailure new_extra;
				new_extra.reason = "join target could not achieve consensus on join request";
				state_transition(cur_state, BDR_NODE_STATE_JOIN_FAILED,
					cur_state->join_target_id, &new_extra);
				ereport(WARNING,
						(errmsg("BDR node join has failed - could not achieve consensus on join")));

				break;
			}
			case CONSENSUS_MESSAGE_IN_PROGRESS:
			{
				/*
				 * Peer hasn't got everyone to agree yet, so we'll just re-enter
				 * the loop next time around and submit a new query.
				 */
				/* TODO: back-off, or maybe blocking request on other end? */
			}
		}
	}
}

/*
 * A peer node says it wants to go into catchup mode and sent us a
 * BDR_MSG_NODE_CATCHUP_READY.
 * 
 * That peer might be us; we handle our own catchup mode request here too, in
 * response to a BDR_NODE_STATE_SEND_CATCHUP_READY state. In that case we'll
 * transition to BDR_NODE_STATE_STANDBY on commit.
 *
 * (This won't really require consensus, but it's simplest to treat it as if it does)
 *
 * At this point we need to create slots for the peer to use. The peer won't
 * be replaying data from them yet, but it should connect and advance them
 * so we don't retain excess resources. (TODO)
 *
 * TODO: should create an ephemeral slot here, and make it permanent on commit?
 */
void
bdr_join_handle_catchup_proposal(BdrMessage *msg)
{
	BdrNodeInfo		   *local;

	Assert(is_bdr_manager());
	Assert(msg->message_type == BDR_MSG_NODE_CATCHUP_READY);

	/*
	 * Catchup ready announcements are empty, with no payload,
	 * but we might want to add one later, so we don't check.
	 */

	local = bdr_get_local_node_info(false, false);

	if (local->bdr_node->node_id != msg->originator_id)
	{
		/*
		 * Just like in bdr_node_join_handle_proposal, we can't create the slot
		 * here if we might have done writes or if we expect to do 2PC. We have
		 * to transition to a temporary local state to queue the slot creation
		 * for after we accept this, instead.
		 */
		BdrStateEntry cur_state;
		state_get_expected(&cur_state, true, false,
			BDR_NODE_STATE_ACTIVE);
		state_transition(&cur_state, BDR_NODE_STATE_ACTIVE_SLOT_CREATE_PENDING,
			msg->originator_id, NULL);
	}
	else
	{
		/*
		 * We're processing a locally originated message. On consensus we need
		 * to transition to a new state to continue the join.
		 */
		BdrStateEntry cur_state;
		state_get_expected(&cur_state, true, false,
			BDR_NODE_STATE_SEND_CATCHUP_READY);
		state_transition(&cur_state, BDR_NODE_STATE_STANDBY,
			cur_state.join_target_id, NULL);
	}
}

/*
 * A node in catchup mode announces that it wants to join as a full peer
 * and sent us a BDR_MSG_NODE_ACTIVE message.
 *
 * That peer could be us, in which case we'll be in
 * BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE state and will transition to 
 * BDR_NODE_STATE_ACTIVE.
 */
void
bdr_join_handle_active_proposal(BdrMessage *msg)
{
	BdrNodeInfo		   *local, *remote;
	BdrStateEntry		cur_state;

	Assert(is_bdr_manager());
	Assert(msg->message_type == BDR_MSG_NODE_ACTIVE);

	/*
	 * Catchup ready announcements are empty, with no payload,
	 * but we might want to add one later, so we don't check.
	 */

	local = bdr_get_local_node_info(false, false);
	remote = bdr_get_node_info(msg->originator_id, false);

	if (local->bdr_node->node_id != remote->bdr_node->node_id)
	{
		state_get_expected(&cur_state, true, true, BDR_NODE_STATE_ACTIVE);

		/*
		 * We can now create a subscription to the node, to be started
		 * once we commit.
		 */
		bdr_create_subscription(local, remote, 0, false);
	}
	else
	{
		/*
		 * We're the joining node, so enable our subs to all peers.
		 */
		List	   *nodes;
		ListCell   *lc;

		state_get_expected(&cur_state, true, true,
			BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE);

		nodes = bdr_get_nodes_info(local->bdr_node_group->id);

		/*
		 * TODO: later we'll instead be switching these from catchup-only mode
		 * to actually replaying directly, after first switching the join
		 * subscription from catchup to normal replay.
		 */
		foreach (lc, nodes)
		{
			BdrNodeInfo	   *remote = lfirst(lc);
			List		   *subs;
			ListCell	   *lcsub;

			subs = bdr_get_node_subscriptions(remote->bdr_node->node_id);
			foreach (lcsub, subs)
			{
				PGLogicalSubscription *sub = lfirst(lcsub);
				sub->enabled = true;
				alter_subscription(sub);
			}
		}

		/* Enter fully joined steady state */
		state_transition(&cur_state, BDR_NODE_STATE_ACTIVE,
			cur_state.join_target_id, NULL);
	}

	/*
	 * TODO: should set local state to active/ready
	 */

	/*
	 * TODO: should signal the manager to start the subscription
	 * once we commit
	 */
}

static void
read_nodeinfo_required_attr(PGresult *res, int rownum, int colnum)
{
	if (PQgetisnull(res, rownum, colnum))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("column %s (number %d) in row %d of result was null",
				 		PQfname(res, colnum), colnum, rownum)));
	}
}

/*
 * Mirror of make_nodeinfo_result
 */
static BdrNodeInfo*
read_nodeinfo_result(PGresult *res, int rownum)
{
	BdrNodeInfo *info;
	char *val;

	info = palloc(sizeof(BdrNodeInfo));
	info->bdr_node = palloc0(sizeof(BdrNode));
	info->pgl_node = palloc0(sizeof(PGLogicalNode));
	info->pgl_interface = palloc0(sizeof(PGlogicalInterface));
	info->bdr_node_group = NULL;

	/*
	 * TODO: use SELECT *, and gracefully ignore missing fields
	 */
	if (PQnfields(res) < 10)
		elog(ERROR, "expected at least 10 fields, peer BDR too old?");

	if (rownum + 1 > PQntuples(res))
		elog(ERROR, "attempt to read row %d but only %d rows in output",
			 rownum, PQntuples(res));

	val = PQgetvalue(res, rownum, 0);
	if (sscanf(val, "%u", &info->bdr_node->node_id) != 1)
		elog(ERROR, "could not parse info node id '%s'", val);

	info->pgl_node->id = info->bdr_node->node_id;
	info->pgl_interface->nodeid = info->bdr_node->node_id;

	read_nodeinfo_required_attr(res, rownum, 1);
	info->pgl_node->name = pstrdup(PQgetvalue(res, rownum, 1));

	read_nodeinfo_required_attr(res, rownum, 2);
	val = PQgetvalue(res, rownum, 2);
	if (sscanf(val, "%u", &info->bdr_node->local_state) != 1)
		elog(ERROR, "could not parse info node state '%s'", val);

	read_nodeinfo_required_attr(res, rownum, 3);
	val = PQgetvalue(res, rownum, 3);
	if (sscanf(val, "%d", &info->bdr_node->seq_id) != 1)
		elog(ERROR, "could not parse info node sequence id '%s'", val);

	info->bdr_node->confirmed_our_join = false;

	if (!PQgetisnull(res, rownum, 4))
	{
		info->bdr_node_group = palloc0(sizeof(BdrNodeGroup));

		val = PQgetvalue(res, rownum, 4);
		if (sscanf(val, "%u", &info->bdr_node_group->id) != 1)
			elog(ERROR, "could not parse info nodegroup id '%s'", val);

		read_nodeinfo_required_attr(res, rownum, 5);
		info->bdr_node_group->name = pstrdup(PQgetvalue(res, rownum, 5));
		info->bdr_node->node_group_id = info->bdr_node_group->id;
	}
	else
	{
		info->bdr_node_group = NULL;
		info->bdr_node->node_group_id = 0;
	}

	read_nodeinfo_required_attr(res, rownum, 6);
	val = PQgetvalue(res, rownum, 6);
	if (sscanf(val, "%u", &info->pgl_interface->id) != 1)
		elog(ERROR, "could not parse pglogical interface id '%s'", val);

	read_nodeinfo_required_attr(res, rownum, 7);
	info->pgl_interface->name = pstrdup(PQgetvalue(res, rownum, 7));

	read_nodeinfo_required_attr(res, rownum, 8);
	info->pgl_interface->dsn = pstrdup(PQgetvalue(res, rownum, 8));

	read_nodeinfo_required_attr(res, rownum, 9);
	info->bdr_node->dbname = pstrdup(PQgetvalue(res, rownum, 9));

	check_nodeinfo(info);
	return info;
}

#define NODEINFO_FIELD_NAMES 

/*
 * Send query to probe a remote node to get BdrNodeInfo for the node.
 *
 * Returns 1 for successful dispatch, 0 for failure.
 */
static int
start_get_remote_node_info(PGconn *conn)
{
	return PQsendQuery(conn, "SELECT node_id, node_name, node_local_state, node_seq_id, nodegroup_id, nodegroup_name, pgl_interface_id, pgl_interface_name, pgl_interface_dsn, bdr_dbname FROM bdr.local_node_info()");
}

static BdrNodeInfo*
finish_get_remote_node_info(PGconn *conn)
{
	PGresult *res = PQgetResult(conn);
	BdrNodeInfo *remote;

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		const char *msg = pstrdup(PQresultErrorMessage(res));
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to get remote node info: %s", msg)));
	}

	if (PQntuples(res) == 0)
		elog(ERROR, "could not get remote node info: no tuples");

	remote = read_nodeinfo_result(res, 0);

	PQclear(res);

	/*
	 * We know there's only one result set, so this really shouldn't
	 * block for long. It's a bit naughty to check it here without
	 * testing if we'll block first, but it really should be safe...
	 */
	res = PQgetResult(join.conn);
	Assert(res == NULL);

	return remote;
}

/*
 * Synchronously probe a remote node to get BdrNodeInfo for the node.
 */
BdrNodeInfo *
get_remote_node_info(PGconn *conn)
{
	Assert(!is_bdr_manager());

	if (!start_get_remote_node_info(conn))
		ereport(ERROR,
				(errmsg("unable to send bdr.local_node_info query to remote"),
				 errdetail("libpq: %s", PQerrorMessage(conn))));

	for (;;)
	{
		if (!PQconsumeInput(conn))
		{
			ereport(ERROR,
					(errmsg("connection to peer broke while waiting for query response"),
					 errdetail("libpq: %s", PQerrorMessage(conn))));
			PQfinish(conn);
		}
		
		if (!PQisBusy(conn))
			return finish_get_remote_node_info(conn);

		backend_sleep_conn(2500 /* millis */, conn);
	}

	Assert(false); /* unreachable */
}

/*
 * Clone a remote BDR node's nodegroup to the local node and make the local node
 * a member of it. Create the default repset for the nodegroup in the process.
 *
 * Returns the created nodegroup id.
 */
void
bdr_join_copy_remote_nodegroup(BdrNodeInfo *local, BdrNodeInfo *remote)
{
	BdrNodeGroup	   *newgroup = palloc(sizeof(BdrNodeGroup));
	PGLogicalRepSet		repset;

	/*
	 * Look up the local node on the remote so we can get the info
	 * we need about the newgroup.
	 */
	Assert(local->bdr_node_group == NULL);

	/*
	 * Create local newgroup with the same ID, a matching replication set, and
	 * bind our node to it. Very similar to what happens in
	 * bdr_create_newgroup_sql().
	 */
	repset.id = InvalidOid;
	repset.nodeid = local->bdr_node->node_id;
	repset.name = (char*)remote->bdr_node_group->name;
	repset.replicate_insert = true;
	repset.replicate_update = true;
	repset.replicate_delete = true;
	repset.replicate_truncate = true;
	repset.isinternal = true;

	newgroup->id = remote->bdr_node_group->id;
	newgroup->name = remote->bdr_node_group->name;
	newgroup->default_repset = create_replication_set(&repset);
	if (bdr_nodegroup_create(newgroup) != remote->bdr_node_group->id)
	{
		/* shouldn't happen */
		elog(ERROR, "failed to create newgroup with id %u",
			 remote->bdr_node_group->id);
	}

	/* Assign the newgroup to the local node */
	local->bdr_node->node_group_id = newgroup->id;
	bdr_modify_node(local->bdr_node);

	local->bdr_node_group = newgroup;
}

/*
 * Copy the node entry from the remote BdrNodeInfo to the local
 * node.
 */
void
bdr_join_copy_remote_node(BdrNodeInfo *local, BdrNodeInfo *remote)
{
	Assert(local->bdr_node->node_id != remote->bdr_node->node_id);

	create_node(remote->pgl_node);
	create_node_interface(remote->pgl_interface);
	bdr_node_create(remote->bdr_node);
}

/*
 * Copy all BDR node catalog entries that are members of the identified
 * nodegroup to the local node.
 */
static void
bdr_join_start_copy_remote_nodes(BdrNodeInfo *local)
{
	Oid			paramTypes[1] = {OIDOID};
	const char *paramValues[1];
	char		nodeid[MAX_DIGITS_INT32];
	int			ret;

	/*
	 * We use a helper function on the other end to collect the info, shielding
	 * us somewhat from catalog changes and letting us fetch the pgl and bdr info
	 * all at once.
	 */
	snprintf(nodeid, 30, "%u", local->bdr_node_group->id);
	paramValues[0] = nodeid;
	ret = PQsendQueryParams(join.conn, "SELECT node_id, node_name, node_local_state, node_seq_id, nodegroup_id, nodegroup_name, pgl_interface_id, pgl_interface_name, pgl_interface_dsn, bdr_dbname FROM bdr.node_group_member_info($1)",
					   1, paramTypes, paramValues, NULL, NULL, 0);

	if (!ret)
	{
		ereport(WARNING,
				(errmsg("failed to submit request for remote node list - couldn't send query"),
				 errdetail("libpq: %s", PQerrorMessage(join.conn))));
		bdr_join_reset_connection();
	}

	join.query_result_pending = true;
}

/*
 * Finish copying the remote nodes to the local node, processing the
 * query results from bdr_join_start_copy_remote_nodes.
 */
static void
bdr_join_finish_copy_remote_nodes(BdrNodeInfo *local)
{
	int i;
	PGresult *res = PQgetResult(join.conn);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		const char *msg = pstrdup(PQerrorMessage(join.conn));
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to get remote node list: %s", msg)));
	}

	if (PQntuples(res) == 0)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("failed to get remote node list: no remote nodes found with nodegroup id %u", local->bdr_node_group->id)));
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		BdrNodeInfo *peer = read_nodeinfo_result(res, i);
		BdrNodeInfo *local_copy;
		Assert(peer->bdr_node_group->id == local->bdr_node_group->id);

		if (peer->bdr_node->node_id == local->bdr_node->node_id)
			continue;
		
		local_copy = bdr_get_node_info(peer->pgl_node->id, true);

		/* TODO: do a proper upsert here, not just create-if-exists */
		if (!local_copy || local_copy->pgl_node == NULL)
			create_node(peer->pgl_node);
		if (!local_copy || local_copy->pgl_interface == NULL)
			create_node_interface(peer->pgl_interface);
		if (!local_copy || local_copy->bdr_node == NULL)
			bdr_node_create(peer->bdr_node);
	}

	/*
	 * We know there's only one result set, so this really shouldn't
	 * block for long. It's a bit naughty to check it here without
	 * testing if we'll block first, but it really should be safe...
	 */
	res = PQgetResult(join.conn);
	Assert(res == NULL);

	join.query_result_pending = false;
}

static void
bdr_join_continue_copy_remote_nodes(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES);

	if (!join.query_result_pending)
	{
		/* send join request query to server */
		bdr_join_start_copy_remote_nodes(local);
		join.query_result_pending = true;
	}

	/* wait for server to reply with progress handle for join */
	if (check_for_query_result())
	{
		bdr_join_finish_copy_remote_nodes(local);

		state_transition(cur_state, BDR_NODE_JOIN_SUBSCRIBE_JOIN_TARGET,
			cur_state->join_target_id, NULL);
	}
}

static void
bdr_join_continue_get_catchup_lsn(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN);

	if (!join.query_result_pending)
	{
		if (PQsendQuery(join.conn, "SELECT * FROM pg_current_wal_insert_lsn()"))
			join.query_result_pending = true;
		else
		{
			ereport(WARNING,
					(errmsg("failed to submit remote lsn request on target - couldn't send query"),
					 errdetail("libpq: %s", PQerrorMessage(join.conn))));
			bdr_join_reset_connection();
		}
	}

	/* wait for server to reply with progress handle for join */
	if (check_for_query_result())
	{
		PGresult *res;
		ExtraDataJoinWaitCatchup extra;
		const char *val;

		res = PQgetResult(join.conn);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			const char *msg = pstrdup(PQresultErrorMessage(res));
			ereport(ERROR,
					(errmsg("failed to query join target for pg_current_wal_insert_lsn()"),
					 errdetail("libpq: %s: %s", PQresStatus(PQresultStatus(res)), msg)));
		}

		val = PQgetvalue(res, 0, 0);
		elog(bdr_debug_level, "BDR join of %u waiting for replay past origin lsn %s",
			 bdr_get_local_nodeid(), val);
		extra.min_catchup_lsn =
			DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(val)));

		PQclear(res);
		state_transition(cur_state, BDR_NODE_STATE_JOIN_WAIT_CATCHUP,
			cur_state->join_target_id, &extra);

		/*
		 * We know there's only one result set, so this really shouldn't
		 * block for long. It's a bit naughty to check it here without
		 * testing if we'll block first, but it really should be safe...
		 */
		res = PQgetResult(join.conn);
		Assert(res == NULL);
	}
}

static char*
bdr_gen_sub_name(BdrNodeInfo *subscriber, BdrNodeInfo *provider)
{
	StringInfoData	sub_name;

	Assert(provider->bdr_node_group != NULL);
	Assert(subscriber->bdr_node_group != NULL);
	Assert(provider->bdr_node_group->id == subscriber->bdr_node_group->id);
	Assert(provider->bdr_node->node_id != subscriber->bdr_node->node_id);

	initStringInfo(&sub_name);
	/*
	 * Annoyingly, sub names must be unique across all providers on a
	 * subscriber so we have to qualify the sub name by the provider name.
	 *
	 * This is redundant with the provider name in the slot created on the peer
	 * end, since it also has the subscriber name in it, but not much to be
	 * done about it.
	 */
	appendStringInfo(&sub_name, "%s_%s",
		provider->bdr_node_group->name,
		provider->pgl_node->name);
	return sub_name.data;
}

/*
 * Create a subscription, optionally initially disabled, from 'local' to 'remote, possibly
 * dumping data too.
 */
static void
bdr_create_subscription(BdrNodeInfo *local, BdrNodeInfo *remote, int apply_delay_ms, bool for_join)
{
	List				   *replication_sets = NIL;
	NameData				slot_name;
	char				   *sub_name;
	PGLogicalSubscription	sub;
	PGLSubscriptionWriter	sub_writer;
	PGLogicalSyncStatus		sync;
	Interval				apply_delay;

	elog(bdr_debug_level, "creating subscription for %u on %u",
		 remote->bdr_node->node_id, local->bdr_node->node_id);

	/*
	 * For now we support only one replication set, with the same name as the
	 * BDR group. (DDL should be done through it too).
	 */
	replication_sets = lappend(replication_sets, pstrdup(local->bdr_node_group->name));

	/*
	 * Make sure there's no existing BDR subscription to this node.
	 */
	check_overlapping_replication_sets(replication_sets, 
		remote->pgl_node->id, remote->pgl_node->name);

	sub_name = bdr_gen_sub_name(local, remote);

	check_nodeinfo(local);
	check_nodeinfo(remote);

	if (local->bdr_node->node_id == remote->bdr_node->node_id)
	{
		Assert(false);
		elog(ERROR, "attempt to subscribe to own node");
	}

	/*
	 * Create the subscription using the remote node and interface
	 * we copied earlier.
	 */
	sub.id = InvalidOid;
	sub.name = sub_name;
	sub.origin_if = remote->pgl_interface;
	sub.target_if = local->pgl_interface;
	sub.replication_sets = replication_sets;
	/*
	 * BDR handles forwarding separately in the output plugin hooks
	 * so it can forward by nodegroup, not origin list.
	 */
	sub.forward_origins = NIL;
	/*
	 * TODO: in future we should enable subs in catchup-only mode
	 * of some kind.
	 */
	sub.enabled = true;
	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  remote->pgl_node->name, sub_name);
	sub.slot_name = pstrdup(NameStr(slot_name));

	interval_from_ms(apply_delay_ms, &apply_delay);
	sub.apply_delay = &apply_delay;

	sub.isinternal = true;

	create_subscription(&sub);

	/*
	 * Create the writer for the subscription.
	 */
	sub_writer.id = InvalidOid;
	sub_writer.sub_id = sub.id;
	sub_writer.name = sub_name;
	sub_writer.writer = "HeapWriter";
	sub_writer.options = NIL;

	pgl_create_subscription_writer(&sub_writer);

	/*
	 * Prepare initial sync. BDR will only ever do two kinds - a full dump,
	 * on the join target, or no sync for other nodes.
	 */
	if (for_join)
		sync.kind = SYNC_KIND_FULL;
	else
		sync.kind = SYNC_KIND_INIT;

	sync.subid = sub.id;
	sync.nspname = NULL;
	sync.relname = NULL;
	sync.status = SYNC_STATUS_INIT;
	create_local_sync_status(&sync);

	/* Create the replication origin */
	(void) replorigin_create(sub.slot_name);

	pglogical_subscription_changed(sub.id);

	/*
	 * TODO: create bdr.subscriptions entry for this sub
	 */
	elog(bdr_debug_level, "created subscription for %u on %u",
		 remote->bdr_node->node_id, local->bdr_node->node_id);
}

static void
bdr_join_continue_subscribe_join_target(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_JOIN_SUBSCRIBE_JOIN_TARGET);

	if (!join.query_result_pending)
	{
		if (start_get_remote_node_info(join.conn))
			join.query_result_pending = true;
		else
		{
			ereport(WARNING,
					(errmsg("unable to submit remote node info query"),
					 errdetail("libpq: %s", PQerrorMessage(join.conn))));
			bdr_join_reset_connection();
		}
	}

	if (check_for_query_result())
	{
		BdrNodeInfo *remote = finish_get_remote_node_info(join.conn);
		join.query_result_pending = false;
		bdr_create_subscription(local, remote, 0, true);

		/*
		 * Now that we've created a subscription to the target we can start
		 * talking to it.
		 */
		bdr_start_consensus(bdr_max_nodes, cur_state->current);
		bdr_messaging_refresh_nodes();

		state_transition(cur_state, BDR_NODE_STATE_WAIT_SUBSCRIBE_COMPLETE,
			cur_state->join_target_id, NULL);
	}
}

static void
bdr_join_continue_wait_subscribe_complete(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	List				   *subs;
	PGLogicalSubscription  *sub;
	PGLogicalSyncStatus	   *sync;

	Assert(cur_state->current == BDR_NODE_STATE_WAIT_SUBSCRIBE_COMPLETE);

	subs = bdr_get_node_subscriptions(bdr_get_local_nodeid());

	/*
	 * Only one BDR sub should exist. We created it earlier, we're just waiting
	 * for it to sync now.
	 */
	Assert(list_length(subs) == 1);
	sub = linitial(subs);
	Assert(sub->target->id == bdr_get_local_nodeid());
	Assert(sub->origin->id == cur_state->join_target_id);

	/*
	 * Is the subscription synced up yet?
	 */
	sync = get_subscription_sync_status(sub->id, true);
	if (sync && sync->status == SYNC_STATUS_READY)
	{	
		state_transition(cur_state, BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN,
			cur_state->join_target_id, NULL);
	}

}

static void
bdr_join_continue_wait_catchup(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	XLogRecPtr cur_progress;
	ExtraDataJoinWaitCatchup *extra;
	RepOriginId origin_id;
	List *subs;
	PGLogicalSubscription *sub;

	Assert(cur_state->current == BDR_NODE_STATE_JOIN_WAIT_CATCHUP);
	extra = cur_state->extra_data;

	subs = bdr_get_node_subscriptions(bdr_get_local_nodeid());
	/* Only one BDR sub should exist right now */
	Assert(list_length(subs) == 1);
	sub = linitial(subs);
	Assert(sub->target->id == bdr_get_local_nodeid());
	Assert(sub->origin->id == cur_state->join_target_id);

	origin_id = replorigin_by_name(sub->slot_name, false);
	Assert(origin_id != InvalidRepOriginId);

	/*
	 * We need to continue replay until our subscription to the join
	 * target overtakes the upstream's insert lsn from a point after
	 * we took the initial dump snapshot.
	 */
	cur_progress = replorigin_get_progress(origin_id, false);
	if ( cur_progress > extra->min_catchup_lsn )
	{
		elog(LOG, "%u replayed past minimum recovery lsn %X/%X",
			 bdr_get_local_nodeid(),
			 (uint32)(extra->min_catchup_lsn>>32), (uint32)extra->min_catchup_lsn);
		state_transition(cur_state, BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS,
			cur_state->join_target_id, NULL);
	}
	else
		elog(bdr_debug_level, "%u waiting for origin '%s' to replay past %X/%X; currently %X/%X",
			 bdr_get_local_nodeid(), sub->slot_name,
			 (uint32)(extra->min_catchup_lsn>>32), (uint32)extra->min_catchup_lsn,
			 (uint32)(cur_progress>>32), (uint32)cur_progress);
}

static void
bdr_join_continue_copy_repset_memberships(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS);

	elog(WARNING, "replication set memberships copy not implemented");

	state_transition(cur_state, BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS,
		cur_state->join_target_id, NULL);
}

static void
bdr_join_continue_create_subscriptions(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	List	   *nodes;
	ListCell   *lc;

	Assert(cur_state->current == BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS);

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);
	
	foreach (lc, nodes)
	{
		BdrNodeInfo *remote = lfirst(lc);

		if (remote->bdr_node->node_id == join.target->bdr_node->node_id)
			continue;

		if (remote->bdr_node->node_id == local->bdr_node->node_id)
			continue;

		bdr_create_subscription(local, remote, 0, false);
	}

	bdr_messaging_refresh_nodes();

	state_transition(cur_state, BDR_NODE_STATE_SEND_CATCHUP_READY,
		cur_state->join_target_id, NULL);
}

static void
bdr_join_continue_send_catchup_ready(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	uint64 handle;

	Assert(cur_state->current == BDR_NODE_STATE_SEND_CATCHUP_READY);
	Assert(IsTransactionState());
	CommitTransactionCommand();

	/*
	 * Local processing of this message via bdr_join_handle_catchup_proposal
	 * will transition us to BDR_NODE_STATE_STANDBY, which is how we exit
	 * this state.
	 */
	handle = bdr_msgs_enqueue_one(BDR_MSG_NODE_CATCHUP_READY, NULL);
	if (handle == 0)
		elog(ERROR, "failed to submit BDR_MSG_NODE_CATCHUP_READY consensus message");

	elog(WARNING, "waiting for all nodes to confirm catchup ready not yet implemented");
}

static void
bdr_join_continue_standby(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_STANDBY);

	/*
	 * TODO: here we should wait for some external signal to delay
	 * promotion to active node, and stay in standby catchup
	 * mode indefinitely.
	 */

	elog(LOG, "skipping standby and going straight to active");

	state_transition(cur_state, BDR_NODE_STATE_CREATE_SLOTS,
		cur_state->join_target_id, NULL);
}

static void
bdr_join_continue_request_global_seq_id(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	uint64 handle = 0;
	ExtraDataConsensusWait extra;

	Assert(cur_state->current == BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID);

	/*
	 * TODO: here we should submit a global consensus request
	 * (via our join target), assigning ourselves a new global
	 * sequence ID based on what we see is free.
	 */

	elog(WARNING, "requesting global sequence ID assignment not implemented");

	extra.request_message_handle = handle;
	state_transition(cur_state, BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID,
		cur_state->join_target_id, &extra);
}

static void
bdr_join_continue_wait_global_seq_id(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	ExtraDataConsensusWait *extra;
	Assert(cur_state->current == BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID);

 	extra = cur_state->extra_data;

	elog(WARNING, "waiting for global sequence ID assignment not implemented");

	state_transition(cur_state, BDR_NODE_STATE_CREATE_SLOTS,
		cur_state->join_target_id, NULL);
}

/*
 * Unlike for pglogical, BDR creates replication slots for its peers directly.
 * The peers don't have to ask for slot creation via a walsender command or SQL
 * function call. This is done so that nodes can create slots for a peer as
 * part of a consensus message exchange during setup.
 *
 * So these are inbound slots, which other peers will use to talk to us.
 * We expect the peer to in turn create the slots we need to talk to it.
 */
static void
bdr_join_continue_create_slots(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	List	   *nodes;
	ListCell   *lc;

	Assert(cur_state->current == BDR_NODE_STATE_CREATE_SLOTS);

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);

	foreach (lc, nodes)
	{
		BdrNodeInfo *remote = lfirst(lc);

		if (remote->bdr_node->node_id == local->bdr_node->node_id)
			continue;

		bdr_join_create_slot(local, remote);
	}

	state_transition(cur_state, BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE,
		cur_state->join_target_id, NULL);
}

static void
bdr_join_continue_send_active_announce(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	uint64 handle;

	Assert(cur_state->current == BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE);
	Assert(IsTransactionState());
	CommitTransactionCommand();

	/*
	 * This consensus message will tell our peers we're going
	 * active. When our own handler for it is invoked, it'll also
	 * transition us to BDR_NODE_STATE_ACTIVE.
	 */
	handle = bdr_msgs_enqueue_one(BDR_MSG_NODE_ACTIVE, NULL);
	if (handle == 0)
		elog(ERROR, "failed to submit BDR_MSG_NODE_ACTIVE consensus message");

	/* TODO: wait for consensus in an extra state phase here */
	elog(WARNING, "not waiting for consensus on active announce, not implemented");
}

/*
 *
 * Pg's replication slots code lacks any interface to check if a slot exists.
 * You can acquire it, but either it'll block if in use, or with nowait mode,
 * it'll ERROR. So there's no nice way to ask "does this slot exist?". And
 * there's no way to create one if it doesn't exist.
 *
 * So we must do the test ourselves. This is racey with a concurrent slot
 * creation, but ... "don't do that". If someone does create a conflicting
 * slot we'll error out when we try to create, and re-check when we run again.
 *
 * Returns true if the slot exists and is a pglogical slot. false if no such
 * slot exists. ERROR's if the slot exists but isn't a pglogical slot.
 */
static bool
bdr_replication_slot_exists(Name slot_name)
{
	ReplicationSlot    *found = NULL;
	int					i;

	LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && strcmp(NameStr(*slot_name), NameStr(s->data.name)) == 0)
		{
			found = s;
			break;
		}
	}

	if (found && found->data.database != MyDatabaseId)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("slot %s exists but is not a logical slot for the current database",
					 NameStr(*slot_name)),
				 errdetail("Expected slot for %u, found slot for %u",
				 	 MyDatabaseId, found->data.database)));

	if (found && found->data.persistency != RS_PERSISTENT)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("slot %s exists but is not a persistent slot",
					 NameStr(*slot_name))));

	if (found && strcmp(NameStr(found->data.plugin), "pglogical") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("slot %s exists but uses plugin '%s' not expected 'pglogical'",
					 NameStr(*slot_name), NameStr(found->data.plugin))));

	LWLockRelease(ReplicationSlotAllocationLock);

	return found != NULL;
}

static void
bdr_join_create_slot(BdrNodeInfo *local, BdrNodeInfo *remote)
{
	char		*sub_name;
	NameData	slot_name;
	LogicalDecodingContext *ctx = NULL;

	/*
	 * Subscription names here are from the PoV of the remote
	 * node, since this creation is happening on the provider.
	 */
	sub_name = bdr_gen_sub_name(remote, local);

	gen_slot_name(&slot_name, remote->bdr_node->dbname,
				  local->pgl_node->name, sub_name);

	/*
	 * Slot creation is NOT transactional. If we're being asked to create a
	 * slot for peers we could've failed after some slots were created, so
	 * we can't assume a clean slate here.
	 *
	 * An already-existing pglogical slot for this db with the right name
	 * is fine to use, since it must be at or behind the position a new
	 * slot would get created at.
	 *
	 * We don't hold a lock over these two, so someone could create the
	 * slot after we check it, but then we'll just ERROR in creation and
	 * retry.
	 */
	if (bdr_replication_slot_exists(&slot_name))
		return;

	/*
	 * See: pg_create_logical_replication_slot for all this stuff; rest is mostly
	 * verbatim from there.
	 */
	CheckLogicalDecodingRequirements();

	/*
	 * Acquire a logical decoding slot, this will check for conflicting names.
	 * Initially create persistent slot as ephemeral - that allows us to
	 * nicely handle errors during initialization because it'll get dropped if
	 * this transaction fails. We'll make it persistent at the end. Temporary
	 * slots can be created as temporary from beginning as they get dropped on
	 * error as well.
	 */
	ReplicationSlotCreate(NameStr(slot_name), true, RS_TEMPORARY);

	/*
	 * Create logical decoding context, to build the initial snapshot.
	 */
	ctx = CreateInitDecodingContext("pglogical", NIL,
									false,	/* do not build snapshot */
									logical_read_local_xlog_page, NULL, NULL,
									NULL);

	/* build initial snapshot, might take a while */
	DecodingContextFindStartpoint(ctx);

	/* don't need the decoding context anymore */
	FreeDecodingContext(ctx);

	/* ok, slot is now fully created, mark it as persistent if needed */
	ReplicationSlotPersist();
	ReplicationSlotRelease();
}

static void
maintain_join_context(void)
{
	if (join.mctx == NULL)
		join.mctx = AllocSetContextCreate(TopMemoryContext,
										  "bdr_join",
										  ALLOCSET_DEFAULT_SIZES);

	bdr_cache_local_nodeinfo();
}

/*
 * We need a wait-event slot for join so we can use it for the socket we use to
 * talk to peer node(s).
 */
int
bdr_join_get_wait_event_space_needed(void)
{
	return 1;
}

/*
 * A wait event has come in. If it's for our connection to the remote,
 * hand it off to whatever the current join phase handler is.
 */
void
bdr_join_wait_event(struct WaitEvent *events, int nevents,
						 long *max_next_wait_ms)
{
	int i;
	for (i = 0; i < nevents; i++)
	{
		if (events[i].pos == join.wait_event_pos)
		{
			Assert(events[i].user_data == (void*)&join);
			/*
			 * We don't have to call into bdr_join_continue
			 * or bdr_state_dispatch here. The manager will
			 * do it for us in its own wait event handler.
			 *
			 * Right now we have nothing at all to do here
			 * since we'll poll our connection whenever we're
			 * woken up.
			 */
			break;
		}
	}
}

static void
bdr_join_wait_event_set_register(void)
{
	Assert(join.wait_set != NULL);
	Assert(join.conn != NULL && PQstatus(join.conn) == CONNECTION_OK);
	Assert(join.wait_event_pos == -1);

	/*
	 * We only need WL_SOCKET_READABLE here. We'll always consume from a socket
	 * if something's readable, and we're not bothering to do non-blocking
	 * sends since we won't ever exceed our send buffer size.
	 */
	join.wait_event_pos = AddWaitEventToSet(join.wait_set,
											WL_SOCKET_READABLE,
											PQsocket(join.conn),
											NULL, &join);

	Assert(join.wait_event_pos != -1);
}

/*
 * Re-register any libpq connection with the wait event set.
 *
 * Assume the socket wants to be woken for everything; next 
 * time we get woken we'll update the setting.
 */
void
bdr_join_wait_event_set_recreated(struct WaitEventSet *new_set)
{
	if (join.wait_set == new_set)
		return;
	join.wait_set = new_set;
	if (join.conn != NULL
		&& PQstatus(join.conn) == CONNECTION_OK
		&& join.wait_event_pos != -1)
	{
		join.wait_event_pos = -1;
		bdr_join_wait_event_set_register();
	}
}

/*
 * The manager state machine calls into here to continue a BDR
 * node join. From here we dispatch to one or more non-blocking
 * routines to continue asynchronous join processes.
 */
void
bdr_join_continue(BdrNodeState cur_state,
	long *max_next_wait_msecs)
{
	BdrStateEntry locked_state;
	BdrNodeInfo *local;

	maintain_join_context();
 	local = bdr_get_cached_local_node_info();

	StartTransactionCommand();
	/* Lock the state and decode extradata */
	state_get_expected(&locked_state, true, true, cur_state);

	if (bdr_join_maintain_conn(local, locked_state.join_target_id))
	{
		switch (locked_state.current)
		{
			case BDR_NODE_STATE_JOIN_START:
				bdr_join_continue_join_start(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
				bdr_join_continue_wait_confirm(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES:
				bdr_join_continue_copy_remote_nodes(&locked_state, local);
				break;

			case BDR_NODE_JOIN_SUBSCRIBE_JOIN_TARGET:
				bdr_join_continue_subscribe_join_target(&locked_state, local);
				break;

			case BDR_NODE_STATE_WAIT_SUBSCRIBE_COMPLETE:
				bdr_join_continue_wait_subscribe_complete(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN:
				bdr_join_continue_get_catchup_lsn(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
				bdr_join_continue_wait_catchup(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS:
				bdr_join_continue_copy_repset_memberships(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS:
				bdr_join_continue_create_subscriptions(&locked_state, local);
				break;

			case BDR_NODE_STATE_SEND_CATCHUP_READY:
				bdr_join_continue_send_catchup_ready(&locked_state, local);
				break;

			case BDR_NODE_STATE_STANDBY:
				bdr_join_continue_standby(&locked_state, local);
				break;

			case BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID:
				bdr_join_continue_request_global_seq_id(&locked_state, local);
				break;

			case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
				bdr_join_continue_wait_global_seq_id(&locked_state, local);
				break;

			case BDR_NODE_STATE_CREATE_SLOTS:
				bdr_join_continue_create_slots(&locked_state, local);
				break;

			case BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE:
				bdr_join_continue_send_active_announce(&locked_state, local);
				break;

			default:
				/* shouldn't be called for other states */
				Assert(false);
				elog(ERROR, "unhandled case");
		}
	}

	if (IsTransactionState())
	{
		/*
		 * One of the above may have committed the transaction we started at
		 * the beginning of this call in order to submit a consensus message
		 * instead. If that's the case we'd better not try to commit it.
		 */
		if (bdr_messaging_active_nodeid() == 0)
			CommitTransactionCommand();
	}

	/*
	 * HACK HACK HACK
	 * TODO
	 *
	 * Should only be set for the few areas where we must poll.
	 */
	*max_next_wait_msecs = 1000;
}
