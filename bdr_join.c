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
#include "pglogical_rpc.h"

#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_functions.h"
#include "bdr_join.h"
#include "bdr_manager.h"
#include "bdr_state.h"
#include "bdr_worker.h"
	
/* We cannot use ERRCODE_DUPLICATE_OBJECT because it's bit-packed */
#define SQLSTATE_DUPLICATE_OBJECT "42710"

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

	TimestampTz last_replay_progress_update;
};

static const int CATCHUP_REPLAY_PROGRESS_UPDATE_FREQUENCY_MS = 1000;

struct BdrJoinProgress join = { NULL, NULL, false, NULL, -1, NULL, 0};

static void bdr_create_local_slot(BdrNodeInfo *local, BdrNodeInfo *remote);
static void bdr_create_subscription(BdrNodeInfo *local, BdrNodeInfo *remote,
	int apply_delay_ms, bool for_join, char initial_mode,
	bool initially_enabled);
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
	Assert(local->bdr_node->local_state == BDR_PEER_STATE_JOINING);
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
static MNConsensusStatus
bdr_join_submit_get_result(void)
{
	MNConsensusStatus outcome;
	Assert(sizeof(MNConsensusStatus) == sizeof(int));

	Assert(join.query_result_pending);

	if (check_for_query_result())
	{
		PGresult *res = PQgetResult(join.conn);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			const char *msg = pstrdup(PQresultErrorMessage(res));
			PQclear(res);
			ereport(ERROR,
					(errmsg("failed to submit join request outcome query on join target"),
					 errdetail("libpq: %s", msg)));
		}

		outcome = mn_consensus_status_from_str(PQgetvalue(res, 0, 0));

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
		MNConsensusStatus outcome;

		outcome = bdr_join_submit_get_result();

		switch (outcome)
		{
			case MNCONSENSUS_EXECUTED:
			{
				/* join request submitted successfully */
				state_transition(cur_state, BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES,
					NULL);
				break;
			}
			case MNCONSENSUS_FAILED:
			{
				/*
				 * TODO: we should be able to transition back to
				 * BDR_NODE_STATE_JOIN_START if the request fails to achieve
				 * consensus, and retry. This will be important once we support
				 * majority consensus.
				 */
				ExtraDataJoinFailure fail_extra;
				fail_extra.reason = "join target could not achieve consensus on join request";
				state_transition(cur_state, BDR_NODE_STATE_JOIN_FAILED,
					&fail_extra);
				ereport(WARNING,
						(errmsg("BDR node join has failed - could not achieve consensus on join")));

				break;
			}
			case MNCONSENSUS_NO_LEADER:
			{
				/* No leader, we'll need to resubmit the request. */
				join.query_result_pending = false;
				break;
			}
			default:
				elog(ERROR, "unexpected outcome %u", outcome);
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
	BdrMsgJoinRequest  *req = (BdrMsgJoinRequest *) msg;
	BdrNodeInfo		   *local;
	BdrNodeGroup	   *local_nodegroup;
	BdrNode				bnode;
	PGLogicalNode		pnode;
	PGlogicalInterface	pnodeif;
	BdrStateEntry		cur_state;

	Assert(msg->message_type == BDR_MSG_NODE_JOIN_REQUEST);

	/*
	 * We might be replaying our own join request, if that's the case
	 * return.
	 */
	local = bdr_get_local_node_info(false, true);
	if (local->bdr_node->node_id == req->joining_node_id)
		return;

	/* FIXME RM#1118: This is bogus, any node active in messaging could get this */
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
	bnode.local_state = req->joining_node_state;
	bnode.seq_id = 0;
	bnode.dbname = req->joining_node_dbname;

	if (req->joining_node_state != BDR_PEER_STATE_JOINING)
		elog(WARNING, "joining node reported unexpected initial state %s, expected %s",
			 bdr_peer_state_name(req->joining_node_state),
			 bdr_peer_state_name(BDR_PEER_STATE_JOINING));

	pnodeif.id = req->joining_node_if_id;
	pnodeif.name = req->joining_node_if_name;
	pnodeif.nodeid = pnode.id;
	pnodeif.dsn = req->joining_node_if_dsn;

	/* TODO: do upserts here in case pgl node or even bdr node exists already */
	create_node(&pnode);
	create_node_interface(&pnodeif);
	bdr_node_create(&bnode);

	/*
	 * TODO: move addition of the peer node from prepare
	 * phase to accept phase callback
	 */
//	mn_consensus_add_or_update_node(bnode.node_id, pnodeif.dsn, false);

	/*
	 * Ideally we'd create the peer's replication slot here, transactionally,
	 * so that it'd be bound by our concensus negotation. It wouldn't be left dangling
	 * if the join failed, couldn't be left un-created by accident, etc.
	 *
	 * But we can't, because of the issues outlined in bdr_create_local_slot().
	 *
	 * So we let the peer connect over walsender protocol and make the slot.
	 * We can't even wait for that to succeed before we ack, because decoding
	 * won't finish the slot creation until our xact commits.
	 */

	/*
	 * We don't subscribe to the node yet, that only happens once it goes
	 * active.
	 */
}

/*
 * Get the reply to the query from bdr_join_submit_request, with
 * a handle we can use to track progress of our consensus message
 * on the join target.
 *
 * Returns 0 until result successfully read.
 */
static MNConsensusStatus
bdr_join_submit_outcome_get_result(void)
{
	MNConsensusStatus outcome;
	Assert(sizeof(MNConsensusStatus) == sizeof(int));

	Assert(join.query_result_pending);

	if (check_for_query_result())
	{
		PGresult *res = PQgetResult(join.conn);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			const char *msg = pstrdup(PQresultErrorMessage(res));
			PQclear(res);
			ereport(ERROR,
					(errmsg("failed to submit join request outcome query on join target"),
					 errdetail("libpq: %s", msg)));
		}

		outcome = mn_consensus_status_from_str(PQgetvalue(res, 0, 0));

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
 * A peer node says it wants to go into catchup mode and sent us a
 * BDR_MSG_NODE_STANDBY_READY.
 *
 * That peer might be us; we handle our own standby mode request here too, in
 * response to a BDR_NODE_STATE_SEND_STANDBY_READY state. In that case we'll
 * transition to BDR_NODE_STATE_STANDBY on commit.
 */
void
bdr_join_handle_standby_proposal(BdrMessage *msg)
{
	BdrNodeInfo		   *local;

	Assert(msg->message_type == BDR_MSG_NODE_STANDBY_READY);

	/*
	 * Catchup ready announcements are empty, with no payload,
	 * but we might want to add one later, so we don't check.
	 */
	local = bdr_get_local_node_info(false, false);

	if (local->bdr_node->node_id != msg->origin_id)
	{
		BdrStateEntry	cur_state;
		BdrNode		   *peer;

		/* FIXME RM#1118: this is bogus, any joining or standby node could get this */
		state_get_expected(&cur_state, true, false,
			BDR_NODE_STATE_ACTIVE);

		peer = bdr_get_node(msg->origin_id, false);
		if (peer->local_state != BDR_PEER_STATE_JOINING)
			elog(WARNING, "unexpected peer node state on %u: %s during catchup proposal, expected %s",
				 peer->node_id,
				 bdr_peer_state_name(peer->local_state),
				 bdr_peer_state_name(BDR_PEER_STATE_JOINING));
		else
			elog(bdr_debug_level, "updating peer %u status from %s to %s",
				 peer->node_id,
				 bdr_peer_state_name(peer->local_state),
				 bdr_peer_state_name(BDR_PEER_STATE_STANDBY));

		peer->local_state = BDR_PEER_STATE_STANDBY;

		bdr_modify_node(peer);

		bdr_consensus_refresh_nodes(cur_state.current);
	}
	else
	{
		BdrPeerState old_local_state;

		/*
		 * We're processing a locally originated message. On consensus we need
		 * to transition to a new state to continue the join.
		 */
		BdrStateEntry cur_state;

		state_get_expected(&cur_state, true, false,
			BDR_NODE_STATE_SEND_STANDBY_READY);

		bdr_consensus_refresh_nodes(cur_state.current);

		/*
		 * We've reached local standby mode and peers will update their idea of
		 * our state accordingly. So we should make sure our local entry
		 * matches, even though we don't really use it for much.
		 */
		local = bdr_get_local_node_info(true, false);
		old_local_state = local->bdr_node->local_state;
		local->bdr_node->local_state = BDR_PEER_STATE_STANDBY;
		elog(bdr_debug_level, "%s updated local state from %s to %s",
			local->pgl_node->name, bdr_peer_state_name(old_local_state),
			bdr_peer_state_name(local->bdr_node->local_state));

		bdr_modify_node(local->bdr_node);

		state_transition(&cur_state, BDR_NODE_STATE_STANDBY,
			NULL);
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

	Assert(msg->message_type == BDR_MSG_NODE_ACTIVE);

	/*
	 * Catchup ready announcements are empty, with no payload,
	 * but we might want to add one later, so we don't check.
	 */

	local = bdr_get_local_node_info(false, false);
	remote = bdr_get_node_info(msg->origin_id, false);

	if (local->bdr_node->node_id != remote->bdr_node->node_id)
	{
		/*
		 * Another node is going from standby to active.
		 *
		 * FIXME RM#1118: this is wrong, we could be in any valid state, and it's totally
		 * normal for standbys to handle this...
		 */
		state_get_expected(&cur_state, true, true, BDR_NODE_STATE_ACTIVE);

		/*
		 * If we're currently in an state where we've created slots for active
		 * peers and we're yet to be confirmed as a standby by the group, the
		 * remote peer won't have created a slot for us. But we didn't create
		 * one on it when we prepared for standby mode, because it was only
		 * another standby then.
		 *
		 * We're not a standby yet so we can allow it to promote without
		 * breaking any promises. We just have to go back to the slot creation
		 * phase and make a slot for it, then repeat the catchup phase.
		 */
		if (cur_state.current > BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS
			&& cur_state.current < BDR_NODE_STATE_STANDBY)
		{
			/* Restart entering standby at the slot creation phase */
			elog(bdr_debug_level,
				 "peer %s went active, going back to CREATE_PEER_SLOTS state",
				 remote->pgl_node->name);
			state_transition(&cur_state, BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS, NULL);
		}


		/*
		 * We can now create a subscription to the joining node, to be started
		 * once we commit.
		 */
		bdr_create_subscription(local, remote, 0, false,
			BDR_SUBSCRIPTION_MODE_NORMAL, true);
	}
	else
	{
		/*
		 * We're the joining/promoting node, so enable our subs to all peers.
		 */
		List	   *subs;
		ListCell   *lc;

		state_get_expected(&cur_state, true, true,
			BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE);

		/*
		 * Switch the catchup mode subscription we used for join to normal
		 * replay.
		 */
		subs = bdr_get_node_subscriptions(local->pgl_node->id);
		foreach (lc, subs)
		{
			BdrSubscription *sub = lfirst(lc);
			PGLogicalSubscription *psub = get_subscription(sub->pglogical_subscription_id);
			/* Subscription must point to us */
			Assert(sub->target_node_id == local->pgl_node->id);
			/* Can't have a subscription to ourselves */
			Assert(sub->origin_node_id != local->pgl_node->id);
			if (sub->origin_node_id == cur_state.peer_id)
			{
				/* It's our join target subscription */
				Assert(sub->mode == BDR_SUBSCRIPTION_MODE_CATCHUP);
				sub->mode = BDR_SUBSCRIPTION_MODE_NORMAL;
			}
			else
			{
				/* It's a subscription to another peer */
				Assert(sub->mode == BDR_SUBSCRIPTION_MODE_NORMAL);
				Assert(psub->enabled == false);
				sub->mode = BDR_SUBSCRIPTION_MODE_NORMAL;
			}
			bdr_alter_bdr_subscription(sub);
			psub->enabled = true;
			/*
			 * This will kill the subscription's workers and restart them
			 */
			alter_subscription(psub);
		}

		/* Enter fully joined steady state */
		Assert(cur_state.goal == BDR_NODE_STATE_ACTIVE);
		state_transition_goal(&cur_state, BDR_NODE_STATE_ACTIVE,
			cur_state.goal, 0, 0, NULL);
	}

	if (remote->bdr_node->local_state != BDR_PEER_STATE_STANDBY)
		elog(WARNING, "node %s requesting promotion but was not in expected state %s (was: %s)",
			 remote->pgl_node->name,
			 bdr_peer_state_name(BDR_PEER_STATE_STANDBY),
			 bdr_peer_state_name(remote->bdr_node->local_state));
	else
		elog(bdr_debug_level, "node %s state changing from %s to %s",
			 remote->pgl_node->name,
			 bdr_peer_state_name(remote->bdr_node->local_state),
			 bdr_peer_state_name(BDR_PEER_STATE_ACTIVE));

	remote->bdr_node->local_state = BDR_PEER_STATE_ACTIVE;
	bdr_modify_node(remote->bdr_node);
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

	join.query_result_pending = false;

	return remote;
}

/*
 * Synchronously probe a remote node to get BdrNodeInfo for the node.
 */
BdrNodeInfo *
get_remote_node_info(PGconn *conn)
{
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

		state_transition(cur_state, BDR_NODE_STATE_JOIN_CREATE_TARGET_SLOT,
			NULL);
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
			&extra);

		/*
		 * We know there's only one result set, so this really shouldn't
		 * block for long. It's a bit naughty to check it here without
		 * testing if we'll block first, but it really should be safe...
		 */
		res = PQgetResult(join.conn);
		Assert(res == NULL);

		join.query_result_pending = false;
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
	 * If we used subscription names as part of the slot name in the slot
	 * created on the peer it'd be even worse. But we don't, because then
	 * we'd also have to add the subscriber name to ensure we got unique
	 * slot names, and that'd just get horrid.
	 */
	appendStringInfo(&sub_name, "%s_%s",
		provider->bdr_node_group->name,
		provider->pgl_node->name);
	return sub_name.data;
}

/*
 * Generate a slot / replication origin name for BDR.
 *
 * We don't use pglogical's name generation because it's extremely redundant
 * when used for a mesh. See comments on bdr_gen_sub_name.
 *
 * Instead we use the format:
 *
 *     bdr_subscriberdbname_nodegroupname_originname_targetname
 *
 * with the same abbreviation as used by pgl. origin = provider, target =
 * subscriber.
 *
 * The short names are unfortunate, but ... oh well.
 *
 * Each of these name components is needed.
 *
 * We need the subscriber dbname to prevent slot name collisions if someone is
 * silly enough to create multiple parallel BDR setups, with the same nodegroup
 * name + node names. Arguably avoidable with "don't be stupid", but we all
 * know how well that works in practical terms. The dbname is more aggressively
 * abbreviated since it's deemed less important.
 *
 * We need the nodegroup name in case we support multiple nodegroups in future.
 * A pair of nodes could be joined to >1 nodegroup, effecively creating
 * multiple subscriptions like pgl has.
 *
 * We need the subscriber name to prevent replication slot name conflicts
 * on the provider.
 *
 * We need the provider name to prevent replication origin name conflicts
 * on the subscriber, because pgl uses the same name for slots + origins.
 */
void
bdr_gen_slot_name(Name slot_name, const char *dbname,
			  const char *nodegroup, const char *provider_node,
			  const char *subscriber_node)
{
	memset(NameStr(*slot_name), 0, NAMEDATALEN);
	/* 63-char limit, so 56 chars of variable data */
	snprintf(NameStr(*slot_name), NAMEDATALEN,
			 "bdr_%s_%s_%s_%s",
			 shorten_hash(dbname, 11),
			 shorten_hash(nodegroup, 13),
			 shorten_hash(provider_node, 16),
			 shorten_hash(subscriber_node, 16));
	NameStr(*slot_name)[NAMEDATALEN-1] = '\0';
}

/*
 * Create a subscription, optionally initially disabled, from 'local' to 'remote, possibly
 * dumping data too.
 *
 * Creates the bdr.subscription entry and the underlying
 * pglogical.subscription, writer, etc.
 */
static void
bdr_create_subscription(BdrNodeInfo *local, BdrNodeInfo *remote,
	int apply_delay_ms, bool for_join, char initial_mode,
	bool initially_enabled)
{
	List				   *replication_sets = NIL;
	NameData				slot_name;
	char				   *sub_name;
	BdrSubscription		   *bsub_existing;
	BdrSubscription			bsub_new;
	PGLogicalSubscription	sub;
	PGLSubscriptionWriter	sub_writer;
	PGLogicalSyncStatus		sync;
	Interval				apply_delay;

	elog(bdr_debug_level, "creating subscription for %u on %u",
		 remote->bdr_node->node_id, local->bdr_node->node_id);

	bsub_existing = bdr_get_node_subscription(local->pgl_node->id,
		remote->pgl_node->id, local->bdr_node_group->id, true);

	if (bsub_existing != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bdr subscription from %u to %u for nodegroup %u already exists with id %u",
				 		local->pgl_node->id, remote->pgl_node->id,
						local->bdr_node_group->id,
						bsub_existing->pglogical_subscription_id)));

	/*
	 * For now we support only one replication set, with the same name as the
	 * BDR group. (DDL should be done through it too).
	 */
	replication_sets = lappend(replication_sets, pstrdup(local->bdr_node_group->name));

	/*
	 * Make sure there's no existing subscription to this node with the same
	 * BDR replication set.
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
	sub.enabled = initially_enabled;
	bdr_gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  local->bdr_node_group->name,
				  remote->pgl_node->name /* provider */,
				  local->pgl_node->name /* subscriber */);
	sub.slot_name = pstrdup(NameStr(slot_name));

	interval_from_ms(apply_delay_ms, &apply_delay);
	sub.apply_delay = &apply_delay;

	sub.isinternal = true;

	create_subscription(&sub);

	/*
	 * Record that this is a BDR-owned subscription
	 */
	bsub_new.pglogical_subscription_id = sub.id;
	bsub_new.nodegroup_id = local->bdr_node_group->id;
	bsub_new.origin_node_id = remote->pgl_node->id;
	bsub_new.target_node_id = local->pgl_node->id;
	bsub_new.mode = initial_mode;
	bdr_create_bdr_subscription(&bsub_new);

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

	elog(bdr_debug_level, "created subscription for provider %s on local subscriber %s with slot name %s",
		 remote->pgl_node->name, local->pgl_node->name, NameStr(slot_name));
}

static void
bdr_join_continue_subscribe_join_target(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET);

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
		bdr_create_subscription(local, remote, 0, true,
			BDR_SUBSCRIPTION_MODE_CATCHUP, true);

		/*
		 * Now that we've created a subscription to the target we can start
		 * talking to it.
		 */
		bdr_start_consensus(cur_state->current);
		bdr_consensus_refresh_nodes(cur_state->current);

		state_transition(cur_state, BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE,
			NULL);
	}
}

static void
bdr_join_continue_wait_subscribe_complete(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	PGLogicalSubscription  *sub;
	PGLogicalSyncStatus	   *sync;
	BdrSubscription		   *bsub;

	Assert(cur_state->current == BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE);

	bsub = bdr_get_node_subscription(bdr_get_local_nodeid(), cur_state->peer_id,
		bdr_get_local_nodegroup_id(false), true);


	/*
	 * Only one BDR sub should exist. We created it earlier, we're just waiting
	 * for it to sync now.
	 */
	sub = get_subscription(bsub->pglogical_subscription_id);
	Assert(sub->target->id == bdr_get_local_nodeid());
	Assert(sub->origin->id == cur_state->peer_id);

	/*
	 * Is the subscription synced up yet?
	 */
	sync = get_subscription_sync_status(sub->id, true);
	if (sync && sync->status == SYNC_STATUS_READY)
	{
		state_transition(cur_state, BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS,
			NULL);
	}

}

static void
bdr_join_continue_wait_catchup(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	XLogRecPtr cur_progress;
	ExtraDataJoinWaitCatchup *extra;
	RepOriginId origin_id;
	BdrSubscription 	   *bsub;
	PGLogicalSubscription  *sub;

	Assert(cur_state->current == BDR_NODE_STATE_JOIN_WAIT_CATCHUP);
	extra = cur_state->extra_data;

	/*
	 * We need to continue replay until our subscription to the join
	 * target overtakes the upstream's insert lsn from a point after
	 * we took the initial dump snapshot and copied nodes entries, etc.
	 */
	bsub = bdr_get_node_subscription(local->pgl_node->id,
		cur_state->peer_id, local->bdr_node_group->id, false);
	sub = get_subscription(bsub->pglogical_subscription_id);
	origin_id = replorigin_by_name(sub->slot_name, false);
	cur_progress = replorigin_get_progress(origin_id, false);
	if ( cur_progress > extra->min_catchup_lsn )
	{
		elog(LOG, "%u replayed past minimum recovery lsn %X/%08X",
			 bdr_get_local_nodeid(),
			 (uint32)(extra->min_catchup_lsn>>32), (uint32)extra->min_catchup_lsn);
		state_transition(cur_state, BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS,
			NULL);
	}
	else
		elog(bdr_debug_level, "%u waiting for origin '%s' to replay past %X/%08X; currently %X/%08X",
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
		NULL);
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

		bdr_create_subscription(local, remote, 0, false,
			BDR_SUBSCRIPTION_MODE_NORMAL, false);
	}

	bdr_consensus_refresh_nodes(cur_state->current);

	state_transition(cur_state, BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN,
		NULL);
}

static void
bdr_join_continue_send_standby_ready(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	BdrMessage			msg;
	MNConsensusStatus	status;
	ExtraDataConsensusReq *extra;

	Assert(cur_state->current == BDR_NODE_STATE_SEND_STANDBY_READY);

	extra = cur_state->extra_data;
	if (extra->executed)
		return;

	/*
	 * Local processing of this message via bdr_join_handle_catchup_proposal
	 * will transition us to BDR_NODE_STATE_STANDBY, which is how we exit
	 * this state.
	 */
	msg.message_type = BDR_MSG_NODE_STANDBY_READY;
	msg.origin_id = local->pgl_node->id;
	status = bdr_consensus_request(&msg);

	/* Leader not available, we'll have to retry. */
	if (status == MNCONSENSUS_NO_LEADER)
		return;

	/* Check if success. */
	if (status != MNCONSENSUS_EXECUTED)
		elog(ERROR, "failed to transition to STANDBY_READY state");

	extra->executed = true;
	state_transition(cur_state, BDR_NODE_STATE_SEND_STANDBY_READY, &extra);
}

static void
bdr_join_continue_standby(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_STANDBY);

	if (cur_state->goal == BDR_NODE_STATE_STANDBY)
	{
		/*
		 * We've been asked to pause in standby state, so there's no further
		 * work to do. The node will remain in standby until poked by an outside
		 * influence like a SELECT bdr.node_promote().
		 */
	}
	else if (cur_state->goal == BDR_NODE_STATE_ACTIVE)
	{
		/* Time to start up as a full member */
		state_transition(cur_state, BDR_NODE_STATE_PROMOTING, NULL);
	}
	else
	{
		elog(WARNING, "unexpected goal state %s",
			bdr_node_state_name(cur_state->goal));
	}
}

/*
 * This is just a dummy state that kicks off promotion from standby, so we only
 * need one place that knows the first state in promotion and can clearly see
 * when a promotion request was made.
 */
static void
bdr_join_continue_promote(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_PROMOTING);

	state_transition(cur_state, BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID, NULL);
}

static void
bdr_join_continue_request_global_seq_id(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID);

	/*
	 * TODO: here we should submit a global consensus request
	 * (via our join target), assigning ourselves a new global
	 * sequence ID based on what we see is free.
	 */

	elog(WARNING, "requesting global sequence ID assignment not implemented");

	state_transition(cur_state, BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID, NULL);
}

static void
bdr_join_continue_wait_global_seq_id(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID);

	elog(WARNING, "waiting for global sequence ID assignment not implemented");

	state_transition(cur_state, BDR_NODE_STATE_CREATE_LOCAL_SLOTS, NULL);
}

static void
bdr_create_remote_slot(BdrNodeInfo *local, BdrNodeInfo *remote)
{
	PGconn	   *conn;
	PGresult   *res;
	XLogRecPtr	confirmed_flush_lsn;
	NameData	slot_name;
	const char *values[2];
	bool		already_exists = false;
	const char *lsnstr;
	uint32		hi, lo;
	bool		found;
	JoinCatchupMinimum jcm;

	found = bdr_get_join_catchup_minimum(remote->pgl_node->id, &jcm, true);

	if (found && jcm.slot_min_lsn != InvalidXLogRecPtr)
	{
		elog(bdr_debug_level, "skipping creation of slot on peer \"%s\": previous creation recorded with lsn %X/%X",
			 remote->pgl_node->name, (uint32)(jcm.slot_min_lsn>>32),
			 (uint32)jcm.slot_min_lsn);
		return;
	}

	conn = bdr_join_connect_remote(local, remote->pgl_interface->dsn);

	/*
	 * The local subscription isn't created yet, so we won't look at it 
	 * for the slot name.
	 */
	bdr_gen_slot_name(&slot_name,
				  local->bdr_node->dbname,
				  local->bdr_node_group->name,
				  remote->pgl_node->name /* provider from remote PoV */,
				  local->pgl_node->name /* subscriber from remote PoV */);

	elog(bdr_debug_level, "%s creating remote replication slot \"%s\" on peer \"%s\"",
		 local->pgl_node->name, NameStr(slot_name), remote->pgl_node->name);

	values[0] = NameStr(slot_name);
	values[1] = "pglogical_output";

	/*
	 * TODO: This makes a synchronous libpq call, which is a bit naughty. We
	 * should do it asynchronously.
	 */
	res = PQexecParams(conn, "SELECT lsn FROM pg_create_logical_replication_slot($1, $2)",
					   2, NULL, values, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		if (strcmp(sqlstate, SQLSTATE_DUPLICATE_OBJECT) == 0)
		{
			elog(bdr_debug_level, "found existing slot \"%s\" on peer \"%s: %s",
				 NameStr(slot_name), remote->pgl_node->name, PQerrorMessage(conn));
			PQclear(res);
			already_exists = true;
		}
		else
		{
			const char *msg = pstrdup(PQerrorMessage(conn));
			PQclear(res);
			PQfinish(conn);
			elog(ERROR, "failed create replication slot %s on peer %s: %s",
				 NameStr(slot_name), remote->pgl_node->name, msg);
		}
	}
	else
	{
		if (PQgetisnull(res, 0, 0))
			elog(ERROR, "LSN unexpectedly null on slot creation");
		lsnstr = PQgetvalue(res, 0, 0);
		elog(bdr_debug_level, "created replication slot \"%s\" on peer \"%s\" with lsn %s",
			 NameStr(slot_name), remote->pgl_node->name, lsnstr);
	}

	Assert(lsnstr != NULL || already_exists);

	if (already_exists)
	{
		/*
		 * This can happen if we ERROR or crash after creating the slot and
		 * before local COMMIT. 
		 */
		res = PQexecParams(conn, "SELECT confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND plugin = $2",
						   2, NULL, values, NULL, NULL, 0);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			const char *msg = pstrdup(PQerrorMessage(conn));
			PQclear(res);
			PQfinish(conn);
			elog(ERROR, "failed to fetch confirmed_flush_lsn for slot %s on peer %s: %s",
				 NameStr(slot_name), remote->pgl_node->name, msg);
		}

		if (PQntuples(res) != 1)
		{
			int ntuples = PQntuples(res);
			PQclear(res);
			PQfinish(conn);
			elog(ERROR, "failed to fetch confirmed_flush_lsn for slot %s on peer %s: expected 1 tuple, found %d",
				 NameStr(slot_name), remote->pgl_node->name, ntuples);
		}

		if (PQgetisnull(res, 0, 0))
		{
			PQclear(res);
			PQfinish(conn);
			elog(ERROR, "failed to fetch confirmed_flush_lsn for slot %s on peer %s: result is null",
				 NameStr(slot_name), remote->pgl_node->name);
		}

		lsnstr = PQgetvalue(res, 0, 0);
	}

	Assert(lsnstr != NULL);

	if (sscanf(lsnstr, "%X/%X", &hi, &lo) != 2)
	{
		PQfinish(conn);
		elog(ERROR, "failed to parse %s as LSN", lsnstr);
	}

	PQclear(res);

	confirmed_flush_lsn = (((XLogRecPtr)hi) << 32) | lo;

	elog(bdr_debug_level, "created/found replication slot \"%s\" on peer \"%s\" at lsn %X/%X",
		 NameStr(slot_name), remote->pgl_node->name,
		 (uint32)(confirmed_flush_lsn>>32), (uint32)confirmed_flush_lsn);

	/*
	 * Record the position we must pass in catchup mode before we can exit
	 * catchup and switch to direct replay from this slot.
	 *
	 * The join-target is inherently caught-up to, so we don't bother keeping
	 * track of it.
	 */
	if (remote->pgl_node->id != join.target->pgl_node->id)
	{
		jcm.node_id = remote->pgl_node->id;
		jcm.slot_min_lsn = confirmed_flush_lsn;
		jcm.passed_slot_min_lsn = false;
		bdr_upsert_join_catchup_minimum(&jcm);
	}

	PQfinish(conn);
}

/*
 * Create a replication slot for our use on the join target node, via
 * walsender protocol and libpq. See bdr_create_local_slot() for why
 * we don't do it as part of the join request consensus message.
 */
static void
bdr_join_continue_create_target_slot(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	Assert(cur_state->current == BDR_NODE_STATE_JOIN_CREATE_TARGET_SLOT);

	/* TODO: it's a bit naughty to create a slot synchronously here */
	elog(bdr_debug_level, "%s remotely creating slot on join target %s",
		 local->pgl_node->name, join.target->pgl_node->name);
		 
	bdr_create_remote_slot(local, join.target);

	state_transition(cur_state, BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET, NULL);
}

/*
 * Create replication slots on all remaining active peer nodes, using
 * walsender protocol and libpq. See bdr_create_local_slot() for why
 * we don't do it as part of the join request consensus message.
 *
 * We need these slots created on all active nodes before we enter
 * standby mode, so we know we can switch out of catchup mode and
 * have everything we need to remain a consistent member of the
 * cluster.
 */
static void
bdr_join_continue_create_peer_slots(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	List		   *nodes;
	ListCell	   *lc;

	Assert(cur_state->current == BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS);

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);

	foreach (lc, nodes)
	{
		BdrNodeInfo *remote = lfirst(lc);

		if (remote->bdr_node->node_id == local->bdr_node->node_id)
			/* We don't need a loopback slot on our own node */
			continue;

		if (remote->bdr_node->node_id == cur_state->peer_id)
			/* We created the join target slot earlier */
			continue;

		/*
		 * We must create slots on all active peers before we can safely enter
		 * standby mode. Otherwise if we were promoted we'd be unable to replay
		 * from the peer. Making a slot at promotion time isn't good enough
		 * since there could be a gap between what we last replayed in catchup
		 * and the minimum we can replay from a new slot... and we might tbe
		 * being promoted because the join target node (our catchup upstream)
		 * crashed.
		 *
		 * We don't have to make slots on other standbys. They'll make slots
		 * for all standbys and active nodes when they promote. If going active
		 * races with another node concurrently going into standby, we handle
		 * that with consensus message ordering.
		 *
		 * TODO: this can ERROR, and we don't want to discard knowledge of each
		 * slot we create if the next one ERROR's. But we'd also like to keep our
		 * state entry lock. We can use a subproc, or just loop reacquiring the
		 * state lock. If we commit and loop we need to use a different memory
		 * context for the nodes list so we don't clobber it.
		 */
		if (remote->bdr_node->local_state == BDR_PEER_STATE_ACTIVE)
		{
			/*
			 * TODO: it's a bit naughty doing a synchronous libpq call here.
			 * We should really do it asynchronously so we don't hold up
			 * the event loop.
			 */
			elog(bdr_debug_level, "%s remotely creating slot on peer %s in state %s before going active",
				 local->pgl_node->name, remote->pgl_node->name,
				 bdr_peer_state_name(remote->bdr_node->local_state));
			bdr_create_remote_slot(local, remote);
		}
		else
		{
			elog(bdr_debug_level, "%s not remotely creating slot on peer %s before going active: peer is %s",
				 local->pgl_node->name, remote->pgl_node->name,
				 bdr_peer_state_name(remote->bdr_node->local_state));
		}
	}

	state_transition(cur_state, BDR_NODE_STATE_JOIN_WAIT_STANDBY_REPLAY, NULL);
}

static void
bdr_join_request_replay_progress_update(void)
{
	TimestampTz now = GetCurrentTimestamp();

	if (TimestampDifferenceExceeds(join.last_replay_progress_update,
		now, CATCHUP_REPLAY_PROGRESS_UPDATE_FREQUENCY_MS))
	{
		int ret;

		Assert(!join.query_result_pending);

		ret = PQsendQuery(join.conn,
			"SELECT bdr.request_replay_progress_update()");

		if (!ret)
		{
			ereport(WARNING,
					(errmsg("failed to submit request for replay progress update - couldn't send query"),
					 errdetail("libpq: %s", PQerrorMessage(join.conn))));
			bdr_join_reset_connection();
		}

		join.query_result_pending = true;

		join.last_replay_progress_update = now;
	}
}

/*
 * Wait for catchup replay to pass the minmimum startpoint for all replication
 * slots on all peers, i.e. their creation-time confirmed_flush_lsn.
 *
 * If we don't wait for this before we announce that we're a standby, we're
 * making false promises. If we were promoted we could have a gap between the
 * current position on the peer that we've replayed 2nd-hand via our join
 * target, and the minimum start position on the slot that peer has for us.
 * We'd have no way to replay that data unless we returned to catchup
 * mode.
 *
 * So wait until all local origins pass the creation LSNs of slots on all
 * peers.
 *
 * Because some peers might be idle and not generating rows we'll see via
 * catchup, their origins won't advance locally. We aren't yet connected to the
 * peer yet, our own subscriptions aren't active. But our join target is
 * replaying from them directly so its origins get advanced due to keepalives,
 * empty transactions, etc. We can ask our join target to tell us its own
 * replay positions, and because we clone everything from the join target we
 * can trust that we also have the same local data. So we'll periodically
 * poke the join target for a position update.
 */
static void
bdr_join_continue_wait_standby_replay(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	ListCell   *lc;
	List	   *nodes;

	bool catchup_complete = true;
	Assert(cur_state->current == BDR_NODE_STATE_JOIN_WAIT_STANDBY_REPLAY);

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);

	/*
	 * Is there an in-flight replay progress update request we need to clear
	 * off the libpq connection?
	 */
	if (check_for_query_result())
	{
		PGresult *res;

		res = PQgetResult(join.conn);
		Assert(res != NULL);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			const char *msg = pstrdup(PQresultErrorMessage(res));
			ereport(ERROR,
					(errmsg("failed to request replay progress update from peer"),
					 errdetail("libpq: %s: %s", PQresStatus(PQresultStatus(res)), msg)));
		}

		PQclear(res);

		res = PQgetResult(join.conn);
		Assert(res == NULL);

		join.query_result_pending = false;
	}

	foreach (lc, nodes)
	{
		BdrNodeInfo			   *remote = lfirst(lc);
		XLogRecPtr				local_origin_lsn;
		RepOriginId				origin_id;
		BdrSubscription 	   *bsub;
		PGLogicalSubscription  *sub;
		JoinCatchupMinimum		jcm;

		if (remote->bdr_node->node_id == local->bdr_node->node_id)
			/* We know we're caught up with ourselves */
			continue;

		if (remote->bdr_node->node_id == cur_state->peer_id)
			/* This is the catchup peer, we're inherently up to date */
			continue;

		if (bdr_get_join_catchup_minimum(remote->bdr_node->node_id,
			&jcm, true) && jcm.passed_slot_min_lsn)
			/* No catchup required for this peer or already caught up */
			continue;

		bsub = bdr_get_node_subscription(local->pgl_node->id,
			remote->pgl_node->id, local->bdr_node_group->id, false);
		sub = get_subscription(bsub->pglogical_subscription_id);
		Assert(sub->target->id == bdr_get_local_nodeid());

		if (jcm.slot_min_lsn == InvalidXLogRecPtr)
		{
			/*
			 * Because of possible crashes etc, it's possible we could've created slots on
			 * a peer during BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS but not recorded the minimum
			 * LSN at which the slot becomes valid. We must look up such entries
			 * in the peer's pg_replication_slots and update our local state, so we know
			 * when local replay has passed that point and it's safe to enter standby
			 * or promote to active.
			 *
			 * To deal with this we just bail back to slot creation.
			 */
			state_transition(cur_state, BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS,
							 NULL);
			return;
		}

		origin_id = replorigin_by_name(sub->slot_name, false);
		Assert(origin_id != InvalidRepOriginId);

		local_origin_lsn = replorigin_get_progress(origin_id, false);

		if (local_origin_lsn == InvalidXLogRecPtr
			|| local_origin_lsn < jcm.slot_min_lsn)
			catchup_complete = false;

		elog(bdr_debug_level,
			 "%s past %X/%X for peer %s; currently at %X/%X",
			 catchup_complete ? "caught up" : "still waiting to catch up",
			 (uint32)(jcm.slot_min_lsn >> 32),
			 (uint32)jcm.slot_min_lsn,
			 remote->pgl_node->name,
			 (uint32)(local_origin_lsn >> 32),
			 (uint32)local_origin_lsn);

		if (!catchup_complete)
			break;

		/* Don't keep re-checking the catchup lsn now we've passed it */
		jcm.passed_slot_min_lsn = true;
		bdr_upsert_join_catchup_minimum(&jcm);
	}

	/* Ready to tell the world we're a standby */
	if (catchup_complete)
	{
		ExtraDataConsensusReq extra;

		extra.executed = false;
		state_transition(cur_state, BDR_NODE_STATE_SEND_STANDBY_READY, &extra);
	}
	else
	{
		/*
		 * Do we need to ask for a new replay progress update from our
		 * join target?
		 */
		bdr_join_request_replay_progress_update();
	}
}

/*
 * Before we can call ourselves active and (once supported) enable writes on this
 * node, we must ensure all active peers have replication slots on this node.
 *
 * The peer doesn't need to know about the slot, we just have to have a
 * startpoint for replication with a valid restart_lsn, catalog_xmin and
 * confirmed_flush_lsn locked in.
 *
 * There aren't the same issues that apply to slot creation in response to
 * global consensus messages here, since we're just in a local state handler
 * that won't have an existing write xact.  We can create slots on the peers'
 * behalf, on our end. This means we don't have to do any dancing about with
 * asking the peers to create slots on us when we announce our intended
 * promotion, then wait for them to create them before actually promoting.
 *
 * (See detailed comment in bdr_create_local_slot for details of issues around
 * slots and transactions)
 */
static void
bdr_join_continue_create_local_slots(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	List	   *nodes;
	ListCell   *lc;

	Assert(cur_state->current == BDR_NODE_STATE_CREATE_LOCAL_SLOTS);

	/* Cannot have a write xid due to logical decoding limits */
	Assert(GetCurrentTransactionIdIfAny() == InvalidTransactionId);

	nodes = bdr_get_nodes_info(local->bdr_node_group->id);

	foreach (lc, nodes)
	{
		BdrNodeInfo *remote = lfirst(lc);

		if (remote->bdr_node->node_id == local->bdr_node->node_id)
			continue;

		/*
		 * We must create a slot for any standby or active peer before we start
		 * doing writes, so they can safely replay from us.
		 *
		 * Peers not yet in standby will create their own slots on existing
		 * active nodes when they enter standby.
		 *
		 * There's a race between slot creation on the standby and another node
		 * going active. If some peer has finished creating slots on active
		 * nodes but hasn't yet announced its self as a standby, when we
		 * announce ourselves as active, that peer won't have a slot for us
		 * yet. But we can handle that by having the "active announce" message
		 * handler detect a node in that state and bump it back to the
		 * slot-create state.
		 */
		if (remote->bdr_node->local_state == BDR_PEER_STATE_STANDBY
		    || remote->bdr_node->local_state == BDR_PEER_STATE_ACTIVE)
		{
			elog(bdr_debug_level, "%s creating local slot for peer %s in state %s",
				 local->pgl_node->name, remote->pgl_node->name,
				 bdr_peer_state_name(remote->bdr_node->local_state));
			bdr_create_local_slot(local, remote);
		}
		else
		{
			elog(bdr_debug_level, "%s not creating local slot for peer %s: peer state %s",
				 local->pgl_node->name, remote->pgl_node->name,
				 bdr_peer_state_name(remote->bdr_node->local_state));
		}
	}

	{
		ExtraDataConsensusReq extra;

		extra.executed = false;
		state_transition(cur_state, BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE, &extra);
	}
}

static void
bdr_join_continue_send_active_announce(BdrStateEntry *cur_state, BdrNodeInfo *local)
{
	BdrMessage	msg;
	MNConsensusStatus	status;
	ExtraDataConsensusReq *extra;

	Assert(cur_state->current == BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE);

	extra = cur_state->extra_data;
	if (extra->executed)
		return;

	bdr_consensus_refresh_nodes(cur_state->current);

	msg.message_type = BDR_MSG_NODE_ACTIVE;
	msg.origin_id = local->pgl_node->id;
	status = bdr_consensus_request(&msg);

	/* Leader not available, we'll have to retry. */
	if (status == MNCONSENSUS_NO_LEADER)
		return;

	/* Check if success. */
	if (status != MNCONSENSUS_EXECUTED)
		elog(ERROR, "failed to transition to NODE_ACTIVE state");

	/* TODO: wait for consensus in an extra state phase here */
	elog(WARNING, "not waiting for consensus on active announce, not implemented");

	extra->executed = true;
	state_transition(cur_state, BDR_NODE_STATE_SEND_ACTIVE_ANNOUNCE, &extra);
}

/*
 * Create a logical replication slot on the local PostgreSQL instance, for
 * 'remote' to use to connect to node 'local' and stream changes from
 * 'local'.
 *
 * It's as if 'remote' connected to us and did a CREATE_REPLICATION_SLOT LOGICAL ...
 *
 * If the slot already exists, no action is taken. That's because slot
 * creation is NOT transactional. If we're being asked to create a slot for
 * peers we could've failed after some slots were already created, so we can't
 * assume a clean slate. However, because we create slots as emphemeral then
 * persist them on success, we know we'll have either no slot or a good slot,
 * we won't have a broken and unusable slot.
 *
 * An already-existing pglogical slot for this db with the right name
 * is fine to use, since it must be at or behind the position a new
 * slot would get created at.
 *
 * Note that there are some significant limitations that affect slot creation,
 * meaning you can't use this in all contexts:
 *
 * - Replication slots cannot be created in a transaction that has done writes.
 *   Like, say a consensus message prepare handler.
 *
 * - Replication slots cannot finish creation while a concurrent write xact
 *   that was started before slot creation is still running.  Like, say, in
 *   another backend while a consensus message handler waits for the slot
 *   creation to finish.
 *
 * - Replication slot creation isn't transactional. If we ROLLBACK, a persistent
 *   slot isn't removed.
 *
 * - Similarly, ephemeral slots cannot be reliably made into persistent slots on
 *   commit if we're doing 2PC. If we crash after PREPARE TRANSACTION but before
 *   COMMIT PREPARED, then in recovery the ephemeral slot will be gone but the
 *   prepared xact will still be present.
 *
 * So you can't use this from all contexts - and workarounds like using a
 * bgworker to run the slot creation won't help.
 *
 * (See: RM#1105 and RM#837)
 */
static void
bdr_create_local_slot(BdrNodeInfo *local, BdrNodeInfo *remote)
{
	NameData	slot_name;

	bdr_gen_slot_name(&slot_name,
				  remote->bdr_node->dbname,
				  local->bdr_node_group->name,
				  local->pgl_node->name /* provider */,
				  remote->pgl_node->name /* subscriber */);

	elog(bdr_debug_level, "creating local slot \"%s\" for peer %s",
		 NameStr(slot_name), remote->pgl_node->name);
	pgl_acquire_or_create_slot(NameStr(slot_name), false);
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

struct bdr_join_errcontext {
	BdrNodeState cur_state;
	BdrStateEntry *state_entry;
};

static void
join_errcontext_callback(void *arg)
{
	struct bdr_join_errcontext *ctx = arg;	

	if (ctx->state_entry != NULL)
		errcontext("while executing join state handler for %s (%s) for goal %s",
				   bdr_node_state_name(ctx->state_entry->current),
				   ctx->state_entry->extra_data == NULL
					? "" : state_stringify_extradata(ctx->state_entry),
				   bdr_node_state_name(ctx->state_entry->goal)
				   );
	else
		errcontext("while early in state join handler for %s",
				   bdr_node_state_name(ctx->cur_state));
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
	ErrorContextCallback myerrcontext;
	struct bdr_join_errcontext ctxinfo;
	BdrStateEntry locked_state;
	BdrNodeInfo *local;

	ctxinfo.cur_state = cur_state;
	ctxinfo.state_entry = NULL;
	myerrcontext.callback = join_errcontext_callback;
	myerrcontext.arg = &ctxinfo;
	myerrcontext.previous = error_context_stack;
	error_context_stack = &myerrcontext;

	maintain_join_context();
 	local = bdr_get_cached_local_node_info();

	StartTransactionCommand();
	/* Lock the state and decode extradata */
	state_get_expected(&locked_state, true, true, cur_state);
	ctxinfo.state_entry = &locked_state;

	if (bdr_join_maintain_conn(local, locked_state.peer_id))
	{
		switch (locked_state.current)
		{
			case BDR_NODE_STATE_JOIN_START:
				bdr_join_continue_join_start(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_WAIT_CONFIRM:
				elog(ERROR, "unexpected state");
				break;

			case BDR_NODE_STATE_JOIN_COPY_REMOTE_NODES:
				bdr_join_continue_copy_remote_nodes(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_CREATE_TARGET_SLOT:
				bdr_join_continue_create_target_slot(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_SUBSCRIBE_JOIN_TARGET:
				bdr_join_continue_subscribe_join_target(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_WAIT_SUBSCRIBE_COMPLETE:
				bdr_join_continue_wait_subscribe_complete(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_COPY_REPSET_MEMBERSHIPS:
				bdr_join_continue_copy_repset_memberships(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS:
				bdr_join_continue_create_subscriptions(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_GET_CATCHUP_LSN:
				bdr_join_continue_get_catchup_lsn(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_WAIT_CATCHUP:
				bdr_join_continue_wait_catchup(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_CREATE_PEER_SLOTS:
				/* create remote slots for outbound conns to active peers */
				bdr_join_continue_create_peer_slots(&locked_state, local);
				break;

			case BDR_NODE_STATE_JOIN_WAIT_STANDBY_REPLAY:
				/* wait for catchup to replay past all active peer slot start points */
				bdr_join_continue_wait_standby_replay(&locked_state, local);
				break;

			case BDR_NODE_STATE_SEND_STANDBY_READY:
				bdr_join_continue_send_standby_ready(&locked_state, local);
				break;

			case BDR_NODE_STATE_STANDBY:
				bdr_join_continue_standby(&locked_state, local);
				break;

			case BDR_NODE_STATE_PROMOTING:
				bdr_join_continue_promote(&locked_state, local);
				break;

			case BDR_NODE_STATE_REQUEST_GLOBAL_SEQ_ID:
				bdr_join_continue_request_global_seq_id(&locked_state, local);
				break;

			case BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID:
				bdr_join_continue_wait_global_seq_id(&locked_state, local);
				break;

			case BDR_NODE_STATE_CREATE_LOCAL_SLOTS:
				/* local slots for inbound conns from all standbys and active peers */
				bdr_join_continue_create_local_slots(&locked_state, local);
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
		CommitTransactionCommand();

	/*
	 * HACK HACK HACK
	 * TODO
	 *
	 * Should only be set for the few areas where we must poll.
	 */
	*max_next_wait_msecs = 1000;

	error_context_stack = myerrcontext.previous;
}
