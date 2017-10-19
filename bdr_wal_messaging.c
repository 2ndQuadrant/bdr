#include "postgres.h"

#include "access/xlogdefs.h"
#include "access/xact.h"

#include "miscadmin.h"

#include "libpq/pqformat.h"

#include "replication/origin.h"

#include "utils/timestamp.h"

#include "pglogical_messaging.h"

#include "bdr_catcache.h"
#include "bdr_catalogs.h"
#include "bdr_state.h"
#include "bdr_wal_messaging.h"
#include "bdr_worker.h"

#define BDR_WAL_MESSAGE_PREFIX "bdr"

typedef enum BdrWalMessageType {
	BDR_WAL_MSG_TYPE_NONE = '\0',
	BDR_WAL_MSG_TYPE_REPLAY_PROGRESS = 'r'
} BdrWalMessageType;

static TimestampTz last_replay_progress_update = 0;

static const int REPLAY_PROGRESS_UPDATE_FREQUENCY_MS = 5 * 60 * 1000;

/*
 * Write a replay progress update message to a WAL message, with positions of
 * all replication origins for peers. This helps our peers to keep track of WAL
 * retention for any standby slots they maintain for failover purposes, and is
 * also used by joining peers to maintain replication origins for 3rd party
 * nodes when we're the joining node.
 *
 * This doesn't strictly have to travel over WAL messages, we could send it as
 * a result-set over libpq protocol along with the current xlog insert lsn. The
 * recipient would then have to wait until it replayed our own WAL stream past
 * that LSN before the result set became valid and trustworthy. This would
 * save some WAL space, so we might want to do it later, but it's not worth
 * the complexity for now.
 *
 * (Actually, if we could get access to local_lsn, we could use the
 *  max(local_lsn) instead of the current insert lsn).
 *
 * Periodic replay progress messages are sent because we don't want want
 * to be writing WAL messages every time we advance a replication
 * origin. Unfortunately logical decoding doesn't get to "see" tuples
 * for persistent replication origin updates, so we can't do the ideal
 * thing and just decode origin advances and send them directly.
 */
void
bdr_write_replay_progress_update(void)
{
	List		   *bdrsubscriptions;
	ListCell	   *lc;
	StringInfoData	msgdata;

	Assert(IsTransactionState());

	/*
	 * We can't just scan the replication origins array unless we do so
	 * by calling the SQL function pg_show_replication_origin_status.
	 * All the required data structures are private members of origin.c.
	 *
	 * So we'll enumerate subscriptions and look up each origin. It's
	 * more than a tad inefficient, but this isn't something we're going
	 * to be doing so often that that'll be a concern. We probably want
	 * to filter the origins we send to just those used by subscriptions
	 * anyway.
	 */
	bdrsubscriptions = bdr_get_node_subscriptions(bdr_get_local_nodeid());

	initStringInfo(&msgdata);
	PGLPrepareLogicalMessage(&msgdata, InvalidOid /* broadcast */);
	Assert(msgdata.len != 0);

	/* Message type */
	pq_sendbyte(&msgdata, BDR_WAL_MSG_TYPE_REPLAY_PROGRESS);

	/* number of origins, timestamp, etc */
	pq_sendint(&msgdata, list_length(bdrsubscriptions), 4);
	pq_sendint64(&msgdata, GetCurrentTimestamp());

	/* then write out each one */
	foreach (lc, bdrsubscriptions)
	{
		BdrNode				   *bnode;
		BdrSubscription		   *bsub = lfirst(lc);
		PGLogicalSubscription  *sub;
		RepOriginId				origin;
		XLogRecPtr				origin_lsn;

		sub = get_subscription(bsub->pglogical_subscription_id);
		origin = replorigin_by_name(sub->slot_name, true);
		if (origin == InvalidRepOriginId)
			elog(ERROR, "couldn't find replication origin \"%s\" for subscription \"%s\"",
				 sub->slot_name, sub->name);
		origin_lsn = replorigin_get_progress(origin, false);

		bnode = bdr_get_node(bsub->origin_node_id, false);

		/* which peer we're sending info on */
		pq_sendint(&msgdata, bsub->origin_node_id, 4);
		/* How far we replayed from origin's wal stream */
		pq_sendint64(&msgdata, origin_lsn);
		/* Our current idea of the peer node's state */
		pq_sendint(&msgdata, (int)bnode->local_state, 4);
	}

	PGLLogLogicalMessage(BDR_WAL_MESSAGE_PREFIX, &msgdata, true);
}

/*
 * Periodically write a replay progress update to WAL, so any interested
 * peers can update their knowledge of our replay progress. 
 */
void
bdr_maybe_write_replay_progress_update(void)
{
	TimestampTz now = GetCurrentTimestamp();
	if (TimestampDifferenceExceeds(last_replay_progress_update,
		now, REPLAY_PROGRESS_UPDATE_FREQUENCY_MS))
	{
		bool txn_started = false;
		if (!IsTransactionState())
		{
			StartTransactionCommand();
			txn_started = true;
		}
		bdr_write_replay_progress_update();
		last_replay_progress_update = now;
		if (txn_started)
			CommitTransactionCommand();
	}
}

/*
 * Update our bdr.node_peer_progress with replay position data
 * on the sending peer's own peer nodes.
 *
 * We need this so we can release any data we may be retaining
 * for node-loss recovery purposes.
 *
 * If we're in join catchup and this message is from our join
 * target, we'll also need to advance our replication origin
 * for this peer if it's behind the reported position. We know
 * we've received all data up to the reported position on the
 * peer because the join target had at this upstream lsn.
 *
 * We do this so that we advance our local origins for peers who
 * aren't generating any transactions with replicated writes, so we
 * never see actual rows from them. That means we never update their
 * origins via catchup mode. If the local origins never pass the
 * initial confirmed_flush_lsn of the slots we created on each peer,
 * we can't exit catchup mode safely (see
 * BDR_NODE_STATE_JOIN_WAIT_STANDBY_REPLAY and handler for it),
 * so this is necessary to allow us to exit catchup if we have idle
 * peers.
 */
static void
bdr_handle_replay_progress_msg(StringInfo msgdata, uint32 sender_nodeid,
	XLogRecPtr sender_lsn)
{
	int				nentries, i;
	BdrStateEntry	state;
	TimestampTz		sender_timestamp;
	bool			catchup_advance_needed;
	bool			txn_started = false;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		txn_started = true;
	}

	state_get_last(&state, false, false);
	if (state.current > BDR_NODE_STATE_JOIN_CREATE_SUBSCRIPTIONS
	    && state.current <= BDR_NODE_STATE_JOIN_WAIT_STANDBY_REPLAY
		&& state.peer_id == sender_nodeid)
		catchup_advance_needed = true;

	nentries = pq_getmsgint(msgdata, 4);
	sender_timestamp = pq_getmsgint64(msgdata);

	for (i = 0; i < nentries; i++)
	{
		uint32			peer_node_id;
		XLogRecPtr		peer_node_lsn;
		BdrPeerState	peer_state;

		peer_node_id = pq_getmsgint(msgdata, 4);
		peer_node_lsn = pq_getmsgint64(msgdata);
		peer_state = (BdrPeerState) pq_getmsgint(msgdata, 4);

		elog(bdr_debug_level, "handling progress update entry %d for %u lsn %X/%X (advance: %d)",
			 i, peer_node_id, (uint32)(peer_node_lsn>>32), (uint32)peer_node_lsn,
			 catchup_advance_needed);

		/*
		 * TODO: actually update bdr.node_peer_progress
		 * with something like:
		 *
		 * bdr_node_peer_progress_update(sender_nodeid, sender_lsn,
		 *	sender_timestamp, peer_node_id, peer_lsn, peer_state);
		*/

		/*
		 * Update replication origin locally too, if we are in catchup
		 * mode.
		 */
		if (catchup_advance_needed)
		{
			BdrSubscription		   *bsub;

			bsub = bdr_get_node_subscription(bdr_get_local_nodeid(),
				peer_node_id, bdr_get_local_nodegroup_id(false), true);

			/*
			 * We might not know about this peer yet, in which case we'll
			 * update our state later when we do make a direct subscription
			 * for them.
			 */
			if (bsub != NULL)
			{
				PGLogicalSubscription  *sub;
				RepOriginId				origin;

				sub = get_subscription(bsub->pglogical_subscription_id);
				origin = replorigin_by_name(sub->slot_name, true);
				if (origin == InvalidRepOriginId)
					elog(ERROR, "couldn't find replication origin \"%s\" for subscription \"%s\"",
						 sub->slot_name, sub->name);

				elog(bdr_debug_level, "advancing replication origin for peer %u with origin %u on sub %s to %X/%X in response to replay progress update",
					 peer_node_id, origin, sub->name,
					 (uint32)(peer_node_lsn>>32), (uint32)peer_node_lsn);

				replorigin_advance(origin, peer_node_lsn, XactLastCommitEnd,
								   false, false);
			}
		}
	}

	if (msgdata->cursor != msgdata->len)
	{
		Assert(false);
		elog(WARNING, "unexpected data left in buffer: read %d bytes for %d peer entries, but received %d bytes",
			 msgdata->cursor, nentries, msgdata->len);
	}

	if (txn_started)
		CommitTransactionCommand();
}


/*
 * Invoked when the pglogical worker gets a WAL message with the "bdr"
 * prefix.
 */
static void
bdr_wal_message_receive(XLogRecPtr message_lsn, const char *prefix,
	StringInfo message, Oid sender_nodeid, void *callback_data)
{
	char msgtype;

	Assert(strcmp(prefix, BDR_WAL_MESSAGE_PREFIX) == 0);

	msgtype = pq_getmsgbyte(message);

	switch (msgtype)
	{
		case BDR_WAL_MSG_TYPE_REPLAY_PROGRESS:
			bdr_handle_replay_progress_msg(message, sender_nodeid,
				message_lsn);
			break;
	}
}

/*
 * Register to receive WAL messages processed by pglogical's apply workers.
 */
void
bdr_wal_messaging_register(void)
{
	PGLRegisterLogicalMessageCallback(bdr_wal_message_receive,
		NULL /* no callback data */,
		BDR_WAL_MESSAGE_PREFIX);
}
