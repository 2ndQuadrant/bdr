#include "postgres.h"

#include "fmgr.h"

#include "miscadmin.h"

#include "lib/stringinfo.h"

#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"

#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "mn_msgbroker_receive.h"
#include "mn_msgbroker.h"
#include "shm_mq_pool.h"

/* TODO: remove BDR dependency. */
#include "bdr_catcache.h"
#include "bdr_worker.h"

/* Broker state pertaining to each peer, */
typedef struct MsgbReceivePeer
{
	uint32			sender_id;
	uint64			max_received_msgid;
} MsgbReceivePeer;

static HTAB *MsgbReceivePeers = NULL;

static void msgb_receiver_message_cb(MQPoolConn *mqconn, void *data, Size len);

/* Pool in the receiver server proccess. */
static MQPool	   *MyMQPool = NULL;

/* Connection to pool in backend. */
static MQPoolConn  *MyMQConn = NULL;

static uint32		MyNodeId = 0;

static uint32		connected_peer_id = 0;

static msgb_receive_cb		msgb_received_callback = NULL;

/*
 * In a normal user backend, attach to the manager's shmem queue for
 * the connecting peer and prepare to send messages.
 *
 * The peer must tell us the last message id it knows it delivered, or 0 if
 * none delivered yet, so we can handle message id sequences changes if
 * the peer restarts.
 */
PG_FUNCTION_INFO_V1(msgb_connect);

Datum
msgb_connect(PG_FUNCTION_ARGS)
{
	char		   *channel = text_to_cstring(PG_GETARG_TEXT_P(0));
	uint32			origin_node = PG_GETARG_UINT32(1);
	uint32			destination_node = PG_GETARG_UINT32(2);
	uint64			last_sent_msgid = PG_GETARG_INT64(3);
	MQPool		   *mqpool;
	StringInfoData	msg;
	const uint64    message_id = 0;
	char			pool_name[128];

	if (origin_node == 0 || destination_node == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg_internal("peer node sent origin node or destinationnode with id 0")));

	bdr_ensure_active_db();

	bdr_cache_local_nodeinfo();

	if (destination_node != bdr_get_local_nodeid())
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("peer %u expected to connect to our node with id %u but we are node %u",
				 		origin_node, destination_node, bdr_get_local_nodeid())));

	Assert(MQPoolerCtx != NULL);

	snprintf(pool_name, sizeof(pool_name), "%s%d", channel, MyDatabaseId);
	mqpool = shm_mq_pool_get_pool(pool_name);
	if (!mqpool)
		ereport(ERROR, (errmsg("could not get pool for channel \"%s\"",
							   channel)));
	MyMQConn = shm_mq_pool_get_connection(mqpool, false);
	if (!MyMQConn)
		ereport(ERROR, (errmsg("could not get connection for channel \"%s\"",
							   channel)));

	connected_peer_id = origin_node;

	/* Send the connection message to broker. */
	initStringInfo(&msg);
	appendBinaryStringInfo(&msg, (char *) &connected_peer_id, sizeof(uint32));
	appendBinaryStringInfo(&msg, (char *) &message_id, sizeof(uint64));
	appendBinaryStringInfo(&msg, (char *) &last_sent_msgid, sizeof(uint64));
	if (!shm_mq_pool_write(MyMQConn, &msg))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("message broker for node %u appears to have exited",
						bdr_get_local_nodeid())));

	PG_RETURN_VOID();
}

/*
 * SQL-callable to deliver a message to the message broker on the receiving
 * side. This is the normal-backend half of receive processing.
 *
 * Uses a message buffer connection already established by msgb_connect
 * to deliver the payload to the manager.
 */
PG_FUNCTION_INFO_V1(msgb_deliver_message);

Datum
msgb_deliver_message(PG_FUNCTION_ARGS)
{
	uint32			destination_id = PG_GETARG_UINT32(0);
	uint64			message_id = PG_GETARG_INT64(1);
	bytea		   *payload = PG_GETARG_BYTEA_PP(2);
	StringInfoData	msg;

	if (!MyMQConn)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("not connected, use %s.msgb_connect first", MSGB_SCHEMA)));

	bdr_ensure_active_db();

	bdr_cache_local_nodeinfo();

	if (destination_id != bdr_get_local_nodeid())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("message forwarding not supported yet, dest was %u sender %u",
				 				 destination_id, connected_peer_id)));

	Assert(connected_peer_id != 0);

	/*
	 * We're connected to the queue, so we can just deliver the message,
	 * fire-and-forget.
	 *
	 * If the broker already saw it, it'll filter it out after popping it
	 * from the queue.
	 */
	initStringInfo(&msg);
	appendBinaryStringInfo(&msg, (char *) &connected_peer_id, sizeof(uint32));
	appendBinaryStringInfo(&msg, (char *) &message_id, sizeof(uint64));
	appendBinaryStringInfo(&msg, VARDATA_ANY(payload), VARSIZE_ANY(payload));

	if (!shm_mq_pool_write(MyMQConn, &msg))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("message broker for node %u appears to have exited",
						bdr_get_local_nodeid())));

	/*
	 * TODO: it'd be good to get confirmation from the manager that the message
	 * has been processed.
	 */

	PG_RETURN_VOID();
}

/*
 * Start up the dynamic shared memory and shmem memory queues needed to
 * communicate with the normal backends that will deliver messages to us.
 *
 * ./src/test/modules/test_shm_mq should be informative when reading
 * this.
 *
 * Unlike parallel query or test_shm_mq, we don't need in- and out-queues,
 * we'll be pushing messages onto the queue and waiting until we see they have
 * been read. We don't need any replies. So we only require one queue per
 * user backend, which is one per max_connections.
 */
void
msgb_startup_receive(char *channel, uint32 local_node_id, Size recv_queue_size,
					 msgb_receive_cb receive_cb)
{
	char	pool_name[128];
	Assert(receive_cb);

	if (MyNodeId != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("only one msgbroker receiver can be active per process")));

	msgb_received_callback = receive_cb;
	MyNodeId = local_node_id;

	/* Create the pool. */
	snprintf(pool_name, sizeof(pool_name), "%s%d", channel, MyDatabaseId);
	MyMQPool = shm_mq_pooler_new_pool(pool_name, 100, recv_queue_size,
									  NULL, NULL, msgb_receiver_message_cb);
}

void
msgb_shutdown_receive(void)
{
	/* TODO */
}

static MsgbReceivePeer*
msgb_get_peer(uint32 peer_id)
{
	bool		found;
	MemoryContext old_ctxt;
	MsgbReceivePeer *hentry;

	if (MsgbReceivePeers == NULL)
	{
		HASHCTL		ctl;
		int			hash_flags;

		/* Make a new hash table for the cache */
		hash_flags = HASH_ELEM | HASH_CONTEXT;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint32);
		ctl.entrysize = sizeof(MsgbReceivePeer);
		ctl.hcxt = TopMemoryContext;

#if PG_VERSION_NUM >= 90500
		hash_flags |= HASH_BLOBS;
#else
		ctl.hash = uint32_hash;
		hash_flags |= HASH_FUNCTION;
#endif

		old_ctxt = MemoryContextSwitchTo(TopMemoryContext);
		MsgbReceivePeers = hash_create("mn_msg_broker connected peers",
									   10, &ctl, hash_flags);
		(void) MemoryContextSwitchTo(old_ctxt);

		Assert(MsgbReceivePeers != NULL);
	}

	old_ctxt = MemoryContextSwitchTo(TopMemoryContext);
	hentry = (MsgbReceivePeer *) hash_search(MsgbReceivePeers,
											 (void *) &peer_id,
											 HASH_ENTER, &found);
	(void) MemoryContextSwitchTo(old_ctxt);

	if (!found)
	{
		hentry->sender_id = peer_id;
		hentry->max_received_msgid = 0;
	}

	return hentry;
}

/*
 * Peer has (re)connected and sent us, via msgb_report_peer_connect,
 * its idea of the last sent message id. If it's 0, the peer has
 * restarted.
 */
static void
msgb_handle_peer_connect(MsgbReceivePeer *peer, const char *msg, Size len)
{
	uint64		last_sent_msgid;

	if (len != sizeof(uint64))
	{
		elog(WARNING, "reconnect message size was %zu, expected %zu; ignoring",
			 len, sizeof(uint64));
		return;
	}

	memcpy(&last_sent_msgid, msg, sizeof(uint64));

	if (peer->max_received_msgid >= last_sent_msgid)
	{
		/*
		 * We should only see the seen message-id go backwards if it's reset to
		 * zero for a peer restart, or if we received a message but the peer
		 * didn't receive the function result. In the latter case we don't
		 * want to redeliver locally.
		 */
		if (last_sent_msgid == 0)
		{
			peer->max_received_msgid = 0;
			ereport(DEBUG1,
					(errmsg("peer %u reconnected to %u with reset message id after restart; was "UINT64_FORMAT" now 0",
							peer->sender_id, MyNodeId, peer->max_received_msgid)));
		}
		else
			ereport(DEBUG1,
					(errmsg("peer %u reconnected to %u with last msgid "UINT64_FORMAT" but local "UINT64_FORMAT" is greater; ignoring",
					 		peer->sender_id, MyNodeId, last_sent_msgid, peer->max_received_msgid)));
	}

}

/*
 * We've fully received a message over the shm mq and can deliver it to the
 * waiting callback then discard it.
 */
static void
msgb_receiver_message_cb(MQPoolConn *mqconn, void *data, Size len)
{
	char	   *buf = ((char*) data);
	Size		bufsize = len;
	uint64		msgid;
	uint32		sender_id;
	MsgbReceivePeer *peer;

	if (len < sizeof(uint32) * 2)
		elog(ERROR, "unexpected message length %zu", len);

	memcpy(&sender_id, buf, sizeof(uint32));
	buf += sizeof(uint32);
	bufsize -= sizeof(uint32);
	memcpy(&msgid, buf, sizeof(uint64));
	buf += sizeof(uint64);
	bufsize -= sizeof(uint64);

	peer = msgb_get_peer(sender_id);

	if (msgid == 0)
	{
		/*
		 * First message after peer connect, generated internally, is a report
		 * of the current peer message id generator position.
		 */
		msgb_handle_peer_connect(peer, buf, bufsize);
	}
	else
	{
		if (msgid <= peer->max_received_msgid)
			ereport(DEBUG1,
					(errmsg_internal("discarding already processed message id "UINT64_FORMAT" from %u on %u; seen up to "UINT64_FORMAT,
									msgid, peer->sender_id, MyNodeId, peer->max_received_msgid)));

		if (msgid != peer->max_received_msgid + 1)
			ereport(WARNING,
					(errmsg_internal("peer %u sending to %u appears to have skipped from msgid "UINT64_FORMAT" to "UINT64_FORMAT,
									 peer->sender_id, MyNodeId, peer->max_received_msgid, msgid)));

		msgb_received_callback(sender_id, buf, bufsize);

		peer->max_received_msgid = msgid;
	}
}

/*
 * Service the shmem queues, cleaning up after exited peers,
 * consuming pending messages, etc.
 */
void
msgb_wakeup_receive(void)
{
	shm_mq_pooler_work();
}
