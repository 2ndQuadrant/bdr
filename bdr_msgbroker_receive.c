#include "postgres.h"

#include "fmgr.h"

#include "miscadmin.h"

#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"

#include "utils/elog.h"
#include "utils/memutils.h"

#include "bdr_shmem.h"
#include "bdr_worker.h"
#include "bdr_catcache.h"
#include "bdr_msgbroker_receive.h"
#include "bdr_msgbroker.h"

/*
 * Shared-memory queues used to deliver from origin_node_id to the associated
 * segment-handle's node_id are stored in dynamic shared memory, addressed
 * by shm_toc.
 *
 * A worker accesses this via msgb_ctx by looking up the manager for the node
 * of interest by node-id in the shmem_mq_seg array and getting the handle from
 * dyn_seg, then dsm_attach'ing to it. It then queries the DSM ToC and looks
 * up TOC entry 0 to get the mapping of TOC entries to node-ids, finds the TOC
 * entry number for the node ID, and looks up the mq.
 *
 * Thankfully this doesn't have to be done often.
 *
 * We need this indirection because the ToC doesn't let us change keys
 * once assigned, but nodes can come and go. It doesn't give us a way to
 * scan the whole ToC either. (We might be better off not using shm_toc
 * at all, but then we have to do all the alignment, sizing, etc ourselves).
 */
typedef struct MsgbDSMHdr
{
	/*
	 * An array of equal size to the ToC size, with node-ids as values. The
	 * array position of a node-id is the associated ToC entry for it. Free
	 * ToC entries have zero. Allocations and scans must be done with the
	 * spinlock held.
	 */
	slock_t		mutex;
	int			node_toc_map_size;
	uint32		node_toc_map[FLEXIBLE_ARRAY_MEMBER];
} MsgbDSMHdr;

/*
 * We need a static shmem segment to allow backends delivering messages to
 * brokers to find the right broker and shmem_mq. Brokers will be found by
 * node ID.
 *
 * node_id = 0 for unused entries.
 *
 * Changes may only be made with the MsgbShmemContext lock held.
 */
typedef struct MsgbDynamicSegmentHandle
{
	uint32		node_id;
	PGPROC	   *manager;
	/* DSM seg contains MsgbDSMHdr and shmem queues */
	dsm_handle	dsm_seg_handle;
} MsgbDynamicSegmentHandle;

/*
 * The static shmem segment contains a MsgbDynamicSegment per broker.
 *
 * msgb_max_local_nodes could be sized as dynamic shmem assigned by the
 * pglogical supervisor at startup, and we'd store the handle here. So we
 * wouldn't need a limit set at DB startup. But it's good enough for now.
 */
typedef struct MsgbShmemContext
{
	LWLock					   *lock;
	int							msgb_max_local_nodes;
	MsgbDynamicSegmentHandle	shmem_mq_seg[FLEXIBLE_ARRAY_MEMBER];
} MsgbShmemContext;

/* Global handle for the static shmem segment */
static MsgbShmemContext *msgb_ctx = NULL;

/* Handle each worker looks up for its per-db segment */
static MsgbDynamicSegmentHandle *msgb_my_seg = NULL;

/* max DBs, used only in shmem startup until segment ready */
static int msgb_max_local_nodes = 0;

/* Broker state pertaining to each peer, */
typedef struct MsgbReceivePeer
{
	uint32			sender_id;
	uint32			max_received_msgid;
	bool			pending_cleanup;
	shm_mq_handle  *recvqueue;
	/* Staging area for incomplete incoming messages */
	Size			recvsize;
	void		   *recvbuf;
} MsgbReceivePeer;

/* only valid for the broker not normal backends. */
static MsgbReceivePeer *recvpeers = NULL;
static dsm_segment *broker_dsm_seg = NULL;

/*
 * State used by normal backends acting as message receiver workers.
 * Unused in the manager.
 */
static uint32 connected_peer_id = 0;
static shm_mq_handle *send_mq;
static Size msgb_recv_queue_size = 0;

static void msgb_connect_shmem(uint32 origin_node);

msgb_received_hook_type msgb_received_hook = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void msgb_mq_for_node(dsm_segment *seg, uint32 nodeid, MsgbDSMHdr **hdr, int *hdr_idx, shm_mq **mq);
static void msgb_handle_peer_connect(MsgbReceivePeer *peer, const char *msg, Size msg_len);
static void msgb_report_peer_connect(uint32 origin_id, uint32 last_sent_msgid);

inline static bool
InBrokerProcess(void)
{
	return recvpeers != NULL;
}

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
	uint32			origin_node = PG_GETARG_UINT32(0);
	uint32			destination_node = PG_GETARG_UINT32(1);
	uint32			last_sent_msgid = PG_GETARG_UINT32(2);

	Assert(!InBrokerProcess());

	if (origin_node == 0 || destination_node == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg_internal("peer node sent origin node or destinationnode with id 0")));


	bdr_ensure_active_db();

	bdr_cache_local_nodeinfo();

	if (destination_node != bdr_get_local_nodeid())
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("peer %d expected to connect to our node with id %d but we are node %d",
				 		origin_node, destination_node, bdr_get_local_nodeid())));

	Assert(msgb_ctx != NULL);

	msgb_connect_shmem(origin_node);
	msgb_report_peer_connect(origin_node, last_sent_msgid);

	PG_RETURN_VOID();
}

/*
 * On a normal backend, find and attach to the DSM segment and then the shm_mq
 * used to communicate with the broker.
 */
static void
msgb_connect_shmem(uint32 origin_node)
{
	const uint32	local_node = bdr_get_local_nodeid();
	dsm_segment	   *seg;
	shm_toc		   *toc;
	MsgbDSMHdr	   *hdr;
	int				my_node_toc_entry;
	shm_mq		   *mq;
	MemoryContext	old_ctx;
	int				i;
	PGPROC*			cur_receiver;

	Assert(!InBrokerProcess());

	/*
	 * Find the static shmem control segment for the message broker we want to
	 * talk to, so we can look up shmem memory queues and register ourselves.
	 */
	if (msgb_my_seg == NULL)
	{
		LWLockAcquire(msgb_ctx->lock, LW_SHARED);
		for (i = 0; i < msgb_ctx->msgb_max_local_nodes; i++)
		{
			if (msgb_ctx->shmem_mq_seg[i].node_id == local_node)
				msgb_my_seg = &msgb_ctx->shmem_mq_seg[i];
		}
		LWLockRelease(msgb_ctx->lock);

		if (msgb_my_seg == NULL)
		{
	 		/*
			 * TODO: sleep and retry here instead of ERRORing? We know BDR is active for this
			 * node so the message broker should come up soon.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("no receiver found for node id %u", local_node)));
		}
	}

	Assert(msgb_my_seg != NULL && msgb_my_seg->node_id == local_node);

	/*
	 * Find the shmem memory queue for our origin node and attach to it.
	 * To do this we must find the ToC entry corresponding to the node
	 * id and get the shmem_mq.
	 */
	seg = dsm_attach(msgb_my_seg->dsm_seg_handle);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("no shared memory segment found")));

	toc = shm_toc_attach(BDR_SHMEM_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	/*
	 * Using the header at ToC entry 0, find the memory queue for our node.  If
	 * there isn't one, we're not allowed to send messages to this peer as the
	 * queues are managed only via the broker end.
	 *
	 * That way random nodes can't fill up the queue and vanish and we don't have
	 * to deal with ageing-out or scaling the queue pool etc.
	 */
	hdr = shm_toc_lookup(toc, 0, false);
	my_node_toc_entry = -1;
	SpinLockAcquire(&hdr->mutex);
	for (i = 0; i < hdr->node_toc_map_size; i++)
	{
		if (hdr->node_toc_map[i] == origin_node)
			my_node_toc_entry = i;
	}
	SpinLockRelease(&hdr->mutex);

	if (my_node_toc_entry == -1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("receiving node %u does not know about sending node %u and cannot receive messages from it",
				 		local_node, origin_node)));


	mq = shm_toc_lookup(toc, my_node_toc_entry+1, false);

	/*
	 * We need to associate ourselves with the queue under the mq spinlock
	 * because shm_mq doesn't offer a test-and-set interface or an option to
	 * error if the queue is attached. It just Assert()s. So if there's another
	 * backend already connected for this peer we'd crash the server, and without
	 * the lock we'd race. Trying to prevent it in our own DSM would be
	 * unnecessarily complex.
	 */
	SpinLockAcquire(&hdr->mutex);
	cur_receiver = shm_mq_get_sender(mq);
	if (cur_receiver == NULL)
		shm_mq_set_sender(mq, MyProc);
	SpinLockRelease(&hdr->mutex);

	if (cur_receiver != NULL)
	{
		/*
		 * Someone else is attached, or used to be.
		 *
		 * TODO: what happens if this is a dead proc? Or queue detached?
		 * TODO: should we wait for master to notice and clean up queue then retry
		 * rather than ERRORing here?
		 */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("memory queue for node %u already in use",
						origin_node)));
	}

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	send_mq = bdr_shm_mq_attach(mq, seg, NULL);
	(void) MemoryContextSwitchTo(old_ctx);

	if (bdr_shm_mq_wait_for_attach(send_mq) != SHM_MQ_SUCCESS)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("message broker for node %u appears to have exited",
						bdr_get_local_nodeid())));

	/*
	 * Stay attached to the DSM segment now we're set up successfully.
	 */
	dsm_pin_mapping(seg);

	connected_peer_id = origin_node;

	elog(DEBUG1, "peer %d connected message queue", origin_node);
}

/*
 * When a peer (re)connects we must report that as the first message
 * on the message queue, so the other end knows that the message
 * counter sequence may have reset.
 */
static void
msgb_report_peer_connect(uint32 origin_id, uint32 last_sent_msgid)
{
	shm_mq_result	res;
	shm_mq_iovec	msg[2];
	const uint32	message_id = 0;

	Assert(connected_peer_id != 0);
	Assert(send_mq != NULL);

	Assert(!InBrokerProcess());

	msg[0].data = (void*)&message_id;
	msg[0].len = sizeof(uint32);
	msg[1].data = (void*)&last_sent_msgid;
	msg[1].len = sizeof(uint32);

	res = bdr_shm_mq_sendv(send_mq, msg, 2, false);
	if (res != SHM_MQ_SUCCESS)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("message broker for node %u appears to have exited",
						bdr_get_local_nodeid())));
}

/*
 * Peer has (re)connected and sent us, via msgb_report_peer_connect,
 * its idea of the last sent message id. If it's 0, the peer has
 * restarted.
 */
static void
msgb_handle_peer_connect(MsgbReceivePeer *peer, const char *msg, Size msg_len)
{
	uint32 last_sent_msgid;

	Assert(InBrokerProcess());

	if (msg_len != sizeof(uint32))
	{
		elog(WARNING, "reconnect message size was %zu, expected %zu; ignoring",
			 msg_len, sizeof(uint32));
		return;
	}

	last_sent_msgid = *((uint32*)msg); /* FIXME: alignment? */

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
			ereport(DEBUG2,
					(errmsg("peer %u reconnecting to %u with reset message id after restart; was %u now 0",
							peer->sender_id, bdr_get_local_nodeid(), peer->max_received_msgid)));
		}
		else
			ereport(DEBUG2,
					(errmsg("peer %u reconnecting to %u with last msgid %u but local %u is greater; ignoring",
					 		peer->sender_id, bdr_get_local_nodeid(), last_sent_msgid, peer->max_received_msgid)));
	}

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
	int				message_id = PG_GETARG_INT32(1);
	bytea		   *payload = PG_GETARG_BYTEA_PP(2);
	shm_mq_result	res;
	shm_mq_iovec	msg[2];

	Assert(!InBrokerProcess());

	if (connected_peer_id == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("no connected peer, use %s.msgb_connect first", MSGB_SCHEMA)));

	Assert(send_mq != NULL);

	if (destination_id != bdr_get_local_nodeid())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("message forwarding not supported yet, dest was %u sender %u",
				 				 destination_id, connected_peer_id)));

	/*
	 * We're connected to the manager via the shmem queue, so we can just
	 * deliver the message, fire-and-forget.
	 *
	 * If the broker already saw it, it'll filter it out after popping it
	 * from the queue.
	 */
	msg[0].data = (void*)&message_id;
	msg[0].len = sizeof(int);
	msg[1].data = VARDATA_ANY(payload);
	msg[1].len = VARSIZE_ANY(payload);

	res = bdr_shm_mq_sendv(send_mq, msg, 2, false);
	if (res != SHM_MQ_SUCCESS)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("message broker for node %u appears to have exited",
						bdr_get_local_nodeid())));

	/*
	 * TODO: it'd be good to get confirmation from the manager that the message
	 * has been processed, meaning we need a second mq to receive replies from.
	 * We can't just test the mq read position even if shm_mq_get_bytes_read
	 * weren't static, since the broker might exit as soon as it read the
	 * message.
	 */

	PG_RETURN_VOID();
}

static void
broker_on_detach(dsm_segment * seg, Datum arg)
{
	broker_dsm_seg = NULL;
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
msgb_startup_receive(Size recv_queue_size)
{
	shm_toc_estimator e;
	shm_toc    *toc;
	Size		hdr_size, segsize;
	int			i;
	MsgbDSMHdr *hdr;

	if (msgb_received_hook == NULL)
		ereport(ERROR, (errmsg_internal("no message receive hook is registered")));

	Assert(!InBrokerProcess());

	/*
	 * Prepare space to store the receive side handles for the queues
	 * and the information mapping them to attached nodes.
	 */
	recvpeers = MemoryContextAlloc(TopMemoryContext, sizeof(MsgbReceivePeer) * msgb_max_peers);
	memset(recvpeers, 0, sizeof(MsgbReceivePeer) * msgb_max_peers);

	Assert(InBrokerProcess()); /* well duh */

	/*
	 * Allocate an entry for the broker in the static shmem, so workers can use
	 * it to find our dynamic shmem segment containing the delivery memory
	 * queues.
	 */
	Assert(msgb_ctx != NULL);
	Assert(msgb_my_seg == NULL);
	LWLockAcquire(msgb_ctx->lock, LW_EXCLUSIVE);
	Assert(msgb_ctx->msgb_max_local_nodes >= 1);
	for (i = 0; i < msgb_ctx->msgb_max_local_nodes; i++)
	{
		if (msgb_ctx->shmem_mq_seg[i].node_id == 0)
		{
			msgb_my_seg = &msgb_ctx->shmem_mq_seg[i];
			msgb_my_seg->node_id = bdr_get_local_nodeid();
			msgb_my_seg->manager = MyProc;
			Assert(msgb_my_seg->dsm_seg_handle == 0);
			break;
		}
	}
	LWLockRelease(msgb_ctx->lock);

	if (msgb_my_seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("maximum number of message brokers exceeded"),
				 errdetail("all %d local broker slots are in use",
						   msgb_ctx->msgb_max_local_nodes)));

	hdr_size = offsetof(MsgbDSMHdr, node_toc_map) + sizeof(uint32)*msgb_max_peers;

	msgb_recv_queue_size = recv_queue_size;

	/*
	 * Prepare the ToC that maps node-ids to the associated shmem_mq.
	 *
	 * We need a header segment that maps ToC indexes to the sender
	 * node-id using that segment, since 
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, hdr_size);
	for (i = 0; i <= msgb_max_peers; ++i)
		shm_toc_estimate_chunk(&e, msgb_recv_queue_size);
	shm_toc_estimate_keys(&e, msgb_max_peers);
	segsize = shm_toc_estimate(&e);

	/* Create the shared memory segment and establish a table of contents. */
	Assert(broker_dsm_seg == NULL);
	broker_dsm_seg = dsm_create(shm_toc_estimate(&e), 0);
	toc = shm_toc_create(BDR_SHMEM_MAGIC, dsm_segment_address(broker_dsm_seg),
						 segsize);

	/* set up the header */
	hdr = shm_toc_allocate(toc, hdr_size);
	SpinLockInit(&hdr->mutex);
	hdr->node_toc_map_size = msgb_max_peers;
	Assert(hdr_size > sizeof(uint32)*msgb_max_peers);
	memset(hdr->node_toc_map, 0, sizeof(uint32)*msgb_max_peers);
	shm_toc_insert(toc, 0, hdr);

	/*
	 * Reserve space for one message queue per peer in the ToC.
	 *
	 * We don't actuallly initialise the queues until needed; we'll have to
	 * re-init them for re-use after they get released, so might as well delay
	 * initial init too.
	 */
	for (i = 0; i < msgb_max_peers; i++)
	{
		void	   *mq_off = shm_toc_allocate(toc, msgb_recv_queue_size);
		shm_toc_insert(toc, i+1, mq_off);
	}

	on_dsm_detach(broker_dsm_seg, broker_on_detach, (Datum)0);

	/*
	 * Stay attached to the DSM segment now we're set up successfully.
	 */
	dsm_pin_mapping(broker_dsm_seg);

	/* Store a handle to the DSM seg where others can find it */
	LWLockAcquire(msgb_ctx->lock, LW_EXCLUSIVE);
	Assert(msgb_my_seg->dsm_seg_handle == 0);
	msgb_my_seg->dsm_seg_handle = dsm_segment_handle(broker_dsm_seg);
	LWLockRelease(msgb_ctx->lock);
}

/*
 * Register a peer ID in the receiver and attach to a queue we expect to get
 * messages from.
 *
 * No locking required, it's broker-local and the MQ attach does its own.
 *
 * It's legal (but weird) to specify the local node as an origin; you could
 * use the msgbroker for a loopback if you wanted.
 */
void
msgb_add_receive_peer(uint32 origin_id)
{
	int				i;
	int				existing_id = -1;
	MsgbReceivePeer *p = NULL;
	shm_mq		   *mq;
	shm_toc		   *toc;
	MsgbDSMHdr	   *hdr;
	void		   *mq_addr;
	int				my_node_toc_entry, first_free_toc_entry;
	MemoryContext	old_ctx;

	Assert(broker_dsm_seg != NULL);
	Assert(InBrokerProcess());

	/* Find local state space for the peer */
	for (i = 0; i < msgb_max_peers; i++)
	{
		if (recvpeers[i].sender_id == origin_id)
		{
			existing_id = i;
			break;
		}
		else if (recvpeers[i].sender_id == 0 && p == NULL)
			p = &recvpeers[i];
	}

	if (existing_id != -1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("peer node %u is already registered with the broker",
				 		origin_id)));
	
	if (p == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("not enough free node slots to receive from peer %u",
				 		origin_id)));

	/* Add this node to the header ToC entry and allocate a MQ for it. */
	toc = shm_toc_attach(BDR_SHMEM_MAGIC, dsm_segment_address(broker_dsm_seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	/*
	 * Get shared state header and scan it to find and claim an entry.
	 *
	 * While we're at it make sure this node isn't already registered.
	 */
	hdr = shm_toc_lookup(toc, 0, false);
	my_node_toc_entry = -1;
	first_free_toc_entry = -1;
	SpinLockAcquire(&hdr->mutex);
	for (i = 0; i < hdr->node_toc_map_size; i++)
	{
		if (hdr->node_toc_map[i] == origin_id)
			my_node_toc_entry = i;
		else if (first_free_toc_entry == -1 && hdr->node_toc_map[i] == 0)
			first_free_toc_entry = i;
	}
	if (my_node_toc_entry == -1 && first_free_toc_entry != -1)
		hdr->node_toc_map[first_free_toc_entry] = origin_id;
	SpinLockRelease(&hdr->mutex);

	if (my_node_toc_entry != -1)
		/* shouldn't happen */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("receiving node %u already has sending node %u registered in shmem",
				 		bdr_get_local_nodeid(), origin_id)));

	/*
	 * Even though we reserved local state for the peer earlier we could fail
	 * to get space in the shmem queues array for it if the array was recently
	 * full and a peer is being laggard about exiting so we can clean up.
	 *
	 * TODO: handle running out of memqueues more gracefully on peer add
	 */
	if (first_free_toc_entry == -1)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("no free shared memory slots on %u for message queue for node %u while adding peer",
				 		 bdr_get_local_nodeid(), origin_id)));

	/*
	 * We already claimed this entry while holding the spinlock,
	 * if there was one free
	 */
	my_node_toc_entry = first_free_toc_entry;

	/*
	 * The queue won't be created yet - we just allocated space for it earlier,
	 * or it's one that's been cleaned up for re-use. So write a queue into
	 * the space and attach to it.
	 *
	 * Remember ToC is 1-indexed due to header entry
	 */
	mq_addr = shm_toc_lookup(toc, my_node_toc_entry+1, false);
	mq = shm_mq_create(mq_addr, msgb_recv_queue_size);
	Assert(mq == mq_addr);

	shm_mq_set_receiver(mq, MyProc);

	/* Finish setting up broker-side state */
	p->sender_id = origin_id;
	p->max_received_msgid = 0;
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	p->recvqueue = bdr_shm_mq_attach(mq, broker_dsm_seg, NULL);
	p->recvsize = 0;
	if (p->recvbuf != NULL)
		pfree(p->recvbuf);
	p->recvbuf = NULL;
	(void) MemoryContextSwitchTo(old_ctx);

	/* And allow this peer to connect by mapping the entry in the ToC */
	SpinLockAcquire(&hdr->mutex);
	hdr->node_toc_map[my_node_toc_entry] = origin_id;
	SpinLockRelease(&hdr->mutex);
}

/*
 * We don't expect to hear from this peer again, so release it's memory queue
 * and forget about its messages, freeing up a slot for another peer.
 */
void
msgb_remove_receive_peer(uint32 origin_id)
{
	int				i;
	MsgbReceivePeer *p = NULL;
	shm_mq		   *mq;
	MsgbDSMHdr	   *hdr;
	int				my_node_toc_entry;

	Assert(InBrokerProcess());

	/* look up broker-local state for peer */
	for (i = 0; i < msgb_max_peers; i++)
	{
		if (recvpeers[i].sender_id == origin_id)
		{
			p = &recvpeers[i];
			break;
		}
	}

	if (p == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("peer node %u is not registered with the broker",
				 		origin_id)));

	msgb_mq_for_node(broker_dsm_seg, origin_id, &hdr, &my_node_toc_entry, &mq);
	Assert(shm_mq_get_receiver(mq) == MyProc);
	p->pending_cleanup = true;

	shm_mq_detach(mq);

	/*
	 * At this point the broker is detached but the peer may still be attached,
	 * so trying to overwrite it would be ... bad. We set the pending cleanup
	 * flag above and we'll re-use the space later, once the peer has gone.
	 */

	/* Clear broker-side state for re-use */
	p->sender_id = 0;
	p->max_received_msgid = 0;
}

void
msgb_shutdown_receive(void)
{
	if (recvpeers)
	{
		pfree(recvpeers);
		recvpeers = NULL;
	}

	/*
	 * We don't have to detach from our shmem queues etc. When the broker detaches
	 * from the DSM segment it'll all get cleaned up.
	 */
	if (broker_dsm_seg != NULL)
	{
		dsm_detach(broker_dsm_seg);
		Assert(broker_dsm_seg == NULL);
	}
}

static Size
msgb_calc_shmem_size()
{
	Assert(msgb_max_local_nodes >= 1);
	return offsetof(MsgbShmemContext, shmem_mq_seg)
		   + sizeof(MsgbDynamicSegmentHandle)*msgb_max_local_nodes;
}

/*
 * The message broker needs additional shmem to hold the message queues.
 * DSM can be used, since it only has to exist for as long as the message
 * queues do.
 *
 * We'll need a way to get the DSM handle to interested backends though,
 * so a small static shmem segment is required. It only has to be big
 * enough for the DSM handle for each broker and a PGPROC entry to
 * point to the manager.
 */
static void
msgb_shmem_startup_receive(void)
{
	bool        found;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	msgb_ctx = ShmemInitStruct("msgb_dsm_handle",
							   msgb_calc_shmem_size(), &found);

	if (!found)
	{
		memset(msgb_ctx, 0, msgb_calc_shmem_size());
		msgb_ctx->lock = &(GetNamedLWLockTranche("msgb"))->lock;
		msgb_ctx->msgb_max_local_nodes = msgb_max_local_nodes;
	}
}

void
msgb_shmem_init_receive(int max_local_nodes)
{
	Assert(process_shared_preload_libraries_in_progress);

	msgb_max_local_nodes = max_local_nodes;

	/* Allocate enough shmem for the worker limit ... */
	RequestAddinShmemSpace(msgb_calc_shmem_size());

	/*
	 * We'll need to be able to take exclusive locks so only one per-db backend
	 * tries to allocate or free message queue dsm handle entries.
	 */
	RequestNamedLWLockTranche("msgb", 1);

	msgb_ctx = NULL;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = msgb_shmem_startup_receive;
}

/*
 * We've fully received a message over the shm mq and can deliver it to the
 * waiting callback then discard it.
 */
static void
msgb_deliver_msg(MsgbReceivePeer *peer)
{
	uint32 msgid = ((uint32*)peer->recvbuf)[0]; /* FIXME: alignment? */
	char *buf;
	Size bufsize;

	Assert(InBrokerProcess());

	Assert(peer->recvsize > sizeof(uint32));
	bufsize = peer->recvsize - sizeof(uint32);
	buf = ((char*)peer->recvbuf) + sizeof(uint32);

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
					(errmsg_internal("discarding already processed message id %u from %u on %u; seen up to %u",
									msgid, peer->sender_id, bdr_get_local_nodeid(), peer->max_received_msgid)));

		if (msgid != peer->max_received_msgid + 1)
			ereport(WARNING,
					(errmsg_internal("peer %u sending to %u appears to have skipped from msgid %u to %u",
									 peer->sender_id, bdr_get_local_nodeid(), peer->max_received_msgid, msgid)));

		(msgb_received_hook)(msgid, buf, bufsize);

		peer->max_received_msgid = msgid;
	}

	pfree(peer->recvbuf);
	peer->recvbuf = NULL;
	peer->recvsize = 0;
}

/* Look up a node in our shared memory state */
static void
msgb_mq_for_node(dsm_segment *seg, uint32 nodeid, MsgbDSMHdr **hdr, int *hdr_idx, shm_mq **mq)
{
	shm_toc	   *toc;
	int			my_node_toc_entry = -1;
	int			i;

	Assert(InBrokerProcess());

	toc = shm_toc_attach(BDR_SHMEM_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	/* Look up shared state for the peer */
	*hdr = shm_toc_lookup(toc, 0, false);
	SpinLockAcquire(&(*hdr)->mutex);
	for (i = 0; i < (*hdr)->node_toc_map_size; i++)
	{
		if ((*hdr)->node_toc_map[i] == nodeid)
			my_node_toc_entry = i;
	}
	SpinLockRelease(&(*hdr)->mutex);

	if (my_node_toc_entry == -1)
		/* shouldn't happen */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg_internal("receiving node %u not registered with broker shmem %u",
				 		bdr_get_local_nodeid(), nodeid)));

	*mq = shm_toc_lookup(toc, my_node_toc_entry+1, false);
}

/*
 * The peer went away on this slot, or we want to detach from it
 * and have the peer exit so we can reuse the slot.
 *
 * If we know the peer is already gone we can immediately clean up
 * the slot. Otherwise we must mark it pending cleanup and let
 * a sweep clean it later, once the peer noticed our disconnect
 * and detached its self.
 */
static void
msgb_try_cleanup_peer_slot(MsgbReceivePeer *peer)
{
	MsgbDSMHdr *hdr;
	int hdr_idx;
	shm_mq *mq;

	Assert(peer->pending_cleanup == true);
	Assert(InBrokerProcess());

	msgb_mq_for_node(broker_dsm_seg, peer->sender_id, &hdr, &hdr_idx, &mq);
	if (shm_mq_get_receiver(mq) != NULL)
		shm_mq_detach(mq);

	if (shm_mq_get_sender(mq) == NULL)
	{
		SpinLockAcquire(&hdr->mutex);
		hdr->node_toc_map[hdr_idx] = 0;
		SpinLockRelease(&hdr->mutex);

		peer->pending_cleanup = false;
	}
}

/*
 * Service the shmem queues, cleaning up after exited peers,
 * consuming pending messages, etc.
 */
void
msgb_service_connections_receive(void)
{
	int				i;

	if (recvpeers == NULL)
	{
		/* Do nothing if broker is shut down */
		return;
	}

	Assert(InBrokerProcess());

	for (i = 0; i < msgb_max_peers; i++)
	{
		MsgbReceivePeer * const p = &recvpeers[i];

		if (recvpeers[i].sender_id != 0)
		{
			shm_mq_result	res;

			/* If a sender_id is set, there must be an associated queue */
			Assert(p->recvqueue != NULL);

			/*
			 * We must perform a non-blocking read of queue, since we don't know
			 * if there's anything to read at all on this socket. Our latch got set
			 * but we don't know by whom.
			 *
			 * It's a nonblocking read so we can avoid the in_shm_mq dance with
			 * the bdr_shm_mq_receive wrapper, we can't get stuck in shm_mq_wait_internal.
			 */
			res = shm_mq_receive(p->recvqueue, &p->recvsize, &p->recvbuf, true);
			switch (res)
			{
				case SHM_MQ_WOULD_BLOCK:
					/*
					 * There's nothing here to read, or we read a partial message
					 * but not all of it fit in the buffer. Either way we preserve
					 * state and wait until our latch is set again.
					 */
					continue;
				case SHM_MQ_SUCCESS:
					/*
					 * Yay, we can deliver the message. We need to read out the
					 * message-id that's addressed to us and pass the the
					 * message to the caller.
					 */
					msgb_deliver_msg(p);
					break;
				case SHM_MQ_DETACHED:
					/*
					 * Our peer went away. They'll need a fresh queue to reconnect
					 * to so we have to reset our side's state.
					 */
					p->pending_cleanup = true;
					break;
			}
		}

		if (p->pending_cleanup)
			msgb_try_cleanup_peer_slot(p);

		CHECK_FOR_INTERRUPTS();
	}
}
