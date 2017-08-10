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

typedef struct MsgbReceiveQueue
{
	uint32 sender_id;
	int max_received_msgid;
} MsgbReceiveQueue;

static MsgbReceiveQueue *recvqueues = NULL;

/*
 * State used by normal backends acting as message receiver workers.
 * Unused in the manager.
 */
static uint32 connected_peer_id = 0;
static shm_mq_handle *send_mq;

static void msgb_connect_shmem(uint32 origin_node);

msgb_received_hook_type msgb_received_hook = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * In a normal user backend, attach to the manager's shmem queue for
 * the connecting peer and prepare to send messages.
 */
PG_FUNCTION_INFO_V1(msgb_connect);

Datum
msgb_connect(PG_FUNCTION_ARGS)
{
	uint32			origin_node = PG_GETARG_UINT32(0);
	uint32			destination_node = PG_GETARG_UINT32(1);

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

	/*
	 * TODO: add a way to tell when the downstream broker has restarted and
	 * reset its message IDs, so we know to start accepting lower message IDs
	 * for the peer. Maybe we need to send a start timestamp here.
	 */

	PG_RETURN_VOID();
}

static void
msgb_connect_shmem(uint32 origin_node)
{
	const uint32	local_node = bdr_get_local_nodeid();
	dsm_segment	   *seg;
	shm_toc		   *toc;
	MsgbDSMHdr	   *hdr;
	int				my_node_toc_entry, first_free_toc_entry;
	shm_mq		   *mq;
	MemoryContext	old_ctx;
	int				i;
	PGPROC*			cur_receiver;

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
	 * Using the header at ToC entry 0, find out if our node already has a
	 * memory queue and if not, claim a free one.
	 */
	hdr = shm_toc_lookup(toc, 0, false);
	first_free_toc_entry = -1;
	my_node_toc_entry = -1;
	SpinLockAcquire(&hdr->mutex);
	for (i = 0; i < hdr->node_toc_map_size; i++)
	{
		if (hdr->node_toc_map[i] == local_node)
			my_node_toc_entry = i;
		else if (hdr->node_toc_map[i] == 0 && first_free_toc_entry == -1)
			first_free_toc_entry = i;
	}
	/* Didn't find an entry for us, claim one */
	if (my_node_toc_entry == -1 && first_free_toc_entry != -1)
	{
		hdr->node_toc_map[first_free_toc_entry] = local_node;
		my_node_toc_entry = first_free_toc_entry;
	}
	SpinLockRelease(&hdr->mutex);

	if (my_node_toc_entry == -1 && first_free_toc_entry == -1)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("couldn't find free broker entry for this node, too many connected nodes")));

	mq = shm_toc_lookup(toc, my_node_toc_entry, false);

	/*
	 * We need to do this under the mq spinlock because
	 * shm_mq doesn't offer a test-and-set interface
	 * or an option to error if the queue is attached. It
	 * just Assert()s.
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
	send_mq = shm_mq_attach(mq, seg, NULL);
	(void) MemoryContextSwitchTo(old_ctx);

	if (shm_mq_wait_for_attach(send_mq) != SHM_MQ_SUCCESS)
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
	 */
	res = shm_mq_send(send_mq, sizeof(int), &message_id, false);
	if (res != SHM_MQ_SUCCESS)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("message broker for node %u appears to have exited",
						bdr_get_local_nodeid())));

	res = shm_mq_send(send_mq, VARSIZE_ANY(payload), VARDATA_ANY(payload), false);
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

	/*
	 * TODO FIXME: Right now ignoring "seen" message IDs will be wrong
	 * if the remote broker has restarted, see comments in connect
	 * function.
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
msgb_startup_receive(Size recv_queue_size)
{
	shm_toc_estimator e;
	dsm_segment *seg;
	shm_toc    *toc;
	Size		hdr_size, segsize;
	int			i;
	MsgbDSMHdr *hdr;

	if (msgb_received_hook == NULL)
		ereport(ERROR, (errmsg_internal("no message receive hook is registered")));

	/*
	 * Allocate an entry for ourselves in the static shmem, so workers
	 * can find our dynamic shmem: segment and know who we are.
	 *
	 * TODO: handle cleanup of dead workers via atexit
	 */
	Assert(msgb_ctx != NULL);
	Assert(msgb_my_seg == NULL);
	LWLockAcquire(msgb_ctx->lock, LW_EXCLUSIVE);
	for (i = 0; i < msgb_ctx->msgb_max_local_nodes; i++)
	{
		if (msgb_ctx->shmem_mq_seg[i].node_id == 0)
		{
			msgb_my_seg = &msgb_ctx->shmem_mq_seg[i];
			msgb_my_seg->node_id = bdr_get_local_nodeid();
			msgb_my_seg->manager = MyProc;
			Assert(msgb_my_seg->dsm_seg_handle == 0);
		}
	}
	LWLockRelease(msgb_ctx->lock);

	if (msgb_my_seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("maximum number of message brokers exceeded")));

	hdr_size = offsetof(MsgbDSMHdr, node_toc_map) + sizeof(uint32)*msgb_max_peers;

	/*
	 * Prepare the ToC that maps node-ids to the associated shmem_mq.
	 *
	 * We need a header segment that maps ToC indexes to the sender
	 * node-id using that segment, since 
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, hdr_size);
	for (i = 0; i <= msgb_max_peers; ++i)
		shm_toc_estimate_chunk(&e, (Size) recv_queue_size);
	shm_toc_estimate_keys(&e, msgb_max_peers);
	segsize = shm_toc_estimate(&e);

	/* Create the shared memory segment and establish a table of contents. */
	seg = dsm_create(shm_toc_estimate(&e), 0);
	toc = shm_toc_create(BDR_SHMEM_MAGIC, dsm_segment_address(seg),
						 segsize);

	/* set up the header */
	hdr = shm_toc_allocate(toc, hdr_size);
	SpinLockInit(&hdr->mutex);
	hdr->node_toc_map_size = msgb_max_peers;
	memset(hdr->node_toc_map, 0, sizeof(uint32)*msgb_max_peers);
	shm_toc_insert(toc, 0, hdr);

	/* Set up one message queue per peer */
	for (i = 0; i < msgb_max_peers + 1; i++)
	{
		shm_mq	   *mq;

		mq = shm_mq_create(shm_toc_allocate(toc, (Size) recv_queue_size),
						   (Size) recv_queue_size);
		shm_toc_insert(toc, i+1, mq);
	}

	/* Store a handle to the DSM seg where others can find it */
	LWLockAcquire(msgb_ctx->lock, LW_EXCLUSIVE);
	msgb_my_seg->dsm_seg_handle = dsm_segment_handle(seg);
	LWLockRelease(msgb_ctx->lock);
}

void
msgb_shutdown_receive(void)
{
	elog(WARNING, "TODO");
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
 * Service the shmem queues, cleaning up after exited peers,
 * consuming pending messages, etc.
 */
void
msgb_service_connections_receive(void)
{
	elog(ERROR, "TODO");
}
