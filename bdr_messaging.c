/*-------------------------------------------------------------------------
 *
 * bdr_messaging.c
 * 		BDR message handling using consensus manager and message broker
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_messaging.c
 *
 * This is the BDR side of the messaging implementation. It uses the
 * consensus manager and the underlying message broker, both of which
 * are fairly BDR-indendent, to implement BDR state management and
 * inter-node messaging.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"

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

#include "pglogical_worker.h"
#include "pglogical_plugins.h"

#include "bdr_shmem.h"
#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_join.h"
#include "bdr_msgbroker.h"
#include "bdr_msgbroker_send.h"
#include "bdr_messaging.h"
#include "bdr_worker.h"

typedef enum SubmitMQMessageType
{
	MSG_UNUSED_TYPE = 0,
	MSG_START_ENQUEUE,
	MSG_START_ENQUEUE_RESULT,
	MSG_ENQUEUE_PROPOSAL,
	MSG_ENQUEUE_RESULT,
	MSG_FINISH_ENQUEUE,
	MSG_FINISH_ENQUEUE_RESULT,
	MSG_QUERY_STATUS,
	MSG_QUERY_STATUS_RESULT
} SubmitMQMessageType;

/*
 * A message (inbound or reply) carried on the message submission
 * queues.
 *
 * Used for enqueuing messages, getting response statuses, etc.
 *
 * (We do NOT need this for getting committed message payloads to other
 * backends, since they're available from the DB.)
 */
typedef struct SubmitMQMessage
{
	SubmitMQMessageType msg_type;
	Size payload_length;
	char * payload[FLEXIBLE_ARRAY_MEMBER];
} SubmitMQMessage;

typedef enum SubmitMQState
{
	MQ_NEED_REINIT,
	MQ_WAIT_PEER_DETACH,
	MQ_WAIT_PEER_MESSAGE,
	MQ_PEER_ATTACHED,
	MQ_REPLY_PENDING
} SubmitMQState;

/*
 * manager-local state for the shm_mq pair (TODO) used to receive messages from
 * other backends. Presumably to be later extended into a set of pairs.
 *
 * This same struct is used on backends when talking to the manager.
 */
typedef struct SubmitMQ
{
	SubmitMQState	state;
	MemoryContext	mq_context;
	/* all these are in mq_context */
	shm_mq_handle  *recvq_handle;
	shm_mq_handle  *sendq_handle;
	void 		   *recvbuf;
	Size			recvbufsize;
	void		   *sendbuf;
	Size			sendbufsize;
} SubmitMQ;

static SubmitMQ submit_mq;

static bool bdr_proposals_receive(ConsensusProposal *msg);
static bool bdr_proposals_prepare(List *messages);
static void bdr_proposals_commit(List *messages);
static void bdr_proposals_rollback(void);
static void bdr_request_recreate_wait_event_set_hook(WaitEventSet *old_set);
static void bdr_messaging_atexit(int code, Datum argument);
static void bdr_startup_submit_shm_mq(void);
static void bdr_reinit_submit_shm_mq(void);
static void bdr_detach_manager_queue(void);

static BdrManagerShmem *my_manager;

static bool atexit_registered = false;

/*
 * TODO this is a single-queue-pair hack to let us do basic
 * message submissions from backends. See notes on bdr_shmem.h
 */

void
bdr_start_consensus(int bdr_max_nodes)
{
	List	   *subs;
	ListCell   *lc;
	StringInfoData si;

	Assert(is_bdr_manager());

	my_manager = bdr_shmem_lookup_manager_segment(bdr_get_local_nodeid(), false);

	initStringInfo(&si);

	consensus_proposals_receive_hook = bdr_proposals_receive;
	consensus_proposals_prepare_hook = bdr_proposals_prepare;
	consensus_proposals_commit_hook = bdr_proposals_commit;
	consensus_proposals_rollback_hook = bdr_proposals_rollback;
	msgb_request_recreate_wait_event_set_hook = bdr_request_recreate_wait_event_set_hook;

	bdr_startup_submit_shm_mq();

	Assert(!IsTransactionState());
	StartTransactionCommand();

	consensus_begin_startup(bdr_get_local_nodeid(), BDR_SCHEMA_NAME,
					  BDR_MSGJOURNAL_REL_NAME, bdr_max_nodes);

	subs = bdr_get_node_subscriptions(bdr_get_local_nodeid());

	foreach (lc, subs)
	{
		PGLogicalSubscription *sub = lfirst(lc);
		resetStringInfo(&si);
		appendStringInfoString(&si, sub->origin_if->dsn);
		appendStringInfo(&si, " application_name='bdr_msgbroker %i'",
						 bdr_get_local_nodeid());
		Assert(sub->target->id == bdr_get_local_nodeid());
		consensus_add_node(sub->origin->id, sub->origin_if->dsn);
	}

	consensus_finish_startup();

	pfree(si.data);

	CommitTransactionCommand();
}

static void
bdr_startup_submit_shm_mq(void)
{
	submit_mq.state = MQ_NEED_REINIT;
	if (!atexit_registered)
	{
		before_shmem_exit(bdr_messaging_atexit, (Datum)0);
		atexit_registered = true;
	}

	Assert(submit_mq.mq_context == NULL);
	submit_mq.mq_context  = AllocSetContextCreate(TopMemoryContext,
												  "bdr messaging mgr shm_mq IPC",
												  ALLOCSET_DEFAULT_SIZES);

	bdr_reinit_submit_shm_mq();
}

/*
 * Create and attach to the mq's, either for initial startup or after
 * a detach.
 */
static void
bdr_reinit_submit_shm_mq(void)
{
	shm_mq *sendq, *recvq;
	MemoryContext old_ctx;

	Assert(submit_mq.state == MQ_NEED_REINIT);
	Assert(submit_mq.recvq_handle == NULL);
	Assert(submit_mq.sendq_handle == NULL);

	LWLockAcquire(bdr_ctx->lock, LW_EXCLUSIVE);

	Assert(my_manager != NULL);
	sendq = shm_mq_create((void*)my_manager->shm_send_mq, SHM_MQ_SIZE);
	recvq = shm_mq_create((void*)my_manager->shm_recv_mq, SHM_MQ_SIZE);
	Assert((void*)sendq == (void*)my_manager->shm_send_mq);
	Assert((void*)recvq == (void*)my_manager->shm_recv_mq);

	shm_mq_set_receiver(recvq, MyProc);
	shm_mq_set_sender(sendq, MyProc);

	old_ctx = MemoryContextSwitchTo(submit_mq.mq_context);
	submit_mq.recvq_handle = shm_mq_attach(recvq, NULL, NULL);
	submit_mq.sendq_handle = shm_mq_attach(sendq, NULL, NULL);
	(void) MemoryContextSwitchTo(old_ctx);
	LWLockRelease(bdr_ctx->lock);

	submit_mq.state = MQ_WAIT_PEER_MESSAGE;
}

/*
 * Do local-side shmem detach. Same in manager and
 * peer except that we swap "send" and "receive"
 * in peer.
 */
static void
submit_mq_detach(void)
{
	shm_mq *sendq, *recvq, *tempq;

	sendq = (void*)my_manager->shm_send_mq;
	recvq = (void*)my_manager->shm_recv_mq;

	if (!is_bdr_manager())
	{
		/* "send" and "receive" are reversed on peer */
		tempq = sendq;
		sendq = recvq;
		recvq = tempq;
	}

	if (submit_mq.mq_context == NULL)
		Assert(submit_mq.sendq_handle == NULL && submit_mq.recvq_handle == NULL);

	if (submit_mq.recvq_handle != NULL)
	{
		shm_mq_detach(recvq);
		submit_mq.recvq_handle = NULL;
		submit_mq.recvbuf = NULL;
		submit_mq.recvbufsize = 0;
	}
	if (submit_mq.sendq_handle != NULL)
	{
		shm_mq_detach(sendq);
		submit_mq.sendq_handle = NULL;
		submit_mq.sendbuf = NULL;
		submit_mq.sendbufsize = 0;
	}
	if (submit_mq.mq_context != NULL)
		MemoryContextReset(submit_mq.mq_context);
}

/*
 * A peer has detached, so we need to clean up local state. After this we
 * can lock and re-init the queues.
 */
static void
bdr_detach_submit_shm_mq(void)
{
	shm_mq *sendq, *recvq;

	if (submit_mq.mq_context == NULL)
	{
		/* We're shut down */
		return;
	}

	Assert(is_bdr_manager());

	if (submit_mq.state == MQ_NEED_REINIT)
		return;

	sendq = (void*)my_manager->shm_send_mq;
	recvq = (void*)my_manager->shm_recv_mq;

	if (submit_mq.state != MQ_WAIT_PEER_DETACH)
		/* Do the local-side detach work */
		submit_mq_detach();
	
	/*
	 * Our side is detached now but what about the peer?
	 * They may still be connected to the queue, in which
	 * case we cannot safely clobber it. So we must wait
	 * for the peer to detach and re-create.
	 */
	submit_mq.state = MQ_WAIT_PEER_DETACH;

	if (shm_mq_get_sender(recvq) == NULL && shm_mq_get_receiver(sendq) == NULL)
	{
		/* Peer is gone */
		submit_mq.state = MQ_NEED_REINIT;
	}

}

/*
 * Prep a message for submission on the queue.
 *
 * Usable on both manager and peers.
 */
static void
bdr_make_msg_submit_shm_mq(SubmitMQMessageType msgtype, Size payload_size,
						   void *payload)
{
	SubmitMQMessage *msg;

	/*
	 * It's OK to abandon any data here, shm_mq's handle remembers it
	 * and will clean it up if needed when we try to send the next msg
	 */
	submit_mq.sendbufsize = offsetof(SubmitMQMessage,payload) + payload_size;
	submit_mq.sendbuf = MemoryContextAlloc(submit_mq.mq_context,
										   submit_mq.sendbufsize);

	msg = submit_mq.sendbuf;
	msg->msg_type = msgtype;
	msg->payload_length = payload_size;
	memcpy(msg->payload, payload, payload_size);

	Assert(submit_mq.sendbufsize == offsetof(SubmitMQMessage,payload) + msg->payload_length);
}

/*
 * We got a message from a peer in the manager and need to dispatch a reply
 * once we've acted on it.
 */
static void
bdr_received_submit_shm_mq(void)
{
	SubmitMQMessage *msg;

	Assert(is_bdr_manager());
	Assert (submit_mq.state == MQ_WAIT_PEER_MESSAGE);
	Assert(submit_mq.recvbufsize >= offsetof(SubmitMQMessage,payload));

	/*
	 * Unpack the message and decide what the sender wants to do.
	 *
	 * Since the message structure is from the same postgres instance and hasn't
	 * gone over the wire, we can just access it directly.
	 *
	 * FIXME: alignment; should memcpy header into stack struct?
	 */
 	msg = submit_mq.recvbuf;

	switch(msg->msg_type)
	{
		case MSG_START_ENQUEUE:
		{
			bool ret = bdr_msgs_begin_enqueue();
			bdr_make_msg_submit_shm_mq(MSG_START_ENQUEUE_RESULT,
										sizeof(bool), &ret);
			break;
		}
		case MSG_ENQUEUE_PROPOSAL:
		{
			uint64 handle;
			/*
			 * FIXME: should be doing message serialization/deserialization
			 * here, not passing around struct BdrMessage. See bdr_msgs_enqueue.
			 *
			 * TODO: error reporting to peers on submit failure
			 */
			handle = bdr_msgs_enqueue((BdrMessage*)msg->payload);
			bdr_make_msg_submit_shm_mq(MSG_ENQUEUE_RESULT, sizeof(uint64),
									   &handle);
			break;
		}
		case MSG_FINISH_ENQUEUE:
		{
			uint64 ret = bdr_msgs_finish_enqueue();
			bdr_make_msg_submit_shm_mq(MSG_FINISH_ENQUEUE_RESULT,
									   sizeof(uint64), &ret);
			break;
		}
		case MSG_QUERY_STATUS:
		{
			ConsensusProposalStatus status;
			status = bdr_msg_get_outcome(*((uint64*)msg->payload));
			bdr_make_msg_submit_shm_mq(MSG_QUERY_STATUS_RESULT,
									   sizeof(ConsensusProposalStatus),
									   &status);
			break;
		}
		case MSG_UNUSED_TYPE:
		case MSG_START_ENQUEUE_RESULT:
		case MSG_ENQUEUE_RESULT:
		case MSG_FINISH_ENQUEUE_RESULT:
		case MSG_QUERY_STATUS_RESULT:
			elog(ERROR, "peer sent unsupported message type %u to manager, shmem protocol violation",
				 msg->msg_type);
	}
	
	/* reply queued */
	Assert(submit_mq.sendbufsize > 0);
	Assert(submit_mq.sendbuf != NULL);

	submit_mq.state = MQ_REPLY_PENDING;
}

/*
 * When an attempt is made to use bdr_messaging from a backend other than the
 * manager worker, we dispatch the request over the messaging system's shmem
 * message queues. Then block waiting for a reply. See
 * bdr_service_submit_shmem_mq for the protocol.
 *
 * The non-manager side is handled with
 * bdr_attach_manager_queue, bdr_submit_manager_queue, bdr_detach_manager_queue.
 *
 * Note that any backend calling this must have a signal handler that sets
 * ProcDiePending and InterruptPending on SIGTERM; see 
 * https://www.postgresql.org/message-id/CAMsr+YHmm=01LsuEYR6YdZ8CLGfNK_fgdgi+QXUjF+JeLPvZQg@mail.gmail.com
 * 
 * Unlike the manager-side functions, these ones will block when
 * the manager isn't ready, block waiting for replies, etc.
 */
static void
bdr_attach_manager_queue(void)
{
	shm_mq *recvq, *sendq;

	Assert(!is_bdr_manager());

	if (submit_mq.mq_context != NULL
		&& submit_mq.sendq_handle != NULL
		&& submit_mq.recvq_handle != NULL)
	{
		/*
		 * Already attached, or we were. Don't bother checking
		 * the peer status, we'll notice if it's gone during submit.
		 */
		return;
	}

	submit_mq.mq_context  = AllocSetContextCreate(TopMemoryContext,
												  "bdr messaging peer shm_mq IPC",
												  ALLOCSET_DEFAULT_SIZES);

	my_manager = bdr_shmem_lookup_manager_segment(bdr_get_local_nodeid(),
												  false);

	/*
	 * the queue names in manager shmem are from the manager's PoV; everything
	 * subsequent to that is from our PoV
	 */
	recvq = (void*)my_manager->shm_send_mq;
	sendq = (void*)my_manager->shm_recv_mq;

	if (!atexit_registered)
	{
		before_shmem_exit(bdr_messaging_atexit, (Datum)0);
		atexit_registered = true;
	}

	/*
	 * We must attach to both queues under a lock that prevents
	 * them being concurrently clobbered by the manager. Once
	 * we're attached, our attachment protects them.
	 *
	 * Ideally we'd pass the BgWorkerHandle for the manager here, but
	 * we can't copy them into shmem because they're an opaque struct.
	 */
	LWLockAcquire(bdr_ctx->lock, LW_EXCLUSIVE);
	shm_mq_set_receiver(recvq, MyProc);
	shm_mq_set_sender(sendq, MyProc);
	submit_mq.sendq_handle = shm_mq_attach(sendq, NULL, NULL);
	submit_mq.recvq_handle = shm_mq_attach(recvq, NULL, NULL);
	LWLockRelease(bdr_ctx->lock);

	submit_mq.recvbuf = NULL;
	submit_mq.recvbufsize = 0;
	submit_mq.sendbuf = NULL;
	submit_mq.sendbufsize = 0;

	/*
	 * Manager should already be attached, but just in case.
	 *
	 * Waits forever unless interrupted by fatal signal.
	 * TODO: nonblocking mode where we retry later?
	 */
	(void) shm_mq_wait_for_attach(submit_mq.sendq_handle);
	(void) shm_mq_wait_for_attach(submit_mq.recvq_handle);
}

/*
 * From a non-manager backend, submit a message to the manager
 * and return the manager's reply.
 */
static SubmitMQMessage*
bdr_submit_manager_queue(SubmitMQMessageType msgtype, Size payload_size,
						 void *payload)
{
	SubmitMQMessage *reply_msg;

	Assert(!is_bdr_manager());

	Assert(my_manager != NULL);
	Assert(submit_mq.sendq_handle != NULL);
	Assert(submit_mq.recvq_handle != NULL);

	/* Populates submit_mq.sendbuf and sendbufsize */
	bdr_make_msg_submit_shm_mq(msgtype, payload_size, payload);

	/* Exchange a pair of messages with the manager */
	switch (shm_mq_send(submit_mq.sendq_handle,
							submit_mq.sendbufsize,
							submit_mq.sendbuf, false))
	{
		case SHM_MQ_SUCCESS:
			break;
		case SHM_MQ_DETACHED:
			bdr_detach_manager_queue();
			/* TODO: better error */
			elog(ERROR, "manager detached during message submit");
		case SHM_MQ_WOULD_BLOCK:
			Assert(false);
			break;
	}

	submit_mq.recvbufsize = 0;
	submit_mq.recvbuf = NULL;
	switch (shm_mq_receive(submit_mq.recvq_handle,
							   &submit_mq.recvbufsize,
							   &submit_mq.recvbuf, false))
	{
		case SHM_MQ_SUCCESS:
			break;
		case SHM_MQ_DETACHED:
			bdr_detach_manager_queue();
			/* TODO: better error */
			elog(ERROR, "manager detached during message reply receive");
		case SHM_MQ_WOULD_BLOCK:
			Assert(false);
			break;
	}

	reply_msg = submit_mq.recvbuf;
	Assert(submit_mq.recvbufsize == offsetof(SubmitMQMessage,payload) + reply_msg->payload_length);

	return reply_msg;
}

static void
bdr_detach_manager_queue(void)
{
	Assert(!is_bdr_manager());

	/*
	 * The non-manager side doesn't need to do any state management
	 * or wait for the manager to detach. It just closes down.
	 */
	submit_mq_detach();
}

/*
 * Test to see if the reply we sent is what we should be sending
 * for a given incoming message
 */
static void
check_allowed_reply_type(void)
{
	SubmitMQMessageType inbound, reply;
	bool ok;

	Assert(submit_mq.recvbuf != NULL);
	Assert(submit_mq.sendbuf != NULL);
	inbound = ((SubmitMQMessage*)submit_mq.recvbuf)->msg_type;
	reply = ((SubmitMQMessage*)submit_mq.sendbuf)->msg_type;

	switch (inbound)
	{
		case MSG_START_ENQUEUE:
			ok = (reply == MSG_START_ENQUEUE_RESULT); break;
		case MSG_ENQUEUE_PROPOSAL:
			ok = (reply == MSG_ENQUEUE_RESULT); break;
		case MSG_FINISH_ENQUEUE:
			ok = (reply == MSG_FINISH_ENQUEUE_RESULT); break;
		case MSG_QUERY_STATUS:
			ok = (reply == MSG_QUERY_STATUS_RESULT); break;
		case MSG_START_ENQUEUE_RESULT:
		case MSG_UNUSED_TYPE:
		case MSG_ENQUEUE_RESULT:
		case MSG_FINISH_ENQUEUE_RESULT:
		case MSG_QUERY_STATUS_RESULT:
			/* not allowed as replies from manager */
			ok = false;
			break;
	}

	if (!ok)
		elog(ERROR, "internal error: tried to reply to message %u with message %u",
			 inbound, reply);
}

static void
bdr_service_submit_shmem_mq(void)
{
	/*
	 * Poll our one and only shmem MQ for work.
	 *
	 * The protocol here is entirely peer-initiated, and consists of
	 * pairs of messages. There's no ongoing conversation. We're
	 * already attached to the peer end.
	 *
	 * Initial state:
	 *   	state = MQ_WAIT_PEER_MESSAGE
	 *   	we're attached to both our queues, don't know about peer
	 *
	 * - Peer attaches
	 *   	(no state change, we don't care)
	 * - Received message from peer
	 *   	state => MQ_REPLY_PENDING
	 * - Reply sent to peer
	 *   	state => MQ_WAIT_PEER_MESSAGE
	 *
	 * At any time in the above the peer can detach:
	 *
	 * - Peer detached from at least one queue
	 *   	state => MQ_WAIT_PEER_DETACH
	 * - Peer confirmed detached both queues
	 *   	state => MQ_NEED_REINIT
	 * - Reinit queues for reuse
	 *   	state => MQ_WAIT_PEER_MESSAGE
	 * 
	 * at which point we reset the queue for the next user.
	 *
	 * If the peer detaches prematurely, we clean up local state and
	 * reset the queue.
	 *
	 * Note that the peer must not detach from the send queue until it gets
	 * a reply on the receive queue.
	 *
	 * We don't need to explicitly acquire any lock here; the shm_mq does its
	 * own locking, and we're the only process that can overwrite it.
	 *
	 * TODO: allow a batch of messages to be consumed and processed in one go
	 * with replies queued up, so we can free up the bus faster.
	 */
	if (submit_mq.state == MQ_WAIT_PEER_MESSAGE)
	{
		/*
		 * Try to receive until we get a full message.
		 *
		 * The peer might actually already be attached and not have submitted
		 * yet or might have submitted a partial message last time around. We
		 * don't care.
		 */
		switch (shm_mq_receive(submit_mq.recvq_handle,
							 &submit_mq.recvbufsize,
							 &submit_mq.recvbuf, true))
		{
			case SHM_MQ_SUCCESS:
				/* queue up a reply */
				bdr_received_submit_shm_mq();
				submit_mq.state = MQ_REPLY_PENDING;
				break;
			case SHM_MQ_WOULD_BLOCK:
				break;
			case SHM_MQ_DETACHED:
				/*
				 * Peer went away. It could still be attached to our send
				 * queue, though, and we need local state cleanup anyway.
				 */
				bdr_detach_submit_shm_mq();
				Assert(submit_mq.state == MQ_WAIT_PEER_DETACH || submit_mq.state == MQ_NEED_REINIT);
				break;
		}
	}

	if (submit_mq.state == MQ_REPLY_PENDING)
	{
		check_allowed_reply_type();
		switch (shm_mq_send(submit_mq.sendq_handle,
								submit_mq.sendbufsize,
								submit_mq.sendbuf, true))
		{
			case SHM_MQ_SUCCESS:
				/* Peer might send again or detach */
				submit_mq.state = MQ_WAIT_PEER_MESSAGE;
				break;
			case SHM_MQ_WOULD_BLOCK:
				break;
			case SHM_MQ_DETACHED:
				bdr_detach_submit_shm_mq();
				Assert(submit_mq.state == MQ_WAIT_PEER_DETACH || submit_mq.state == MQ_NEED_REINIT);
				break;
		}
	}

	if (submit_mq.state == MQ_WAIT_PEER_DETACH)
	{
		/*
		 * Peer gone from one or both queues but we couldn't fully clean up
		 * last time around because the peer was still attached. Make sure
		 * local state is gone and try again.
		 */
		bdr_detach_submit_shm_mq();
		Assert(submit_mq.state == MQ_WAIT_PEER_DETACH || submit_mq.state == MQ_NEED_REINIT);
	}

	if (submit_mq.state == MQ_NEED_REINIT)
	{
		bdr_reinit_submit_shm_mq();
		Assert(submit_mq.state == MQ_WAIT_PEER_MESSAGE);
	}
}

static void
bdr_shutdown_submit_shm_mq(void)
{
	bdr_detach_submit_shm_mq();
	if (submit_mq.mq_context != NULL)
	{
		Assert(submit_mq.recvq_handle == NULL);
		Assert(submit_mq.sendq_handle == NULL);
		MemoryContextDelete(submit_mq.mq_context);
		submit_mq.mq_context = NULL;
	}
}

void
bdr_shutdown_consensus(void)
{
    consensus_shutdown();

	if (my_manager != NULL)
	{
		bdr_shutdown_submit_shm_mq();
		my_manager = NULL;
	}
}

static void
bdr_messaging_atexit(int code, Datum argument)
{
	if (is_bdr_manager())
		bdr_shutdown_consensus();
	else
		bdr_detach_manager_queue();
}

bool
bdr_msgs_begin_enqueue(void)
{
	if (is_bdr_manager())
	{
		return consensus_begin_enqueue();
	}
	else
	{
		/*
		 * We're not running in the manager. So we need IPC to get the message to the
		 * consensus manager in the pglogical manager worker.
		 *
		 * The submitting backend could be a bgworker or a normal Pg backend.
		 *
		 * Use the manager's memory queues to relay this message via the manager and
		 * return the response.
		 */
		SubmitMQMessage *msg;
		bdr_attach_manager_queue();
		msg = bdr_submit_manager_queue(MSG_START_ENQUEUE, 0, NULL);
		Assert(msg->msg_type == MSG_START_ENQUEUE_RESULT);
		Assert(msg->payload_length == sizeof(bool));
		return *((bool*)msg->payload);
	}
}

/*
 * Enqueue a message for processing on other peers.
 *
 * Returns a handle that can be used to determine when the final message in the
 * set is finalized and what the outcome was. Use bdr_msg_get_outcome(...)
 * to look up the status.
 */
uint64
bdr_msgs_enqueue(BdrMessage *message)
{
	if (is_bdr_manager())
	{
		/*
		 * No need for IPC to enqueue this message, we're on the manager
		 * already.
		 */
		return consensus_enqueue_proposal((const char*)message, BdrMessageSize(message));
	}
	else
	{
		SubmitMQMessage *msg;
		msg = bdr_submit_manager_queue(MSG_ENQUEUE_PROPOSAL,
									   BdrMessageSize(message), (void*)message);
		Assert(msg->msg_type == MSG_ENQUEUE_RESULT);
		Assert(msg->payload_length == sizeof(uint64));
		return *((uint64*)msg->payload);
	}
}

uint64
bdr_msgs_finish_enqueue(void)
{
	if (is_bdr_manager())
		return consensus_finish_enqueue();
	else
	{
		SubmitMQMessage *msg;
		msg = bdr_submit_manager_queue(MSG_FINISH_ENQUEUE, 0, NULL);
		Assert(msg->msg_type == MSG_FINISH_ENQUEUE_RESULT);
		Assert(msg->payload_length == sizeof(uint64));
		return *((uint64*)msg->payload);
	}
}

/*
 * Given a handle from bdr_proposals_enqueue, look up whether
 * a message is committed or not.
 */
ConsensusProposalStatus
bdr_msg_get_outcome(uint64 msg_handle)
{
	/*
	 * TODO: For now only works in manager worker, see comments
	 * in bdr_consensus.c prelude for how to change that.
	 */
	Assert(is_bdr_manager());

	return consensus_proposals_status(msg_handle);
}

static bool
bdr_proposals_receive(ConsensusProposal *msg)
{
	BdrMessage *bmsg;
	/* note, we receive our own messages too */
	elog(LOG, "XXX RECEIVE FROM %u, my id is %u, payload size %lu",
		msg->sender_nodeid, bdr_get_local_nodeid(), msg->payload_length);

	Assert(msg->payload_length >= sizeof(BdrMessage));
	bmsg = (BdrMessage*)msg->payload;
	Assert(msg->payload_length == sizeof(BdrMessage) + bmsg->payload_length);

	if (bmsg->message_type == BDR_MSG_COMMENT)
	{
		elog(bdr_debug_level, "BDR comment msg from %u: \"%s\"",
			 msg->sender_nodeid, bmsg->payload);
	}

    /* TODO: can nack messages here */
    return true;
}

static bool
bdr_proposals_prepare(List *messages)
{
	ListCell *lc;

	elog(LOG, "XXX PREPARE"); /* TODO */

	foreach (lc, messages)
	{
		BdrMessage *msg = lfirst(lc);
		/*
		 * TODO: should dispatch message processing via the local node state
		 * machine, but for now we'll do it directly here
		 */
		switch (msg->message_type)
		{
			case BDR_MSG_COMMENT:
				break;
			case BDR_MSG_NODE_JOIN_REQUEST:
				bdr_join_handle_join_proposal(msg);
				break;
			default:
				elog(ERROR, "unhandled message type %u in prepare proposal",
					 msg->message_type);
		}
	}

    /* TODO: here's where we apply in-transaction state changes like insert nodes */

    /* TODO: can nack messages here too */
    return true;
}

static void
bdr_proposals_commit(List *messages)
{
	elog(LOG, "XXX COMMIT"); /* TODO */

    /* TODO: here's where we allow state changes to take effect */
}

/* TODO add message-id ranges rejected */
static void
bdr_proposals_rollback(void)
{
	elog(LOG, "XXX ROLLBACK"); /* TODO */

    /* TODO: here's where we wind back any temporary state changes */
}

void
bdr_messaging_wait_event(struct WaitEvent *events, int nevents,
						 long *max_next_wait_ms)
{
	/*
	 * This is a good chance for us to check if we've had any new messages
	 * submitted to us for processing.
	 */
	bdr_service_submit_shmem_mq();

	/*
	 * Now process any consensus messages, and do any underlying
	 * message broker communication required.
	 */
	consensus_pump(events, nevents, max_next_wait_ms);

}

void
bdr_messaging_wait_event_set_recreated(struct WaitEventSet *new_set)
{
	if (!bdr_is_active_db())
		return;

	msgb_wait_event_set_recreated(new_set);
}

static void
bdr_request_recreate_wait_event_set_hook(WaitEventSet *old_set)
{
	pglogical_manager_recreate_wait_event_set();
}

int
bdr_get_wait_event_space_needed(void)
{
	return msgb_get_wait_event_space_needed();
}
