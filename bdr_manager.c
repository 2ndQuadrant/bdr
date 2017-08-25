/*-------------------------------------------------------------------------
 *
 * bdr_manager.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_manager.c
 *
 * BDR support integration for the pglogical manager process
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"

#include "miscadmin.h"

#include "replication/slot.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "miscadmin.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_messaging.h"
#include "bdr_manager.h"
#include "bdr_shmem.h"
#include "bdr_worker.h"

int bdr_max_nodes;

static void bdr_manager_atexit(int code, Datum argument);

static BdrManagerShmem *my_manager = NULL;

static bool atexit_registered = false;

/*
 * This hook runs when pglogical's manager worker starts. It brings up the BDR
 * subsystems needed to do inter-node state management.
 */
void
bdr_manager_worker_start(void)
{
	bdr_max_nodes = Min(max_worker_processes, max_replication_slots);
	if (!bdr_is_active_db())
	{
		elog(bdr_debug_level, "BDR not configured on db %u", MyDatabaseId);
		return;
	}

	elog(bdr_debug_level, "configuring BDR for up to %d nodes (unless other resource limits hit)",
		 bdr_max_nodes);

	if (!atexit_registered)
	{
		before_shmem_exit(bdr_manager_atexit, (Datum)0);
		atexit_registered = true;
	}

	my_manager = bdr_shmem_allocate_manager_segment(bdr_get_local_nodeid());

	bdr_start_consensus(bdr_max_nodes);

	/*
	 * TODO: should cross-check known nodes with subscriptions and ensure we have
	 * subs for all nodes.
	 */
}

static void
bdr_manager_atexit(int code, Datum argument)
{
	bdr_shutdown_consensus();
	if (my_manager != NULL)
	{
		bdr_shmem_release_manager_segment(my_manager);
		my_manager = NULL;
	}
}

/*
 * Intercept pglogical's main loop during wait-event processing
 */
void
bdr_manager_wait_event(struct WaitEvent *events, int nevents,
					   long *max_next_wait_msecs)
{
	BdrMessage *msg;
	const char *dummy_payload;
	Size dummy_payload_length;
	uint64 handle;

	static bool iteration_counter = 0;

	if (!bdr_is_active_db())
		return;

	bdr_messaging_wait_event(events, nevents, max_next_wait_msecs);

	/*
	 * For testing purposes, enqueue some messages here and expect to receive
	 * them through the receive/prepare/commit process.
	 *
	 * Submit messages only intermittently.
	 */
	if ((iteration_counter ++) % 5 == 0)
	{
		dummy_payload = "dummy payload";
		dummy_payload_length = strlen(dummy_payload);

		msg = palloc(offsetof(BdrMessage,payload) + dummy_payload_length);
		msg->message_type = BDR_MSG_NOOP;
		msg->payload_length = dummy_payload_length;
		memcpy(msg->payload, dummy_payload, dummy_payload_length);

		handle = bdr_msgs_enqueue_one(msg);
		if (handle == 0)
			elog(WARNING, "XXX manager couldn't enqueue message, will try again later"); /* XXX */
		else
			elog(WARNING, "XXX manager enqueued message with handle "UINT64_FORMAT, handle); /* XXX */
	}
}
