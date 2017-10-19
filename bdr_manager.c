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

#include "commands/dbcommands.h"

#include "miscadmin.h"

#include "replication/slot.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "miscadmin.h"

#include "utils/memutils.h"

#include "bdr_join.h"
#include "bdr_state.h"
#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_manager.h"
#include "bdr_shmem.h"
#include "bdr_worker.h"
#include "bdr_wal_messaging.h"

int bdr_max_nodes;

static void bdr_manager_atexit(int code, Datum argument);

static BdrManagerShmem *my_manager = NULL;

static bool atexit_registered = false;

/*
 * So we can start BDR without restarting the manager we need to
 * remember if BDR catalogs were previously found.
 */
static bool bdr_is_active_in_manager = false;

static void report_failure_reason(void);

/*
 * This hook runs when pglogical's manager worker starts. It brings up the BDR
 * subsystems needed to do inter-node state management.
 *
 * It can also be called via bdr_manager_check_startup_needed from pglogical's
 * wait-event hook, so avoid making too many assumptions about the pglogical
 * manager's state.
 */
void
bdr_manager_worker_start(void)
{
	BdrStateEntry	cur_state;
	char		   *dbname;

	/* We need at least one worker process and one replixation set per node. */
	bdr_max_nodes = Min(max_worker_processes, max_replication_slots);
	if (!bdr_is_active_db())
	{
		elog(bdr_debug_level, "BDR not configured on db %u", MyDatabaseId);
		return;
	}

	StartTransactionCommand();
	state_get_last(&cur_state, false, false);
	dbname = get_database_name(MyDatabaseId);
	CommitTransactionCommand();
	if (cur_state.current == BDR_NODE_STATE_JOIN_FAILED)
		report_failure_reason();

	elog(bdr_debug_level, "configuring BDR manager on node %s (%u) in db %s for up to %d nodes (unless other resource limits hit)",
		 bdr_get_local_node_name(), bdr_get_local_nodeid(), dbname, bdr_max_nodes);

	bdr_is_active_in_manager = true;

	if (!atexit_registered)
	{
		before_shmem_exit(bdr_manager_atexit, (Datum)0);
		atexit_registered = true;
	}

	my_manager = bdr_shmem_allocate_manager_segment(bdr_get_local_nodeid());

	bdr_start_consensus(cur_state.current);
	bdr_consensus_refresh_nodes(cur_state.current);
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

	bdr_is_active_in_manager = false;
}

/*
 * If BDR catalogs have just been created (and then we were probably
 * woken on a latch set), we might need to start up BDR support in
 * an existing pglogical manager.
 */
static void
bdr_manager_check_startup_needed(void)
{
	bool			txn_started = false;
	bool			needs_startup = false;
	MemoryContext	old_ctx;

	if (bdr_is_active_in_manager)
		return;

	/*
	 * Probe the BDR catalogs, uncached, to see if a node and nodegroup have
	 * been defined since we last looked.
	 *
	 * No need to worry about leaking memory here, since the txn gives us
	 * a memory context.
	 */
	old_ctx = CurrentMemoryContext;
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		txn_started = true;
	}
	bdr_refresh_cache_local_nodeinfo();
	if (bdr_is_active_db())
	{
		/*
		 * Any cache of nodeinfo is sure to be invalid and we don't have any
		 * sort of invalidation mechanism, so we'd better re-read it.
		 */
		needs_startup = true;
	}
	if (txn_started)
		CommitTransactionCommand();

	/*
	 * Wakey wakey BDR?
	 *
	 * We could possibly force the manager to exit instead and tell the
	 * supervisor to relaunch it, but that could be racey.
	 */
	if (needs_startup)
	{
		(void) MemoryContextSwitchTo(TopMemoryContext);
		bdr_manager_worker_start();
	}

	(void) MemoryContextSwitchTo(old_ctx);
}

/*
 * Intercept pglogical's main loop during wait-event processing
 */
void
bdr_manager_wait_event(struct WaitEvent *events, int nevents,
					   long *max_next_wait_msecs)
{
	bdr_manager_check_startup_needed();

	if (!bdr_is_active_db())
		return;

	mn_consensus_wakeup(events, nevents, max_next_wait_msecs);
	bdr_join_wait_event(events, nevents, max_next_wait_msecs);

	bdr_state_dispatch(max_next_wait_msecs);

	bdr_maybe_write_replay_progress_update();
}

void
bdr_wait_event_set_recreated(struct WaitEventSet *new_set)
{
	bdr_consensus_wait_event_set_recreated(new_set);
	bdr_join_wait_event_set_recreated(new_set);
}

int
bdr_get_wait_event_space_needed(void)
{
	return bdr_join_get_wait_event_space_needed()
		   + bdr_consensus_get_wait_event_space_needed();
}

static void
report_failure_reason(void)
{
	BdrStateEntry cur_state;
	const char * dbname;
	ExtraDataJoinFailure *extra;

	state_get_last(&cur_state, false, true);
	extra = cur_state.extra_data;
	if (!IsTransactionState())
		StartTransactionCommand();
	dbname = get_database_name(MyDatabaseId);
	ereport(ERROR,
			(errmsg("BDR node join previously failed, cannot start BDR"),
			 errdetail("join failed with: %s", extra->reason),
			 errhint("Drop the database '%s', re-create it, and try joining again",
				dbname)));
}
