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

#include "replication/slot.h"

#include "storage/ipc.h"

#include "miscadmin.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_messaging.h"
#include "bdr_manager.h"

int bdr_max_nodes;

static void bdr_manager_atexit(int code, Datum argument);

/*
 * This hook runs when pglogical's manager worker starts. It brings up the BDR
 * subsystems needed to
 */
void
bdr_manager_worker_start(void)
{
	bdr_max_nodes = Min(max_worker_processes, max_replication_slots);
	elog(INFO, "configuring BDR for up to %d nodes (unless other resource limits hit)",
		 bdr_max_nodes);

	StartTransactionCommand();
	bdr_cache_local_nodeinfo();
	CommitTransactionCommand();

	on_proc_exit(bdr_manager_atexit, (Datum)0);

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
}
