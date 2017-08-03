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

#include "miscadmin.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_consensus.h"
#include "bdr_manager.h"

int bdr_max_nodes;

void
bdr_manager_worker_start(void)
{
	bdr_max_nodes = Min(max_worker_processes, max_replication_slots);
	elog(INFO, "configuring BDR for up to %d nodes (unless other resource limits hit)",
		 bdr_max_nodes);

	StartTransactionCommand();
	bdr_cache_local_nodeinfo();
	CommitTransactionCommand();

	consensus_startup(bdr_get_local_nodeid(), BDR_SCHEMA_NAME,
					  BDR_MSGJOURNAL_REL_NAME, bdr_max_nodes);
}
