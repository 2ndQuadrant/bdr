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

#include "replication/slot.h"

#include "miscadmin.h"

#include "bdr_manager.h"

int bdr_max_nodes;

void
bdr_manager_worker_start(void)
{
	bdr_max_nodes = Min(max_worker_processes, max_replication_slots);
	elog(INFO, "configuring BDR for up to %d nodes (unless other resource limits hit)",
		 bdr_max_nodes);
}
