/*-------------------------------------------------------------------------
 *
 * bdr_catcache.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_catcache.c
 *
 * BDR catalog caches and cache invalidaiton routines
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"

#include "utils/memutils.h"

#include "pglogical_node.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_worker.h"

static BdrNode			   *local_bdr_node;
static MemoryContext		bdr_catcache_context;

/*
 * Look up our local node information in the BDR catalogs for
 * this database and cache it in a global.
 *
 * No invalidations are handled. If the local node info changes the
 * managers and workers must be signalled to restart.
 *
 * Otherwise we'd need an xact whenever we looked up these caches, so we could
 * handle possibly reloading them.
 */
void
bdr_cache_local_nodeinfo(void)
{
	MemoryContext old_ctx;

	if (bdr_catcache_context == NULL)
	{
		bdr_catcache_context = AllocSetContextCreate(CacheMemoryContext,
											  "bdr_catcache",
											  ALLOCSET_DEFAULT_SIZES);
	}
	else
	{
		elog(bdr_debug_level, "BDR re-initing catcache");
		local_bdr_node = NULL;
		MemoryContextReset(bdr_catcache_context);
	}

	old_ctx = MemoryContextSwitchTo(bdr_catcache_context);

	local_bdr_node = bdr_get_local_node(true);

	(void) MemoryContextSwitchTo(old_ctx);
}

uint32
bdr_get_local_nodeid(void)
{
	if (local_bdr_node != NULL)
		return local_bdr_node->node_id;
	else
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("attempted to use BDR catalog cache when bdr is not active")));
}
