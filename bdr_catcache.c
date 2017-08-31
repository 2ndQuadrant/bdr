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

static BdrNodeInfo 		   *local_bdr_node_info;
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
 *
 * TODO: invalidations/resets
 */
void
bdr_cache_local_nodeinfo(void)
{
	MemoryContext old_ctx;

	if (bdr_catcache_context != NULL)
		return;

	bdr_catcache_context = AllocSetContextCreate(CacheMemoryContext,
												 "bdr_catcache",
												 ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(bdr_catcache_context);

	local_bdr_node_info = bdr_get_local_node_info(false, true);

	(void) MemoryContextSwitchTo(old_ctx);
}

bool
bdr_catcache_initialised(void)
{
	return bdr_catcache_context != NULL;
}

const char *
bdr_get_local_node_name(void)
{
	Assert(bdr_catcache_initialised());
	if (local_bdr_node_info != NULL && local_bdr_node_info->bdr_node != NULL)
		return local_bdr_node_info->pgl_node->name;
	else
	{
		Assert(false); /* Crash here in CASSERT builds */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("attempted to use BDR catalog cache when bdr is not active")));
	}
}

uint32
bdr_get_local_nodeid(void)
{
	Assert(bdr_catcache_initialised());
	if (local_bdr_node_info != NULL && local_bdr_node_info->bdr_node != NULL)
		return local_bdr_node_info->bdr_node->node_id;
	else
	{
		Assert(false); /* Crash here in CASSERT builds */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("attempted to use BDR catalog cache when bdr is not active")));
	}
}

uint32
bdr_get_local_nodeid_if_exists(void)
{
	if (!bdr_catcache_initialised())
	{
		if (IsTransactionState())
			bdr_cache_local_nodeinfo();
		else
			elog(ERROR, "attempted to use BDR catalog cache outside transaction without prior init");
	}
	Assert(bdr_catcache_initialised());
	if (local_bdr_node_info != NULL && local_bdr_node_info->bdr_node != NULL)
		return local_bdr_node_info->bdr_node->node_id;
	else
		return 0;
}
