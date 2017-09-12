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

void
bdr_refresh_cache_local_nodeinfo(void)
{
	MemoryContext old_ctx;

	if (bdr_catcache_context != NULL)
		MemoryContextReset(bdr_catcache_context);
	else
		bdr_catcache_context = AllocSetContextCreate(CacheMemoryContext,
													 "bdr_catcache",
													 ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(bdr_catcache_context);

	local_bdr_node_info = bdr_get_local_node_info(false, true);

	(void) MemoryContextSwitchTo(old_ctx);
}

/*
 * Look up our local node information in the BDR catalogs for
 * this database and cache it in a global.
 *
 * No invalidations are handled. If the local node info changes the managers
 * and workers must be signalled to restart, or a manual refresh may be
 * requested with bdr_refresh_cache_local_nodeinfo.  (This renders any
 * references to old cache entries invalid, though).
 *
 * Otherwise we'd need an xact whenever we looked up these caches, so we could
 * handle possibly reloading them.
 *
 * TODO: invalidations/resets
 */
void
bdr_cache_local_nodeinfo()
{
	if (bdr_catcache_context != NULL)
		return;

	bdr_refresh_cache_local_nodeinfo();
}

bool
bdr_catcache_initialised(void)
{
	return bdr_catcache_context != NULL;
}

BdrNodeInfo*
bdr_get_cached_local_node_info(void)
{
	if (local_bdr_node_info == NULL)
	{
		Assert(false); /* Crash here in CASSERT builds */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("attempted to use BDR catalog cache when bdr is not active")));
	}
	return local_bdr_node_info;
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

/*
 * Get the local node id, and ERROR if none is found.
 *
 * Doesn't care if the node is a member of a nodegroup.
 */
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

/*
 * Get the local node id, or 0 if none is found.
 *
 * Doesn't care if the node is a member of a nodegroup.
 */
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

/*
 * Get the local BDR node-group id if the current node is a member of
 * a nodegroup. Otherwise ERROR, or on missing_ok, return 0.
 */
uint32
bdr_get_local_nodegroup_id(bool missing_ok)
{
	Assert(bdr_catcache_initialised());

	if (local_bdr_node_info != NULL)
	{
		const BdrNodeGroup * const ng = local_bdr_node_info->bdr_node_group;
		const BdrNode * const n = local_bdr_node_info->bdr_node;

		if (ng != NULL && n != NULL && n->node_group_id == ng->id)
			return local_bdr_node_info->bdr_node_group->id;
	}

	if (!missing_ok)
	{
		Assert(false); /* Crash here in CASSERT builds */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("attempted to use BDR catalog cache when bdr is not active")));
	}

	return 0;
}

/*
 * Test if BDR is active. Catcache must be inited first.
 *
 * This tests if we have a local BDR node, and that node is a member
 * of a nodegroup.
 */
bool
bdr_is_active_db(void)
{
	return bdr_get_local_nodeid_if_exists() != 0
	       && bdr_get_local_nodegroup_id(true) != 0;
}
