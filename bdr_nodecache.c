/* -------------------------------------------------------------------------
 *
 * bdr_nodecache.c
 *		shmem cache for local node entry in bdr_nodes, holds one entry per
 *		each local bdr database
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_nodecache.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "bdr.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"

static HTAB *BDRNodeCacheHash = NULL;

/*
 * Because PostgreSQL does not have enought relation lookup functions.
 */
static Oid
bdr_get_relname_relid(const char *nspname, const char *relname)
{
	Oid			nspid;
	Oid			relid;

	nspid = get_namespace_oid(nspname, false);
	relid = get_relname_relid(relname, nspid);

	if (!relid)
		elog(ERROR, "cache lookup failed for relation %s.%s",
			 nspname, relname);

	return relid;
}

/*
 * Send cache invalidation singal to all backends.
 */
void
bdr_nodecache_invalidate(void)
{
	CacheInvalidateRelcacheByRelid(bdr_get_relname_relid("bdr", "bdr_nodes"));
}

/*
 * Invalidate the session local cache.
 */
static void
bdr_nodecache_invalidate_callback(Datum arg, Oid relid)
{
	if (BDRNodeCacheHash == NULL)
		return;

	if (relid == InvalidOid ||
		relid == BdrNodesRelid)
	{
		HASH_SEQ_STATUS status;
		BDRNodeInfo	   *entry;

		hash_seq_init(&status, BDRNodeCacheHash);

		/* We currently always invalidate everything */
		while ((entry = (BDRNodeInfo *) hash_seq_search(&status)) != NULL)
		{
			entry->valid = false;
		}
	}
}

static void
bdr_nodecache_initialize()
{
	HASHCTL		ctl;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(BDRNodeId);
	ctl.entrysize = sizeof(BDRNodeInfo);
	ctl.hash = tag_hash;
	ctl.hcxt = CacheMemoryContext;

	BDRNodeCacheHash = hash_create("BDR node cache", 128, &ctl,
								   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	/*
	 * Watch for invalidation events.
	 * XXX: This breaks if the table is dropped and recreated, during the
	 * lifetime of this backend.
	 */
	BdrNodesRelid = bdr_get_relname_relid("bdr", "bdr_nodes");
	CacheRegisterRelcacheCallback(bdr_nodecache_invalidate_callback,
								  (Datum) 0);
}

static BDRNodeInfo*
bdr_nodecache_lookup(BDRNodeId nodeid, bool missing_ok)
{
	BDRNodeInfo	   *entry,
				   *nodeinfo;
	bool			found;
	MemoryContext	saved_ctx;

	/* potentially need to access syscaches */
	Assert(IsTransactionState());

	if (BDRNodeCacheHash == NULL)
		bdr_nodecache_initialize();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(BDRNodeCacheHash, (void *) &nodeid,
						HASH_ENTER, &found);

	if (found && entry->valid)
		return entry;

	/* zero out data part of the entry */
	memset(((char *) entry) + offsetof(BDRNodeInfo, valid),
		   0,
		   sizeof(BDRNodeInfo) - offsetof(BDRNodeInfo, valid));

	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
	nodeinfo = bdr_nodes_get_local_info(nodeid.sysid,
										nodeid.timeline,
										nodeid.dboid);
	MemoryContextSwitchTo(saved_ctx);

	if (nodeinfo == NULL)
	{
		if (!missing_ok)
			elog(ERROR, "could not find node " UINT64_FORMAT ":%u:%u", nodeid.sysid, nodeid.timeline, nodeid.dboid);
		else
			return NULL;
	}

	entry->status = nodeinfo->status;
	if (nodeinfo->local_dsn)
		entry->local_dsn = MemoryContextStrdup(CacheMemoryContext,
											   nodeinfo->local_dsn);
	if (nodeinfo->init_from_dsn)
		entry->init_from_dsn = MemoryContextStrdup(CacheMemoryContext,
												   nodeinfo->init_from_dsn);
	entry->read_only = nodeinfo->read_only;

	if (nodeinfo->name)
		entry->name = MemoryContextStrdup(CacheMemoryContext,
										  nodeinfo->name);

	entry->seq_id = nodeinfo->seq_id;

	entry->valid = true;

	bdr_bdr_node_free(nodeinfo);

	return entry;
}

const char *
bdr_local_node_name(void)
{
	BDRNodeId		nodeid;
	BDRNodeInfo	   *node;

	nodeid.sysid = GetSystemIdentifier();
	nodeid.timeline = ThisTimeLineID;
	nodeid.dboid = MyDatabaseId;
	node = bdr_nodecache_lookup(nodeid, true);

	if (node == NULL)
		return false;

	return node->name;
}

bool
bdr_local_node_read_only(void)
{
	BDRNodeId		nodeid;
	BDRNodeInfo	   *node;

	nodeid.sysid = GetSystemIdentifier();
	nodeid.timeline = ThisTimeLineID;
	nodeid.dboid = MyDatabaseId;
	node = bdr_nodecache_lookup(nodeid, true);

	if (node == NULL)
		return false;

	return node->read_only;
}

char
bdr_local_node_status(void)
{
	BDRNodeId		nodeid;
	BDRNodeInfo	   *node;

	nodeid.sysid = GetSystemIdentifier();
	nodeid.timeline = ThisTimeLineID;
	nodeid.dboid = MyDatabaseId;
	node = bdr_nodecache_lookup(nodeid, true);

	if (node == NULL)
		return '\0';

	return node->status;
}

/*
 * Get 16-bit node sequence ID, or
 * -1 if no node or no sequence assigned.
 */
int32
bdr_local_node_seq_id(void)
{
	BDRNodeId		nodeid;
	BDRNodeInfo	   *node;

	nodeid.sysid = GetSystemIdentifier();
	nodeid.timeline = ThisTimeLineID;
	nodeid.dboid = MyDatabaseId;
	node = bdr_nodecache_lookup(nodeid, true);

	if (node == NULL)
		return -1;

	return node->seq_id;
}
