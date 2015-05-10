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

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/memutils.h"

/* Cache entry. */
typedef struct BDRLocalNodeEntry
{
	/* valid entry */
	bool		in_use;

	/* database oid (this is all we need as we only store local nodes here) */
	Oid			dboid;

	/* node status */
	char		status;

	/* is the node read only? */
	bool		read_only;
} BDRLocalNodeEntry;

typedef struct BdrLocalNodeCacheCtl {
	LWLock			   *lock;
	BDRLocalNodeEntry  *nodes;
} BdrLocalNodeCacheCtl;

static BdrLocalNodeCacheCtl *bdr_node_cache_ctl;
static BDRLocalNodeEntry *bdr_local_node = NULL;

static BDRLocalNodeEntry *bdr_nodecache_lookup(bool create);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static BDRLocalNodeEntry*
bdr_nodecache_lookup(bool create)
{
	int off;
	int free_off = -1;

	if (bdr_local_node != NULL && bdr_local_node->in_use)
		return bdr_local_node;

	for(off = 0; off < bdr_max_databases; off++)
	{
		BDRLocalNodeEntry *node = &bdr_node_cache_ctl->nodes[off];

		if (node->in_use && node->dboid == MyDatabaseId)
		{
			bdr_local_node = node;
			return node;
		}

		if (!node->in_use && free_off == -1)
			free_off = off;
	}

	if (!create)
		return NULL;

	if (free_off != -1)
	{
		BDRLocalNodeEntry  *node = &bdr_node_cache_ctl->nodes[free_off];
		BDRNodeInfo		   *nodeinfo;
		bool				tx_started = false;
		bool				spi_pushed;
		MemoryContext		saved_ctx;

		if (!IsTransactionState())
		{
			tx_started = true;
			StartTransactionCommand();
		}
		spi_pushed = SPI_push_conditional();
		SPI_connect();

		saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
		nodeinfo = bdr_nodes_get_local_info(GetSystemIdentifier(),
											ThisTimeLineID,
											MyDatabaseId);
		MemoryContextSwitchTo(saved_ctx);

		if (nodeinfo == NULL)
			return NULL;

		SPI_finish();
		SPI_pop_conditional(spi_pushed);
		if (tx_started)
			CommitTransactionCommand();

		LWLockAcquire(bdr_node_cache_ctl->lock, LW_EXCLUSIVE);
		node->dboid = nodeinfo->dboid;
		node->in_use = true;
		node->status = nodeinfo->status;
		node->read_only = nodeinfo->read_only;
		bdr_local_node = node;
		LWLockRelease(bdr_node_cache_ctl->lock);

		bdr_bdr_node_free(nodeinfo);

		return node;
	}

	ereport(ERROR,
			(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
			errmsg("Too many databases BDR-enabled for bdr.max_databases"),
			errhint("Increase bdr.max_databases above the current limit of %d", bdr_max_databases)));

}

static size_t
bdr_local_node_cache_shmem_size(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(BdrLocalNodeCacheCtl));
	size = add_size(size, mul_size(sizeof(BDRLocalNodeEntry),
								   bdr_max_databases));

	return size;
}

static void
bdr_local_node_cache_shmem_startup(void)
{
	bool        found;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	bdr_node_cache_ctl = ShmemInitStruct("bdr_local_node_cache",
	 									 bdr_local_node_cache_shmem_size(),
										 &found);
	if (!found)
	{
		memset(bdr_node_cache_ctl, 0, bdr_local_node_cache_shmem_size());
		bdr_node_cache_ctl->lock = LWLockAssign();
		bdr_node_cache_ctl->nodes = (BDRLocalNodeEntry *)
			bdr_node_cache_ctl + sizeof(BdrLocalNodeCacheCtl);
	}
	LWLockRelease(AddinShmemInitLock);
}

/* Needs to be called from a shared_preload_library _PG_init() */
void
bdr_local_node_cache_shmem_init(void)
{
	/* Must be called from postmaster its self */
	Assert(IsPostmasterEnvironment && !IsUnderPostmaster);

	bdr_node_cache_ctl = NULL;

	RequestAddinShmemSpace(bdr_local_node_cache_shmem_size());
	RequestAddinLWLocks(1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = bdr_local_node_cache_shmem_startup;
}


void
bdr_local_node_cache_invalidate(void)
{
	int off;

	LWLockAcquire(bdr_node_cache_ctl->lock, LW_EXCLUSIVE);

	for(off = 0; off < bdr_max_databases; off++)
	{
		BDRLocalNodeEntry *node = &bdr_node_cache_ctl->nodes[off];
		node->in_use = false;
	}

	LWLockRelease(bdr_node_cache_ctl->lock);
}

bool
bdr_local_node_read_only(void)
{
	BDRLocalNodeEntry  *node = bdr_nodecache_lookup(true);

	if (node == NULL)
		return false;

	return node->read_only;
}

char
bdr_local_node_status(void)
{
	BDRLocalNodeEntry  *node = bdr_nodecache_lookup(true);

	if (node == NULL)
		return '\0';

	return node->status;
}
