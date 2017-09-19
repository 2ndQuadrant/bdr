/*-------------------------------------------------------------------------
 *
 * bdr_output.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_output.c
 *
 * Integration into the pglogical output plugin for BDR support
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"

#include "catalog/namespace.h"

#include "nodes/parsenodes.h"

#include "replication/logical.h"

#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pglogical_output_config.h"
#include "pglogical_output_plugin.h"
#include "pglogical_node.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_output.h"
#include "bdr_state.h"
#include "bdr_version.h"
#include "bdr_worker.h"

static BdrSubscriptionMode sub_mode = BDR_SUBSCRIPTION_MODE_NONE;
static BdrNodeInfo *remote;
static BdrNodeInfo *local;

static PGL_decode_origin_filter_hook_type prev_PGL_decode_origin_filter_hook = NULL;
static bool hook_registered = false;
static MemoryContext decoding_ctx = NULL;

typedef struct BdrOriginCacheEntry
{
	RepOriginId origin;
	/* Entry is valid and not due to be purged */
	bool is_valid;
	/* bdr/pgl node id associated with this origin */
	uint32 node_id;
	/* nodegroup id of this node, if it's a BDR node */
	uint32 node_group_id;
} BdrOriginCacheEntry;

#define BDRORIGINCACHE_INITIAL_SIZE 128
static HTAB *BdrOriginCache = NULL;
static int InvalidBdrOriginCacheCnt = 0;

static void bdrorigincache_init(MemoryContext decoding_context);
static BdrOriginCacheEntry *bdrorigincache_get_node(RepOriginId origin);
static void bdrorigincache_destroy(void);
static void bdr_lookup_origin(RepOriginId origin_id, BdrOriginCacheEntry *entry);

/*
 * Invalidation of the origin cache for when an origin is dropped or
 * re-created.
 */
static void
bdrorigincache_invalidation_cb(Datum arg, int cacheid, uint32 origin_id)
 {
	struct BdrOriginCacheEntry *hentry;
	RepOriginId origin = (RepOriginId)origin;

	Assert (BdrOriginCache != NULL);
	Assert (cacheid == REPLORIGIDENT);

	/*
	 * We can't immediately delete entries as invalidations can
	 * arrive while we're in the middle of using one. So we must
	 * mark it invalid and purge it later.
	 */
	hentry = (struct BdrOriginCacheEntry *)
		hash_search(BdrOriginCache, &origin, HASH_FIND, NULL);

	if (hentry != NULL)
	{
		hentry->is_valid = false;
		InvalidBdrOriginCacheCnt++;
	}
}

/*
 * Create a cache mapping replication origins to bdr node IDs
 * and node group IDs, so we can make fast decisions about
 * whether or not to forward a given xact.
 */
static void
bdrorigincache_init(MemoryContext decoding_context)
{
	HASHCTL	ctl;

	InvalidBdrOriginCacheCnt = 0;

	if (BdrOriginCache == NULL)
	{
		MemoryContext old_ctxt;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(struct BdrOriginCacheEntry);
		ctl.hcxt = TopMemoryContext;

		old_ctxt = MemoryContextSwitchTo(TopMemoryContext);
		BdrOriginCache = hash_create("bdr reporigin to node cache",
								   BDRORIGINCACHE_INITIAL_SIZE,
								   &ctl,
								   HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
		(void) MemoryContextSwitchTo(old_ctxt);

		Assert(BdrOriginCache != NULL);

		CacheRegisterSyscacheCallback(REPLORIGIDENT,
			bdrorigincache_invalidation_cb, (Datum)0);
	}
}

/*
 * Look up an entry, creating it if not found.
 *
 * 
 */
static BdrOriginCacheEntry *
bdrorigincache_get_node(RepOriginId origin)
{
	struct BdrOriginCacheEntry *hentry;
	bool found;
	MemoryContext old_mctx;

	/* Find cached function info, creating if not found */
	old_mctx = MemoryContextSwitchTo(TopMemoryContext);
	hentry = (struct BdrOriginCacheEntry*) hash_search(BdrOriginCache,
										 &origin,
										 HASH_ENTER, &found);
	(void) MemoryContextSwitchTo(old_mctx);

	if (!found || !hentry->is_valid)
		bdr_lookup_origin(origin, hentry);

	Assert(hentry != NULL);
	Assert(hentry->is_valid);

	return hentry;
}


/*
 * Flush the bdr origin cache at the end of a decoding session.
 */
static void
bdrorigincache_destroy(void)
{
	HASH_SEQ_STATUS status;
	struct BdrOriginCacheEntry *hentry;

	if (BdrOriginCache != NULL)
	{
		hash_seq_init(&status, BdrOriginCache);

		while ((hentry = (struct BdrOriginCacheEntry*) hash_seq_search(&status)) != NULL)
		{
			if (hash_search(BdrOriginCache,
							(void *) &hentry->origin,
							HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "hash table corrupted");
		}
	}
}

/*
 * We need to filter out transactions from the same node group while retaining
 * transactions forwarded from other peers (pglogical subscriptions, etc) for
 * replication.
 *
 * This leaks memory if called within an existing transaction, but only gets
 * called once per local replication origin entry, so it's probably not worth
 * having a memory context for it.
 */
static void
bdr_lookup_origin(RepOriginId origin_id, BdrOriginCacheEntry *entry)
{
	char *origin_name;
	bool txn_started = false;
	MemoryContext old_ctx;

	entry->node_id = 0;
	entry->node_group_id = 0;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		txn_started = true;
	}
	old_ctx = MemoryContextSwitchTo(decoding_ctx);

	if (replorigin_by_oid(origin_id, true, &origin_name))
	{
		PGLogicalSubscription *sub;

		/*
		 * If it's a PGL/BDR replication origin, we need to look up whether
		 * it's for a BDR node in the same nodegroup. Later on we may support
		 * multiple nodegroups. The replication origin name is the subscription
		 * slot name.
		 *
		 * PGL slot names have a standard format but nothing stops custom
		 * ones being used, plus they can be abbreviated, so we can't
		 * just parse the slot name. If it's a BDR node there must
		 * be a local subscription for this origin and we can use it
		 * to get the nodegroup.
		 */
		sub = get_subscription_by_slot_name(origin_name, true);
		if (sub != NULL)
		{
			/*
			 * It's definitely from pglogical, but is it from BDR?
			 */
			BdrNode *bnode = bdr_get_node(sub->origin->id, true);
			if (bnode != NULL)
			{
				entry->node_id = bnode->node_id;
				entry->node_group_id = bnode->node_group_id;
			}
		}
	}

	(void) MemoryContextSwitchTo(old_ctx);
	if (txn_started)
		CommitTransactionCommand();

	entry->is_valid = true;
}

static bool
bdr_origin_in_same_nodegroup(RepOriginId origin_id)
{
	BdrOriginCacheEntry *entry = bdrorigincache_get_node(origin_id);
	Assert(entry->is_valid);

	return entry->node_group_id == bdr_get_local_nodegroup_id(false);
}

/*
 * Replication origin filter. Returns true to exclude an xact, false
 * to retain it.
 */
static bool
bdr_origin_filter_hook(struct LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	switch (sub_mode)
	{
		case BDR_SUBSCRIPTION_MODE_NORMAL:
			/*
			 * A normal BDR subscription replicates rows from this peer, and
			 * from non-BDR-nodegroup-member origins like pglogical
			 * subscriptions. It need not replicate rows from other BDR peers,
			 * since it knows the peer gets that data directly.
			 */
			if (origin_id == InvalidRepOriginId)
				break;

			if (bdr_origin_in_same_nodegroup(origin_id))
				return true;

		case BDR_SUBSCRIPTION_MODE_CATCHUP:
			/*
			 * A catchup mode subscription forwards rows from everything
			 * unconditionally, trying to remain as close to the same as the
			 * master as possible.
			 *
			 * Replication sets can still exclude changes in the actual change
			 * filters.
			 */
			break;

		case BDR_SUBSCRIPTION_MODE_FASTFORWARD:
			/*
			 * Filter out everything.
			 *
			 * See RM#839
			 *
			 * We possibly don't need this since pgl has the skip_all option.
			 */
			return true;

		case BDR_SUBSCRIPTION_MODE_NONE:
			break;
	}

	if (prev_PGL_decode_origin_filter_hook != NULL)
		return prev_PGL_decode_origin_filter_hook(ctx, origin_id);
	else
		return PGL_standard_decode_origin_filter(ctx, origin_id);
}

void
bdr_output_start(struct LogicalDecodingContext * ctx, struct OutputPluginOptions *opt)
{
	bool txn_started = false;
	MemoryContext old_ctx;

	sub_mode = BDR_SUBSCRIPTION_MODE_NONE;
	decoding_ctx = ctx->context;
	Assert(decoding_ctx != NULL);

	if (peer_bdr_version_num == -1)
		return;

	elog(bdr_debug_level,
		"received connection from bdr node %u nodegroup %u, bdr version %s (%06d)",
		peer_bdr_node_id, peer_bdr_node_group_id,
		peer_bdr_version_str, peer_bdr_version_num);

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		txn_started = true;
	}
	old_ctx = MemoryContextSwitchTo(ctx->context);
	
	bdr_refresh_cache_local_nodeinfo();
	remote = bdr_get_node_info(peer_bdr_node_id, false);

	(void) MemoryContextSwitchTo(old_ctx);
	if (txn_started)
		CommitTransactionCommand();

	local = bdr_get_cached_local_node_info();

	if (local->bdr_node_group->id != peer_bdr_node_group_id)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("connecting node expected this node to be a in a different nodegroup"),
				 errdetail("Local node %u is member of nodegroup %u, but peer %u wants nodegroup %u",
						   local->bdr_node->node_id, local->bdr_node_group->id,
						   peer_bdr_node_id, peer_bdr_node_group_id)));

	if (!hook_registered)
	{
		prev_PGL_decode_origin_filter_hook = PGL_decode_origin_filter_hook;
		PGL_decode_origin_filter_hook = bdr_origin_filter_hook;
		hook_registered = true;
	}

	bdrorigincache_init(ctx->context);
}

/*
 * Process arguments to the output plugin, including those added by
 * bdr_start_replication_params(...)
 */
bool
bdr_process_output_params(struct DefElem *elem)
{
	if (elem == NULL)
	{
		/* end of parameters */
	}
	else if (strcmp(elem->defname, "bdr_version_num") == 0)
	{
		peer_bdr_version_num = PGL_parse_param_uint32(elem);
	}
	else if (strcmp(elem->defname, "bdr_version_num") == 0)
	{
		strncpy(peer_bdr_version_str, strVal(elem), BDR_VERSION_STR_SIZE);
		peer_bdr_version_str[BDR_VERSION_STR_SIZE-1] = '\0';
	}
	else if (strcmp(elem->defname, "bdr_node_id") == 0)
	{
		peer_bdr_node_id = PGL_parse_param_uint32(elem);
	}
	else if (strcmp(elem->defname, "bdr_node_group_id") == 0)
	{
		peer_bdr_node_group_id = PGL_parse_param_uint32(elem);
	}
	else if (strcmp(elem->defname, "bdr_subscription_mode") == 0)
	{
		if (strlen(strVal(elem->arg)) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("bdr_subscription_mode must be a single character, not '%s'",
					 	strVal(elem->arg))));

		sub_mode = (BdrSubscriptionMode)strVal(elem->arg)[0];
	}

	return false;
}

void
bdr_output_shutdown(struct LogicalDecodingContext * ctx)
{
	sub_mode = BDR_SUBSCRIPTION_MODE_NONE;
	bdrorigincache_destroy();
	decoding_ctx = NULL;
}
