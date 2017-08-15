/*-------------------------------------------------------------------------
 *
 * bdr_catalogs.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_catalogs.c
 *
 * BDR catalog access and manipulation, for the replication group membership,
 * etc. See bdr_catcache.c for cached lookups etc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "miscadmin.h"

#include "nodes/makefuncs.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "bdr_catalogs.h"

#define CATALOG_NODE			"node"
#define CATALOG_NODE_GROUP		"node_group"

typedef struct NodeTuple
{
	Oid			pglogical_node_id;
	Oid			node_group_id;
	int32		local_state;
	int16		seq_id;
	bool		confirmed_our_join;
} NodeTuple;

#define Natts_node						5
#define Anum_node_pglogical_node_id		1
#define Anum_node_node_group_id			2
#define Anum_node_local_state			3
#define Anum_node_seq_id				4
#define Anum_node_confirmed_our_join	5

typedef struct NodeGroupTuple
{
	Oid			node_group_id;
	Name		node_group_name;
} NodeGroupTuple;

#define Natts_node_group		2
#define Anum_node_group_id		1
#define Anum_node_group_name	2

static BdrNode *
bdr_node_fromtuple(HeapTuple tuple)
{
	NodeTuple *nodetup = (NodeTuple *) GETSTRUCT(tuple);
	BdrNode *node
		= (BdrNode *) palloc(sizeof(BdrNode));
	node->node_id = nodetup->pglogical_node_id;
	node->node_group_id = nodetup->node_group_id;
	node->local_state = nodetup->local_state;
	node->seq_id = nodetup->seq_id;
	node->confirmed_our_join = nodetup->confirmed_our_join;
	/*
	 * Attributes after this could be NULL or varlena and cannot be accessed
	 * via GETSRUCT directly. If we add any we'll need a TupleDesc
	 * argument.
	 */

	return node;
}

static BdrNode *
bdr_get_node_internal(Oid nodeid, bool missing_ok, PGLogicalNode *pglnode)
{
	BdrNode		   *node = NULL;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	/* and the BDR node info */
	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_node_pglogical_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple) && !missing_ok)
		elog(ERROR, "node %u not found", nodeid);

	if (HeapTupleIsValid(tuple))
		node = bdr_node_fromtuple(tuple);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	node->pgl_node = pglnode;

	return node;
}

/*
 * Load the info for specific node by node-id (the same
 * node-id as the pglogical node).
 */
BdrNode *
bdr_get_node(Oid nodeid, bool missing_ok)
{
	return bdr_get_node_internal(nodeid, missing_ok,
								 get_node(nodeid, missing_ok));
}

/*
 * Get the BDR node info for a node by its pglogical
 * node name.
 */
BdrNode *
bdr_get_node_by_name(const char *name, bool missing_ok)
{
	PGLogicalNode  *pglnode = get_node_by_name(name, missing_ok);

	return bdr_get_node_internal(pglnode->id, missing_ok, pglnode);
}

/*
 * Get a list of all BDR nodes (including parted and pending nodes)
 */
List *
bdr_get_nodes(void)
{
	BdrNode		   *node;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	List		   *res = NIL;

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE_GROUP, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		node = bdr_node_fromtuple(tuple);
		node->pgl_node = get_node(node->node_id, false);

		res = lappend(res, node);
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return res;
}

/*
 * Filter the pglogical subscriptions for a node to report only the
 * BDR subscriptions.
 */
List *
bdr_get_node_subscriptions(uint32 node_id)
{
	/*
	 * TODO: filter the subscription list to exclude non-bdr subscriptions.
	 *
	 * We'll need another table like bdr.subscriptions to track this if more
	 * than one plugin is active, but for now we just assume any isinternal
	 * subscription is ours.
	 */
	List *subs = get_node_subscriptions(node_id, false);
	ListCell *lc;
	ListCell *prev = NULL;

	/* Not leak-proof */
	Assert(CurrentMemoryContext != TopMemoryContext);

	foreach (lc, subs)
	{
		PGLogicalSubscription *sub = lfirst(lc);

		if (!sub->isinternal)
		{
			/* We leak the subscription, but we're not in TopMemoryContext */
			list_delete_cell(subs, lc, prev);
		}

		prev = lc;
	}

	return subs;
}
