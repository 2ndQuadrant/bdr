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
	NameData	node_group_name;
} NodeGroupTuple;

#define Natts_node_group		2
#define Anum_node_group_id		1
#define Anum_node_group_name	2

static BdrNodeGroup *
bdr_nodegroup_fromtuple(HeapTuple tuple)
{
	NodeGroupTuple *nodegtup = (NodeGroupTuple *) GETSTRUCT(tuple);
	BdrNodeGroup *nodegroup = palloc(sizeof(BdrNodeGroup));
	nodegroup->id = nodegtup->node_group_id;
	nodegroup->name = pstrdup(NameStr(nodegtup->node_group_name));
	return nodegroup;
}

BdrNodeGroup *
bdr_get_nodegroup(Oid node_group_id, bool missing_ok)
{
	BdrNodeGroup   *nodegroup = NULL;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE_GROUP, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_node_group_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(node_group_id));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple) && !missing_ok)
		elog(ERROR, "node group %u not found", node_group_id);

	if (HeapTupleIsValid(tuple))
		nodegroup = bdr_nodegroup_fromtuple(tuple);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return nodegroup;
}

BdrNodeGroup *
bdr_get_nodegroup_by_name(const char *name,
						  bool missing_ok)
{
	BdrNodeGroup   *nodegroup = NULL;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE_GROUP, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_node_group_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "node group %s not found", name);
	}

	nodegroup = bdr_nodegroup_fromtuple(tuple);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return nodegroup;
}

Oid
bdr_nodegroup_create(BdrNodeGroup *nodegroup)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_node_group];
	bool		nulls[Natts_node_group];

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE_GROUP, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/*
	 * Generate nodegroup Id if non specified.
	 *
	 * It doesn't matter that this hash could change between Pg versions, since
	 * we don't rely on any consistent mapping of name=>id.
	 */
	if (nodegroup->id == InvalidOid)
		nodegroup->id =
			DatumGetUInt32(hash_any((const unsigned char *) nodegroup->name,
									strlen(nodegroup->name)));

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));
	values[Anum_node_group_id - 1] = ObjectIdGetDatum(nodegroup->id);
	values[Anum_node_group_name - 1] = CStringGetTextDatum(nodegroup->name);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, NoLock);

	CommandCounterIncrement();

	return nodegroup->id;
}

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

BdrNode *
bdr_get_node(Oid nodeid, bool missing_ok)
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

	return node;
}

/*
 * Load the info for specific node by node-id (the same
 * node-id as the pglogical node).
 */
BdrNodeInfo *
bdr_get_node_info(Oid nodeid, bool missing_ok)
{
	BdrNode *node = bdr_get_node(nodeid, missing_ok);
	BdrNodeInfo *nodeinfo = NULL;
	if (node != NULL)
	{
		nodeinfo = palloc(sizeof(BdrNodeInfo));
		nodeinfo->bdr_node = node;
		nodeinfo->pgl_node = get_node(nodeid, missing_ok);
		nodeinfo->bdr_node_group = nodeinfo->bdr_node == NULL ? NULL :
								   bdr_get_nodegroup(nodeinfo->bdr_node->node_group_id, missing_ok);
		nodeinfo->pgl_interface = NULL;
	}
	return nodeinfo;
}

/*
 * Load the info for the local node
 *
 * If missing_ok, this can actually return a non-null BdrNodeInfo
 * with a null BdrNode entry and the pglogical node info populated.
 *
 * So take care.
 */
BdrNodeInfo *
bdr_get_local_node_info(bool missing_ok)
{
	BdrNodeInfo *nodeinfo = NULL;
	PGLogicalLocalNode *local_pgl_node = get_local_node(false, missing_ok);
	if (local_pgl_node != NULL)
	{
		nodeinfo = palloc(sizeof(BdrNodeInfo));
		nodeinfo->bdr_node = bdr_get_node(local_pgl_node->node->id,
										  missing_ok);
		nodeinfo->bdr_node_group = nodeinfo->bdr_node == NULL ? NULL : 
								   bdr_get_nodegroup(nodeinfo->bdr_node->node_group_id, false);
		nodeinfo->pgl_node = local_pgl_node->node;
		nodeinfo->pgl_interface = local_pgl_node->node_if;
		pfree(local_pgl_node);
	}
	return nodeinfo;
}

/*
 * Get the BDR node info for a node by its pglogical
 * node name.
 */
BdrNodeInfo *
bdr_get_node_info_by_name(const char *name, bool missing_ok)
{
	BdrNodeInfo *nodeinfo = NULL;
	PGLogicalNode  *pglnode = get_node_by_name(name, missing_ok);
	if (pglnode != NULL)
	{
		nodeinfo = palloc(sizeof(BdrNodeInfo));
		nodeinfo->bdr_node = bdr_get_node(pglnode->id,
										  missing_ok);
		nodeinfo->bdr_node_group = nodeinfo->bdr_node == NULL ? NULL : 
								   bdr_get_nodegroup(nodeinfo->bdr_node->node_group_id, false);
		nodeinfo->pgl_node = pglnode;
		nodeinfo->pgl_interface = NULL;
		pfree(pglnode);
	}

	return nodeinfo;
}

/*
 * Create the bdr node catalog object ONLY.
 *
 * Can be invoked directly by SQL function calls if we're creating a new node
 * on behalf of the user. In that case the consensus system isn't up yet so we
 * can't do the creation via a consensus message submission. Called via
 * bdr_create_node_defaults(...).
 *
 * When a peer node is joining, this is instead invoked in response to
 * a consensus message.
 *
 * The supplied BdrNode should have a populated pgl_node and pgl_interface;
 * these are to be created by a separate (and prior) call to
 * create_node_defaults(...) in the same transaction. The node_id must
 * be the same as that of the underlying pglogical node.
 */
void
bdr_node_create(BdrNode *node)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_node];
	bool		nulls[Natts_node];

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));
	values[Anum_node_pglogical_node_id - 1] = ObjectIdGetDatum(node->node_id);
	values[Anum_node_node_group_id - 1] = ObjectIdGetDatum(node->node_group_id);
	values[Anum_node_local_state - 1] = ObjectIdGetDatum(node->local_state);
	values[Anum_node_seq_id - 1] = Int32GetDatum(node->seq_id);
	values[Anum_node_confirmed_our_join - 1] = BoolGetDatum(node->confirmed_our_join);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, NoLock);

	CommandCounterIncrement();

	/*
	 * TODO: this should call hooks to notify BDR/pglogical
	 */
}

/*
 * Make a BDR node and the underlying pglogical node.
 *
 * If a local pglogical node already exists it must have the same name
 * as supplied here.
 *
 * TODO: most of this should move into the SQL func
 */
void
bdr_create_node_defaults(uint32 nodegroup_id, const char *node_name, const char *local_dsn)
{
	BdrNode bnode;
	PGLogicalLocalNode *pgllocal = get_local_node(false, true);

	/* Validate that nodegroup exists locally */
	(void) bdr_get_nodegroup(nodegroup_id, false);

	/* Ensure local pglogical node exists and has matching characteristics */
	if (pgllocal == NULL)
	{
		/* no local pglogical node, make one */
		if (node_name == NULL || local_dsn == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("node name and connection string must be specified when no local pglogical node already exists")));
		create_node_defaults((char*)node_name, (char*)local_dsn);
		pgllocal = get_local_node(false, false);
	}
	else if (node_name != NULL && strcmp(pgllocal->node->name, node_name) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("A local pglogical node with name %s exists, cannot create node with name %s",
						pgllocal->node->name, node_name)));
	}
	else if (local_dsn != NULL && strcmp(pgllocal->node_if->dsn, local_dsn) != 0)
	{
		ereport(WARNING,
				(errmsg("connection string for existing local node does not match supplied connstring"),
				 errhint("Check the connection string for the local pglogical interface after node creation")));
	}

	/* then make the BDR node on top */
	bnode.node_id = pgllocal->node->id;
	bnode.node_group_id = nodegroup_id;
	/* To be assigned later */
	bnode.seq_id = -1;
	bnode.confirmed_our_join = false;

	bdr_node_create(&bnode);
}

/*
 * Get a list of all BDR nodes (including parted and pending nodes)
 * as BdrNodeInfo, allocated in the current memory context.
 */
List *
bdr_get_nodes_info(void)
{
	BdrNodeInfo	   *nodeinfo;
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
		nodeinfo = palloc(sizeof(BdrNodeInfo));
		nodeinfo->bdr_node = bdr_node_fromtuple(tuple);
		nodeinfo->bdr_node_group = bdr_get_nodegroup(nodeinfo->bdr_node->node_group_id, false);
		nodeinfo->pgl_node = get_node(nodeinfo->bdr_node->node_id, false);
		nodeinfo->pgl_interface = NULL;

		res = lappend(res, nodeinfo);
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
