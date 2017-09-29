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

#include "libpq/pqformat.h"

#include "miscadmin.h"

#include "nodes/makefuncs.h"

#include "pgtime.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#include "bdr_state.h"
#include "bdr_catalogs.h"
#include "bdr_consensus.h"

#define CATALOG_NODE				"node"
#define CATALOG_NODE_GROUP			"node_group"
#define CATALOG_STATE				"state_journal"
#define CATALOG_STATE_COUNTER_IDX	"state_journal_pkey"
#define CATALOG_SUBSCRIPTION		"subscription"

typedef struct NodeTuple
{
	Oid			pglogical_node_id;
	Oid			node_group_id;
	int32		local_state;
	int32		seq_id;
	bool		confirmed_our_join;
	NameData	dbname;
} NodeTuple;

#define Natts_node						6
#define Anum_node_pglogical_node_id		1
#define Anum_node_node_group_id			2
#define Anum_node_local_state			3
#define Anum_node_seq_id				4
#define Anum_node_confirmed_our_join	5
#define Anum_node_dbname				6

typedef struct NodeGroupTuple
{
	Oid			node_group_id;
	NameData	node_group_name;
	Oid			node_group_default_repset;
} NodeGroupTuple;

#define Natts_node_group		3
#define Anum_node_group_id		1
#define Anum_node_group_name	2
#define Anum_node_group_default_repset	3

typedef struct StateTuple
{
	Oid			counter;
	Oid			current;
	Oid			goal;
	TimestampTz entered_time;
	int64		global_consensus_no;
	Oid			peer_id;
	/* After this point use heap_getattr */
} StateTuple;

#define Natts_state						7
#define Anum_state_counter				1
#define Anum_state_current				2
#define Anum_state_goal					3
#define Anum_state_entered_time			4
#define Anum_state_global_consensus_no	5
#define Anum_state_peer_id		6
#define Anum_state_extra_data			7

typedef struct SubscriptionTuple
{
	Oid			pglogical_subscription_id;
	Oid			nodegroup_id;
	Oid			origin_node_id;
	Oid			target_node_id;
	char		mode;
} SubscriptionTuple;

#define Natts_subscription					5
#define Anum_subscription_pglogical_subscription_id 1
#define Anum_subscription_nodegroup_id		2
#define Anum_subscription_origin_node_id	3
#define Anum_subscription_target_node_id	4
#define Anum_subscription_mode				5

static BdrNodeGroup *
bdr_nodegroup_fromtuple(HeapTuple tuple)
{
	NodeGroupTuple *nodegtup = (NodeGroupTuple *) GETSTRUCT(tuple);
	BdrNodeGroup *nodegroup = palloc(sizeof(BdrNodeGroup));
	nodegroup->id = nodegtup->node_group_id;
	nodegroup->name = pstrdup(NameStr(nodegtup->node_group_name));
	nodegroup->default_repset = nodegtup->node_group_default_repset;
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

	if (node_group_id == 0)
	{
		Assert(false);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node group id may not be 0")));
	}

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

	if (nodegroup->default_repset == InvalidOid)
		elog(ERROR, "nodegroup may not have default repset 0");

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
	values[Anum_node_group_name - 1] = CStringGetDatum(nodegroup->name);
	values[Anum_node_group_default_repset - 1] = ObjectIdGetDatum(nodegroup->default_repset);

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
	node->dbname = pstrdup(NameStr(nodetup->dbname));

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
 * Validate that a nodeinfo meets the constaints the rest of the code expects
 * when working with them.
 */
void
check_nodeinfo(BdrNodeInfo* nodeinfo)
{
	if (nodeinfo == NULL)
		return;

	/* Must be a bdr node if there's a nodegroup */
	if (nodeinfo->bdr_node_group != NULL)
	{
		Assert(nodeinfo->bdr_node != NULL);
		Assert(nodeinfo->bdr_node->node_group_id == nodeinfo->bdr_node_group->id);
	}

	/* Must be a pglogical node if there's a BDR node */
	if (nodeinfo->bdr_node != NULL)
	{
		Assert(nodeinfo->pgl_node != NULL);
		Assert(nodeinfo->pgl_node->id == nodeinfo->bdr_node->node_id);
	}

	/* Must be a pgl interface if there's a pgl node */
	if (nodeinfo->pgl_node != NULL)
	{
		Assert(nodeinfo->pgl_interface != NULL);
		Assert(nodeinfo->pgl_interface->nodeid == nodeinfo->pgl_node->id);
	}
}

/*
 * Load the info for specific node by node-id (the same
 * node-id as the pglogical node).
 *
 * If the node is part of a nodegroup, the nodegroup is loaded too.
 * (We assume a single nodegroup only for now)
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

		nodeinfo->pgl_node = get_node(nodeid, true);
		if (nodeinfo->pgl_node == NULL && !missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("bdr %u doesn't have corresponding pglogical node with same id",
				  		nodeinfo->bdr_node->node_id)));

		nodeinfo->bdr_node_group = NULL;
		if (nodeinfo->bdr_node != NULL && nodeinfo->bdr_node->node_group_id != 0)
			nodeinfo->bdr_node_group = bdr_get_nodegroup(nodeinfo->bdr_node->node_group_id, false);

		/*
		 * TODO: bdr currently assumes that there's an interface by the same name
		 * as the node, and we can use it. That's a bit hacky, though we enforce
		 * it in group creation and join so it should be OK.
		 */
		nodeinfo->pgl_interface = get_node_interface_by_name(nodeinfo->pgl_node->id, nodeinfo->pgl_node->name, true);
		if (nodeinfo->pgl_interface == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("node %s doesn't have corresponding interface with same name",
						 nodeinfo->pgl_node->name)));
	}
	check_nodeinfo(nodeinfo);
	return nodeinfo;
}

/*
 * Load the info for the local node
 *
 * If missing_ok, this can actually return a non-null BdrNodeInfo
 * with a null BdrNode entry and the pglogical node info populated.
 *
 * So take care.
 *
 * The nodegroup gets loaded if the local node is a member of one.
 *
 * The pglogical local_node is used as an interlock like in pglogical its self.
 */
BdrNodeInfo *
bdr_get_local_node_info(bool for_update, bool missing_ok)
{
	BdrNodeInfo *nodeinfo = NULL;
	PGLogicalLocalNode *local_pgl_node = get_local_node(for_update, true);
	if (local_pgl_node == NULL && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("local node not found")));

	if (local_pgl_node != NULL)
	{
		nodeinfo = palloc(sizeof(BdrNodeInfo));
		nodeinfo->bdr_node = bdr_get_node(local_pgl_node->node->id,
										  missing_ok);
		if (nodeinfo->bdr_node != NULL && nodeinfo->bdr_node->node_group_id != 0)
			nodeinfo->bdr_node_group = bdr_get_nodegroup(nodeinfo->bdr_node->node_group_id, true);
		else
			nodeinfo->bdr_node_group = NULL;
		nodeinfo->pgl_node = local_pgl_node->node;
		nodeinfo->pgl_interface = local_pgl_node->node_if;

		if (strcmp(nodeinfo->pgl_node->name, nodeinfo->pgl_interface->name) != 0)
			ereport(missing_ok ? WARNING: ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("bdr requires that the pglogical interface %s have the same name as the pglogical node %s",
					        nodeinfo->pgl_interface->name, nodeinfo->pgl_node->name)));

		pfree(local_pgl_node);
	}
	check_nodeinfo(nodeinfo);
	return nodeinfo;
}

/*
 * Get the BDR node info for a node by its pglogical
 * node name.
 *
 * If the node is a member of a locally defined nodegroup,
 * load it too.
 */
BdrNodeInfo *
bdr_get_node_info_by_name(const char *name, bool missing_ok)
{
	PGLogicalNode  *pglnode = get_node_by_name(name, missing_ok);
	/* Wastes a pglogical node lookup, but meh */
	if (pglnode == NULL)
		return NULL;
	else
		return bdr_get_node_info(pglnode->id, missing_ok);
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
	values[Anum_node_dbname - 1] = CStringGetDatum(node->dbname);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Update a BDR node tuple
 */
void
bdr_modify_node(BdrNode *node)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	SysScanDesc	scan;
	HeapTuple	oldtup,
				newtup;
	ScanKeyData	key[1];
	Datum		values[Natts_node];
	bool		nulls[Natts_node];
	bool		replaces[Natts_node];

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Search the catalog. */
	ScanKeyInit(&key[0],
				Anum_node_pglogical_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(node->node_id));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	oldtup = systable_getnext(scan);

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "node %u not found", node->node_id);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));
	memset(replaces, true, sizeof(replaces));

	replaces[Anum_node_pglogical_node_id - 1] = false;

	values[Anum_node_node_group_id - 1] = ObjectIdGetDatum(node->node_group_id);
	values[Anum_node_local_state - 1] = ObjectIdGetDatum(node->local_state);
	values[Anum_node_seq_id - 1] = Int32GetDatum(node->seq_id);
	values[Anum_node_confirmed_our_join - 1] = BoolGetDatum(node->confirmed_our_join);
	/* dbname can change if db is renamed, though currently we won't notice */
	values[Anum_node_dbname - 1] = CStringGetDatum(node->dbname);

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Get a list of all BDR nodes (including parted and pending nodes)
 * as BdrNodeInfo, allocated in the current memory context.
 */
List *
bdr_get_nodes_info(Oid in_group_id)
{
	BdrNodeInfo	   *nodeinfo;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	List		   *res = NIL;

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_NODE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		BdrNode *bnode = bdr_node_fromtuple(tuple);

		if (in_group_id != 0 && bnode->node_group_id != in_group_id)
			continue;

		nodeinfo = palloc(sizeof(BdrNodeInfo));

		nodeinfo->bdr_node = bnode;

		if (nodeinfo->bdr_node->node_group_id == 0)
			nodeinfo->bdr_node_group = NULL;
		else
			nodeinfo->bdr_node_group = bdr_get_nodegroup(nodeinfo->bdr_node->node_group_id, false);

		nodeinfo->pgl_node = get_node(nodeinfo->bdr_node->node_id, true);
		if (nodeinfo->pgl_node == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("bdr %u doesn't have corresponding pglogical node with same id",
						 nodeinfo->bdr_node->node_id)));

		/*
		 * TODO: bdr currently assumes that there's an interface by the same name
		 * as the node, and we can use it. That's a bit hacky, though we enforce
		 * it in group creation and join so it should be OK.
		 */
		nodeinfo->pgl_interface = get_node_interface_by_name(nodeinfo->pgl_node->id, nodeinfo->pgl_node->name, true);
		if (nodeinfo->pgl_interface == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("node %s doesn't have corresponding interface with same name",
						 nodeinfo->pgl_node->name)));

		check_nodeinfo(nodeinfo);
		res = lappend(res, nodeinfo);
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return res;
}

void
bdr_create_bdr_subscription(BdrSubscription *sub)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_subscription];
	bool		nulls[Natts_subscription];

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));
	values[Anum_subscription_pglogical_subscription_id - 1]
		= ObjectIdGetDatum(sub->pglogical_subscription_id);
	values[Anum_subscription_nodegroup_id - 1]
		= ObjectIdGetDatum(sub->nodegroup_id);
	values[Anum_subscription_origin_node_id - 1]
		= ObjectIdGetDatum(sub->origin_node_id);
	values[Anum_subscription_target_node_id - 1]
		= ObjectIdGetDatum(sub->target_node_id);
	values[Anum_subscription_mode - 1]
		= CharGetDatum((char)sub->mode);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

static BdrSubscription*
subscription_fromtuple(HeapTuple tuple)
{
	SubscriptionTuple *stup = (SubscriptionTuple *) GETSTRUCT(tuple);
	BdrSubscription *sub = palloc(sizeof(BdrSubscription));

	sub->pglogical_subscription_id = stup->pglogical_subscription_id;
	sub->nodegroup_id = stup->nodegroup_id;
	sub->origin_node_id = stup->origin_node_id;
	sub->target_node_id = stup->target_node_id;
	sub->mode = (BdrSubscriptionMode)stup->mode;

	return sub;
}

void
bdr_alter_bdr_subscription(BdrSubscription *sub)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	oldtup,
				newtup;
	Datum		values[Natts_subscription];
	bool		nulls[Natts_subscription];
	bool		replaces[Natts_subscription];
	ScanKeyData key[1];
	SysScanDesc	scan;
	BdrSubscription *oldsub;

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_subscription_pglogical_subscription_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sub->pglogical_subscription_id));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	oldtup = systable_getnext(scan);

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "bdr.subscription %u not found",
			sub->pglogical_subscription_id);

	oldsub = subscription_fromtuple(oldtup);
	Assert(oldsub->pglogical_subscription_id == sub->pglogical_subscription_id);
	Assert(oldsub->nodegroup_id == sub->nodegroup_id);
	Assert(oldsub->origin_node_id == sub->origin_node_id);
	Assert(oldsub->target_node_id == sub->target_node_id);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));
	memset(replaces, true, sizeof(replaces));

	replaces[Anum_subscription_pglogical_subscription_id - 1] = false;
	replaces[Anum_subscription_nodegroup_id - 1] = false;
	replaces[Anum_subscription_origin_node_id - 1] = false;
	replaces[Anum_subscription_target_node_id - 1] = false;

	values[Anum_subscription_mode - 1] = CharGetDatum((char)sub->mode);
	replaces[Anum_subscription_mode - 1] = true;

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Get a single subscription by ID.
 */
BdrSubscription*
bdr_get_subscription(uint32 pglogical_subscription_id, bool missing_ok)
{
	RangeVar	   *rv;
	Relation		rel;
	HeapTuple		tuple;
	SysScanDesc		scan;
	ScanKeyData		key[1];
	BdrSubscription*ret = NULL;

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_subscription_pglogical_subscription_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(pglogical_subscription_id));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);
	
	if (!HeapTupleIsValid(tuple) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("BDR subscription %u not found",
				 		pglogical_subscription_id)));

	if (HeapTupleIsValid(tuple))
		ret = subscription_fromtuple(tuple);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return ret;
}

BdrSubscription*
bdr_get_node_subscription(uint32 target_node_id, uint32 origin_node_id,
	uint32 nodegroup_id, bool missing_ok)
{
	RangeVar	   *rv;
	Relation		rel;
	HeapTuple		tuple;
	SysScanDesc		scan;
	ScanKeyData		key[3];
	BdrSubscription*ret = NULL;

	/* Not leak-proof */
	Assert(CurrentMemoryContext != TopMemoryContext);

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_subscription_nodegroup_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodegroup_id));
	ScanKeyInit(&key[1],
				Anum_subscription_origin_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(origin_node_id));
	ScanKeyInit(&key[2],
				Anum_subscription_target_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target_node_id));

	scan = systable_beginscan(rel, 0, true, NULL, 3, key);
	tuple = systable_getnext(scan);
	
	if (!HeapTupleIsValid(tuple) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("subscription for %u->%u in nodegroup %u not found",
				 		target_node_id, origin_node_id, nodegroup_id)));

	if (HeapTupleIsValid(tuple))
		ret = subscription_fromtuple(tuple);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return ret;
}

/*
 * Filter the pglogical subscriptions for a node to report only the
 * BDR subscriptions. Returns a List of BdrSubscription.
 */
List *
bdr_get_node_subscriptions(uint32 target_node_id)
{
	RangeVar	   *rv;
	Relation		rel;
	HeapTuple		tuple;
	List		   *ret = NIL;
	SysScanDesc		scan;
	ScanKeyData		key[1];

	/* Not leak-proof */
	Assert(CurrentMemoryContext != TopMemoryContext);

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_subscription_target_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target_node_id));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		BdrSubscription *bsub = subscription_fromtuple(tuple);
		ret = lappend(ret, bsub);
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return ret;
}

void
interval_from_ms(int ms, Interval *interval)
{
	struct pg_tm	tm;
	fsec_t			micros;

	/*
	 * The non-fractional part must fit into tm_sec
	 */
	memset(&tm, 0, sizeof(struct pg_tm));
	tm.tm_sec = ms / 1000;

	/*
	 * Store the subsecond part in the fractional seconds, as microseconds.
	 */
	micros = (ms % 1000) * 1000;

	if (tm2interval(&tm, micros, interval) != 0)
		elog(ERROR, "error converting %d ms to interval", ms);
}

static void
state_fromtuple(BdrStateEntry *state, HeapTuple tuple, TupleDesc tupDesc,
	bool fetch_extradata)
{
	StateTuple *stup = (StateTuple *) GETSTRUCT(tuple);
	Datum d;
	bool isnull = true;

	state->counter = stup->counter;
	state->current = (BdrNodeState)stup->current;
	state->goal = (BdrNodeState)stup->goal;
	state->entered_time = stup->entered_time;
	state->global_consensus_no = (uint64)stup->global_consensus_no;
	state->peer_id = stup->peer_id;

	if (fetch_extradata)
		d = heap_getattr(tuple, Anum_state_extra_data, tupDesc, &isnull);

	if (isnull)
		state->extra_data = NULL;
	else
	{
		StringInfoData si;
		bytea *extradata = DatumGetByteaPP(PG_DETOAST_DATUM_COPY(d));
		wrapInStringInfo(&si, VARDATA_ANY(extradata), VARSIZE_ANY_EXHDR(extradata));
		state->extra_data = state_extradata_deserialize(&si, state->current);
	}
}

void
state_prune(int maxentries)
{
	/*
	 * Prune the state table, deleting all but maxentries.
	 */
	elog(ERROR, "not implemented");
}

/*
 * Push a new state onto the top of the stack.
 *
 * Counter must be pre-set to exactly increment the prior
 * counter by 1. In most cases you should be using
 * state_transition instead, as it checks the counter
 * and last state.
 */
void
state_push(BdrStateEntry *state)
{
	RangeVar	   *rv;
	Relation		rel;
	TupleDesc		tupDesc;
	HeapTuple		tup;
	Datum			values[Natts_state];
	bool			nulls[Natts_state];
	bytea		   *extra_data = NULL;

	rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_STATE, -1);
	rel = heap_openrv(rv, ShareRowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	if (state->counter == 0)
		elog(ERROR, "attempt to insert invalid state counter 0");

	state->entered_time = GetCurrentTimestamp();

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));
	values[Anum_state_counter - 1] = ObjectIdGetDatum(state->counter);
	values[Anum_state_current - 1] = ObjectIdGetDatum((Oid)state->current);
	values[Anum_state_goal - 1] = ObjectIdGetDatum((Oid)state->goal);
	values[Anum_state_entered_time - 1] = TimestampTzGetDatum(state->entered_time);
	values[Anum_state_global_consensus_no - 1] = Int64GetDatum((int64)state->global_consensus_no);
	values[Anum_state_peer_id - 1] = ObjectIdGetDatum(state->peer_id);

	if (state->extra_data != NULL)
	{
		StringInfoData si;
		initStringInfo(&si);
		pq_sendint(&si, 0, VARHDRSZ); /* will overwrite later */
		state_extradata_serialize(&si, state->current, state->extra_data);
		extra_data = (bytea*)si.data;
 		/* overwrite size; header size included */
		SET_VARSIZE(extra_data, si.len);
	}
	Assert((state->extra_data == NULL) == (extra_data == NULL));
	nulls[Anum_state_extra_data - 1] = extra_data == NULL;
	values[Anum_state_extra_data - 1] = PointerGetDatum(extra_data);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Fetch the current state entry into the passed state struct.
 *
 * Optionally lock the state table so that states cannot be added by other
 * xacts until commit/rollback.
 */
void
state_get_last(BdrStateEntry *state, bool for_update, bool fetch_extradata)
{
	RangeVar   *state_rv;
	RangeVar   *state_idx_rv;
	Relation	state_rel;
	Relation	state_idx;
	SysScanDesc state_scan;
	HeapTuple	state_tuple;
	TupleDesc	tupDesc;
	const int	lockmode = for_update ? ShareRowExclusiveLock
									  : AccessShareLock;
	bool		found = false;
	BdrStateEntry tmp;

	Assert(IsTransactionState());

	state_rv = makeRangeVar(BDR_EXTENSION_NAME, CATALOG_STATE, -1);
	state_idx_rv = makeRangeVar(BDR_EXTENSION_NAME,
								CATALOG_STATE_COUNTER_IDX, -1);
	state_rel = heap_openrv(state_rv, lockmode);
	tupDesc = RelationGetDescr(state_rel);
	state_idx = relation_openrv(state_idx_rv, lockmode);
	if (state_idx->rd_rel->relkind != RELKIND_INDEX)
		elog(ERROR, BDR_EXTENSION_NAME"."CATALOG_STATE_COUNTER_IDX" is not an index");
	state_scan = systable_beginscan_ordered(state_rel, state_idx, NULL,
										   0, NULL);

	state_tuple = systable_getnext_ordered(state_scan, BackwardScanDirection);
	if (HeapTupleIsValid(state_tuple))
	{
		found = true;
		state_fromtuple(&tmp, state_tuple, tupDesc, fetch_extradata);
	}

	systable_endscan_ordered(state_scan);
	index_close(state_idx, lockmode);
	/* Keep the lock until commit if we're called for_update */
	heap_close(state_rel, for_update ? NoLock : lockmode);

	if (!found)
		/* shouldn't happen, callers should check if bdr is ready */
		elog(ERROR, "no entries found in BDR state table, BDR not initialized?");
	
	*state = tmp;
}

/*
 * Decode a tuple from bdr.state_journal into 'state'
 *
 * Exposed separately so we can use it when debugging. This just wraps
 * state_fromtuple with tuple metadata lookup. It's probably going to be pretty
 * slow looking up this metadata every time, but we can deal with that if we
 * ever need to care.
 */
void
state_decode_tuple(BdrStateEntry *state, HeapTupleHeader tupheader)
{
	TupleDesc	tupdesc;
	Oid			tupType;
	int32		tupTypmod;
	HeapTupleData	tuple;
	
	tupType = HeapTupleHeaderGetTypeId(tupheader);
	tupTypmod = HeapTupleHeaderGetTypMod(tupheader);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/*
	 * Build a temporary HeapTuple control structure.
	 *
	 * There must be a better way? Borrowed from record_in
	 */
	tuple.t_len = HeapTupleHeaderGetDatumLength(tupheader);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tupheader;

	state_fromtuple(state, &tuple, tupdesc, true);

	ReleaseTupleDesc(tupdesc);
}
