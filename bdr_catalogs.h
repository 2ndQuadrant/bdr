#ifndef BDR_CATALOGS_H
#define BDR_CATALOGS_H

#define BDR_SCHEMA_NAME "bdr"
#define BDR_EXTENSION_NAME BDR_SCHEMA_NAME

#define BDR_MSGJOURNAL_REL_NAME "global_message_journal"

#include "access/htup.h"

#include "nodes/pg_list.h"

#include "datatype/timestamp.h"

#include "pglogical_node.h"

typedef struct BdrNodeGroup
{
	uint32		id;
	const char *name;
	uint32		default_repset;
} BdrNodeGroup;

typedef struct BdrNode
{
    uint32		node_id;
    uint32		node_group_id;
    uint32		local_state; /* TODO */
    int32		seq_id;
    bool		confirmed_our_join;
	const char *dbname;
} BdrNode;

/*
 * This is a container for node information, since it's spread across
 * a few catalogs but we usually want to fetch it together.
 *
 * The node group and interface may not be set, depending on the call that
 * produced the BdrNodeInfo. Check the call docs.
 */
typedef struct BdrNodeInfo
{
	BdrNode			   *bdr_node;
	BdrNodeGroup	   *bdr_node_group;
    PGLogicalNode	   *pgl_node;
	PGlogicalInterface *pgl_interface;
} BdrNodeInfo;

extern BdrNodeGroup * bdr_get_nodegroup(Oid nodegroup_id, bool missing_ok);

extern BdrNodeGroup * bdr_get_nodegroup_by_name(const char *name,
												bool missing_ok);

extern Oid bdr_nodegroup_create(BdrNodeGroup *nodegroup);

extern BdrNode * bdr_get_node(Oid nodeid, bool missing_ok);

extern BdrNodeInfo * bdr_get_node_info(Oid nodeid, bool missing_ok);

extern BdrNodeInfo * bdr_get_node_info_by_name(const char *name, bool missing_ok);

extern BdrNodeInfo * bdr_get_local_node_info(bool for_update, bool missing_ok);

/* List of BdrNode */
extern List * bdr_get_nodes_info(Oid in_group_id);

/* List of PGLogicalSubscription */
extern List * bdr_get_node_subscriptions(uint32 node_id);

extern void bdr_node_create(BdrNode *node);

extern void bdr_modify_node(BdrNode *node);

extern void bdr_create_node_defaults(const char *node_name,
									 const char *local_dsn);

struct BdrStateEntry;
extern void state_push(struct BdrStateEntry *state);

extern void state_get_last(struct BdrStateEntry *out_state, bool for_update,
	bool get_extradata);

extern void state_prune(int maxentries);

extern void state_decode_tuple(struct BdrStateEntry *state, HeapTupleHeader tup);

/* Random helpers related to working with pgl/bdr catalogs */
extern void interval_from_ms(int ms, Interval *interval);

extern void check_nodeinfo(BdrNodeInfo* nodeinfo);

#endif
