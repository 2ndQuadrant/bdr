#ifndef BDR_CATALOGS_H
#define BDR_CATALOGS_H

#define BDR_SCHEMA_NAME "bdr"
#define BDR_EXTENSION_NAME BDR_SCHEMA_NAME

#define BDR_MSGJOURNAL_REL_NAME "global_message_journal"

#include "nodes/pg_list.h"

#include "pglogical_node.h"

typedef struct BdrNode
{
    uint32 node_id;
    PGLogicalNode *pgl_node;
    uint32 node_group_id;
    int local_state; /* TODO */
    uint16 seq_id;
    bool confirmed_our_join;
} BdrNode;

extern BdrNode * bdr_get_node(Oid nodeid, bool missing_ok);

extern BdrNode * bdr_get_node_by_name(const char *name, bool missing_ok);

extern List * bdr_get_nodes(void);

extern List * bdr_get_node_subscriptions(uint32 node_id);

#endif
