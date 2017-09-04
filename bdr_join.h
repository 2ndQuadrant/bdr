#ifndef BDR_JOIN_H
#define BDR_JOIN_H

#include "bdr_catalogs.h"

struct pg_conn;
typedef struct pg_conn PGconn;

struct BdrMessage;

extern BdrNodeInfo * get_remote_node_info(PGconn *conn);

extern PGconn *bdr_join_connect_remote(const char * remote_node_dsn,
	BdrNodeInfo *local);

extern void bdr_finish_connect_remote(PGconn *conn);

extern uint64 bdr_join_submit_request(PGconn *conn, const char * node_group_name,
	BdrNodeInfo *local);

extern void bdr_join_handle_join_proposal(struct BdrMessage *msg);

extern void bdr_join_handle_catchup_proposal(struct BdrMessage *msg);

extern void bdr_join_handle_active_proposal(struct BdrMessage *msg);

extern BdrNodeInfo *get_remote_node_info(PGconn *conn);

extern void bdr_join_copy_remote_nodegroup(BdrNodeInfo *local,
	BdrNodeInfo *remote);

extern void bdr_join_copy_remote_nodes(PGconn *conn, BdrNodeInfo *local);

extern void bdr_join_subscribe_join_target(PGconn *conn, BdrNodeInfo *local, BdrNodeInfo *remote);

extern void bdr_join_copy_repset_memberships(PGconn *conn, BdrNodeInfo *local);

extern void bdr_join_init_consensus_messages(PGconn *conn, BdrNodeInfo *local);

extern void bdr_join_create_subscriptions(BdrNodeInfo *local, BdrNodeInfo *join_target);

extern XLogRecPtr bdr_get_remote_insert_lsn(PGconn *conn);

extern uint64 bdr_join_send_catchup_ready(BdrNodeInfo *local);

extern void bdr_join_create_slots(BdrNodeInfo *local);

extern uint64 bdr_join_send_active_announce(BdrNodeInfo *local);

extern void bdr_join_go_active(BdrNodeInfo *local);

#endif
