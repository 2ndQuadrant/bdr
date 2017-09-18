#ifndef BDR_WORKER_H
#define BDR_WORKER_H

#define BDR_VERSION_STR_SIZE 64

extern int peer_bdr_version_num;
extern char peer_bdr_version_str[BDR_VERSION_STR_SIZE];
extern uint32 peer_bdr_node_group_id;
extern uint32 peer_bdr_node_id;

extern void bdr_ensure_active_db(void);

extern int bdr_debug_level;

extern bool is_bdr_manager(void);

#endif
