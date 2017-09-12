#ifndef BDR_CATCACHE_H
#define BDR_CATCACHE_H

extern void bdr_cache_local_nodeinfo(void);

extern void bdr_refresh_cache_local_nodeinfo(void);

struct BdrNodeInfo;
extern struct BdrNodeInfo* bdr_get_cached_local_node_info(void);

extern uint32 bdr_get_local_nodeid(void);

extern const char * bdr_get_local_node_name(void);

extern uint32 bdr_get_local_nodeid_if_exists(void);

extern bool bdr_catcache_initialised(void);

extern bool bdr_is_active_db(void);

extern uint32 bdr_get_local_nodegroup_id(bool missing_ok);

#endif
