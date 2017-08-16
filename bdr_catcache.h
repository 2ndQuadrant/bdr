#ifndef BDR_CATCACHE_H
#define BDR_CATCACHE_H

extern void bdr_cache_local_nodeinfo(void);

extern uint32 bdr_get_local_nodeid(void);

extern uint32 bdr_get_local_nodeid_if_exists(void);

inline bool
bdr_is_active_db(void)
{
	return bdr_get_local_nodeid_if_exists() != 0;
};

#endif
