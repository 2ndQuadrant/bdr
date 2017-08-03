/*-------------------------------------------------------------------------
 *
 * bdr_catcache.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_catcache.c
 *
 * BDR catalog caches and cache invalidaiton routines
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"

#include "bdr_catcache.h"

/*
 * Look up our local node information in the BDR catalogs for
 * this database and cache it in a global.
 *
 * No invalidations are handled. If the local node info changes the
 * managers and workers must be signalled to restart.
 *
 * Otherwise we'd need an xact whenever we looked up these caches, so we could
 * handle possibly reloading them.
 */
void
bdr_cache_local_nodeinfo(void)
{
	Assert(IsTransactionState());
	elog(ERROR, "not implemented");
}

uint32
bdr_get_local_nodeid(void)
{
	elog(ERROR, "not implemented");
}
