/*-------------------------------------------------------------------------
 *
 * bdr_worker.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_worker.c
 *
 * Functionality shared by multiple worker types
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"

#include "bdr_catcache.h"
#include "bdr_worker.h"

bool bdr_required_this_conn = false;
int peer_bdr_version_num = -1;
char peer_bdr_version_str[BDR_VERSION_STR_SIZE];

/*
 * GUC storage for bdr.debug_level
 *
 * Not everything should use this, just debug messages you don't usually want
 * to see at all. It's primary utility is to promote BDR messages from debug2
 * or lower, where they'd be lost in the general postgres spam at
 * debug2/debug3, to a useful level when we want to do BDR debugging.
 */
int bdr_debug_level;

/*
 * Ensure BDR is active in this database, ERROR if not active.
 *
 * Will init the catcache if not already initalised, if a txn
 * is open.
 */
void
bdr_ensure_active_db(void)
{
	if (IsTransactionState() && !bdr_catcache_initialised())
		bdr_cache_local_nodeinfo();

	if (!bdr_is_active_db())
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("BDR is not active in this database")));
	}
}
