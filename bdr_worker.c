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
 * TODO: ensure BDR is active in this database, ERROR if not active
 */
void
bdr_ensure_active_db(void)
{
	elog(ERROR, "not implemented");
}
