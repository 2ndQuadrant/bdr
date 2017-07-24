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
