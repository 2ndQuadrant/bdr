/*-------------------------------------------------------------------------
 *
 * bdr_catalogs.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_catalogs.c
 *
 * BDR catalog access and manipulation, for the replication group membership,
 * etc. See bdr_catcache.c for cached lookups etc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr_catalogs.h"
