/*-------------------------------------------------------------------------
 *
 * bdr_pgl_plugin.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_pgl_plugin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/xlog.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "pglogical_worker.h"
#include "pglogical_plugins.h"
#include "pglogical.h"

