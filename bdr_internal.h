/*
 * bdr.h
 *
 * BiDirectionalReplication
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * contrib/bdr/bdr.h
 */
#ifndef BDR_INTERNAL_H
#define BDR_INTERNAL_H

#include <signal.h>

#include "lib/ilist.h"

#define BDR_SLOT_NAME_FORMAT "bdr_%u_%s_%u_%u__%s"
#define BDR_NODE_ID_FORMAT "bdr_"UINT64_FORMAT"_%u_%u_%u_%s"

/* GUC storage for a configured BDR connection. */
typedef struct BdrConnectionConfig
{
	char *dsn;
	int   apply_delay;
	bool  init_replica;
	char *replica_local_dsn;
	/* Quoted identifier-list of replication sets */
	char *replication_sets;

	/*
	 * These aren't technically GUCs, but are per-connection config
	 * information obtained from the GUCs.
	 */
	char *name;
	char *dbname;

	/* Connection config might be broken (blank dsn, etc) */
	bool is_valid;
} BdrConnectionConfig;

typedef struct BdrFlushPosition
{
	dlist_node node;
	XLogRecPtr local_end;
	XLogRecPtr remote_end;
} BdrFlushPosition;

extern volatile sig_atomic_t got_SIGTERM;
extern volatile sig_atomic_t got_SIGHUP;

#endif   /* BDR_INTERNAL_H */
