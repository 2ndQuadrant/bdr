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

/* A configured BDR connection from bdr_connections */
typedef struct BdrConnectionConfig
{
	uint64		sysid;
	TimeLineID	timeline;
	Oid			dboid;

	char *dsn;
	char *local_dsn;
	char *init_from_dsn;

	int   apply_delay;

	/* Quoted identifier-list of replication sets */
	char *replication_sets;
} BdrConnectionConfig;

typedef struct BdrFlushPosition
{
	dlist_node node;
	XLogRecPtr local_end;
	XLogRecPtr remote_end;
} BdrFlushPosition;

extern volatile sig_atomic_t got_SIGTERM;
extern volatile sig_atomic_t got_SIGHUP;

extern List* bdr_read_connection_configs(void);
extern BdrConnectionConfig* bdr_get_my_connection_config(void);
extern void bdr_free_connection_config(BdrConnectionConfig *cfg);


#endif   /* BDR_INTERNAL_H */
