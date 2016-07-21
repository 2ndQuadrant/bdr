/*
 * bdr_internal.h
 *
 * BiDirectionalReplication
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * bdr_internal.h
 */
#ifndef BDR_INTERNAL_H
#define BDR_INTERNAL_H

#include <signal.h>

#include "lib/ilist.h"

#define EMPTY_REPLICATION_NAME ""
#define BDR_SLOT_NAME_FORMAT "bdr_%u_"UINT64_FORMAT"_%u_%u__%s"
#define BDR_NODE_ID_FORMAT "bdr_"UINT64_FORMAT"_%u_%u_%u_%s"

#ifdef __GNUC__
#define BDR_WARN_UNUSED __attribute__((warn_unused_result))
#define BDR_NORETURN __attribute__((noreturn))
#else
#define BDR_WARN_UNUSED
#define BDR_NORETURN
#endif

/* A configured BDR connection from bdr_connections */
typedef struct BdrConnectionConfig
{
	uint64		sysid;
	TimeLineID	timeline;
	Oid			dboid;

	/*
	 * If the origin_ id fields are set then they must refer to our node,
	 * otherwise we wouldn't load the configuration entry. So if origin_is_set
	 * is false the origin was zero, and if true the origin is the local node
	 * id.
	 */
	bool origin_is_my_id;

	char *dsn;

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

extern void bdr_error_nodeids_must_differ(uint64 sysid, TimeLineID timeline,
										  Oid dboid);
extern List* bdr_read_connection_configs(void);
extern BdrConnectionConfig* bdr_get_connection_config(uint64 sysid,
													  TimeLineID timeline,
													  Oid dboid,
													  bool missing_ok);

extern void bdr_free_connection_config(BdrConnectionConfig *cfg);

extern void bdr_slot_name(Name slot_name, uint64 sysid, TimeLineID tlid,
						  Oid dboid, Oid local_dboid);
extern void bdr_parse_slot_name(const char *name, uint64 *remote_sysid,
								Oid *remote_dboid, TimeLineID *remote_tli,
								Oid *local_dboid);

extern int bdr_find_other_exec(const char *argv0, const char *target,
							   uint32 *version, char *retpath);

#endif   /* BDR_INTERNAL_H */
