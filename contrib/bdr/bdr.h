/*
 * bdr.h
 *
 * BiDirectionalReplication
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * contrib/bdr/bdr.h
 */
#ifndef BDR_APPLY_H
#define BDR_APPLY_H

#include "replication/logical.h"

#include "access/xlogdefs.h"

#include "utils/resowner.h"


typedef struct BDRCon
{
	/* remote database name */
	char *dbname;
	/* dsn to connect to the remote database */
	char *dsn;
	/* how much do we want to delay apply, in ms */
	int apply_delay;

	RepNodeId origin_id;
	uint64 sysid;
	TimeLineID timeline;
} BDRCon;

extern ResourceOwner bdr_saved_resowner;
extern BDRCon *bdr_connection;

/* apply support */
extern void process_remote_begin(char *data, size_t r);
extern void process_remote_commit(char *data, size_t r);
extern void process_remote_insert(char *data, size_t r);
extern void process_remote_update(char *data, size_t r);
extern void process_remote_delete(char *data, size_t r);

/* statistic functions */
extern void bdr_count_shmem_init(size_t nnodes);
extern void bdr_count_set_current_node(RepNodeId node_id);
extern void bdr_count_commit(void);
extern void bdr_count_rollback(void);
extern void bdr_count_insert(void);
extern void bdr_count_insert_conflict(void);
extern void bdr_count_update(void);
extern void bdr_count_update_conflict(void);
extern void bdr_count_delete(void);
extern void bdr_count_delete_conflict(void);

#endif
