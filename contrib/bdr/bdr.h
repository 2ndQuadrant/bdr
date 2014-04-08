/*
 * bdr.h
 *
 * BiDirectionalReplication
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * contrib/bdr/bdr.h
 */
#ifndef BDR_H
#define BDR_H

#include "replication/logical.h"
#include "access/xlogdefs.h"
#include "utils/resowner.h"

#define BDR_VERSION_NUM 500

typedef struct BDRWorkerCon
{
	/* name specified in configuration */
	char *name;

	/* local & remote database name */
	char *dbname;
	/* dsn to connect to the remote database */
	char *dsn;
	/* how much do we want to delay apply, in ms */
	int apply_delay;
	/* should we copy the upstream database? */
	bool init_replica;
	/* the dsn to use for the local database */
	char *replica_local_dsn;
	/* the command to use for init_replica */
	char *replica_script_path;

	RepNodeId origin_id;
	uint64 sysid;
	TimeLineID timeline;
} BDRWorkerCon;

typedef struct BDRPerdbCon
{
	/* local database name */
	char *dbname;

	size_t slot;

	List *conns;
} BDRPerdbCon;

extern ResourceOwner bdr_saved_resowner;
extern BDRWorkerCon *bdr_apply_con;
extern BDRPerdbCon *bdr_static_con;

/* DDL replication support */
extern Oid	QueuedDDLCommandsRelid;

/* sequencer support */
extern Oid	BdrSequenceValuesRelid;
extern Oid	BdrSequenceElectionsRelid;
extern Oid	BdrVotesRelid;

/* apply support */
extern void process_remote_begin(StringInfo s);
extern void process_remote_commit(StringInfo s);
extern void process_remote_insert(StringInfo s);
extern void process_remote_update(StringInfo s);
extern void process_remote_delete(StringInfo s);

/* sequence support */
extern void bdr_sequencer_shmem_init(int nnodes, int sequencers);
extern void bdr_sequencer_init(void);
extern void bdr_sequencer_vote(void);
extern void bdr_sequencer_tally(void);
extern void bdr_sequencer_start_elections(void);
extern void bdr_sequencer_fill_sequences(void);

extern void bdr_sequencer_wakeup(void);
extern void bdr_schedule_eoxact_sequencer_wakeup(void);

extern void bdr_sequence_alloc(PG_FUNCTION_ARGS);
extern void bdr_sequence_setval(PG_FUNCTION_ARGS);
extern Datum bdr_sequence_options(PG_FUNCTION_ARGS);

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
extern void bdr_count_disconnect(void);

/* compat check functions */
extern bool bdr_get_float4byval(void);
extern bool bdr_get_float8byval(void);
extern bool bdr_get_integer_timestamps(void);
extern bool bdr_get_bigendian(void);

/* forbid commands we do not support currently (or never will) */
extern void init_bdr_commandfilter(void);

#endif	/* BDR_H */
