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

	RepNodeId origin_id;
	uint64 sysid;
	TimeLineID timeline;
} BDRWorkerCon;

typedef struct BDRSequencerCon
{
	/* local database name */
	char *dbname;

	size_t slot;

	/* yuck */
	size_t num_nodes;
} BDRSequencerCon;

extern ResourceOwner bdr_saved_resowner;
extern BDRWorkerCon *bdr_apply_con;
extern BDRSequencerCon *bdr_sequencer_con;

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

extern void bdr_sequence_alloc(PG_FUNCTION_ARGS);
extern void bdr_sequence_setval(PG_FUNCTION_ARGS);
extern Datum bdr_sequence_options(PG_FUNCTION_ARGS);

/* DDL replication support */
extern void setup_queuedcmds_relid(void);

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


#endif	/* BDR_H */
