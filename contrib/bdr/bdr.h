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

typedef enum
{
	/*
	 * This shm array slot is unused and may be allocated. Must be zero,
	 * as it's set by memset(...) during shm segment init.
	 */
	BDR_WORKER_EMPTY_SLOT = 0,
	/* This shm array slot contains data for an apply worker */
	BDR_WORKER_APPLY,
	/* This shm array slot contains data fro a per-db worker */
	BDR_WORKER_PERDB
} BDRWorkerType;

typedef struct BDRWorkerShmSlotHeader
{
	/* Type of worker. Also used to determine if slot is free. */
	BDRWorkerType worker_type;
} BDRWorkerShmSlotHeader;

/*
 * BDRWorkerCon describes a BDR worker connection.
 *
 * This struct is stored in an array in shared memory, so it can't have any
 * pointers.
 */
typedef struct BDRWorkerCon
{
	BDRWorkerShmSlotHeader header;

	/* name specified in configuration */
	NameData name;

	/* local & remote database name */
	NameData dbname;

	RepNodeId origin_id;

	uint64 sysid;

	TimeLineID timeline;

} BDRWorkerCon;

/*
 * BDRPerdbCon describes a per-database worker, a static bgworker that manages
 * BDR for a given DB.
 *
 * TODO: Make this capable of living in shared memory so we can support dynamic
 * reconfiguration and EXEC_BACKEND. Probably coexist with BDRWorkerCon using
 * first field as worker type, allocating array entries big enough to hold either
 * worker struct. Or use a union.
 */
typedef struct BDRPerdbCon
{
	/* local database name */
	NameData dbname;

	size_t seq_slot;

} BDRPerdbCon;

static const size_t BDR_WORKER_SHM_ENTRY_SIZE = sizeof(BDRPerdbCon) > sizeof(BDRWorkerCon) ? sizeof(BDRPerdbCon) : sizeof(BDRWorkerCon);

extern ResourceOwner bdr_saved_resowner;
extern BDRWorkerCon *bdr_apply_con;
extern BDRPerdbCon *bdr_static_con;

/* bdr.max_workers guc */
extern int bdr_max_workers;

/* bdr_nodes table oid */
extern Oid	BdrNodesRelid;

/* DDL replication support */
extern Oid	QueuedDDLCommandsRelid;
extern Oid	QueuedDropsRelid;

/* sequencer support */
extern Oid	BdrSequenceValuesRelid;
extern Oid	BdrSequenceElectionsRelid;
extern Oid	BdrVotesRelid;

/* Helpers for accessing configuration */
const char *BDRGetWorkerOption(const char * worker_name, const char * option_name, bool missing_ok);

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
