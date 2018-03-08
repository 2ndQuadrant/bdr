/*
 * bdr.h
 *
 * BiDirectionalReplication
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * bdr.h
 */
#ifndef BDR_H
#define BDR_H

#include "access/xlogdefs.h"
#include "postmaster/bgworker.h"
#include "replication/logical.h"
#include "utils/resowner.h"
#include "storage/latch.h"
#include "storage/lock.h"

#include "lib/ilist.h"

#include "libpq-fe.h"

#include "bdr_config.h"

#include "bdr_internal.h"

#include "bdr_version.h"

/* Right now replication_name isn't used; make it easily found for later */
#define EMPTY_REPLICATION_NAME ""

/*
 * BDR_LOCALID_FORMAT is used in fallback_application_name. It's distinct from
 * BDR_NODE_ID_FORMAT in that it doesn't include the remote dboid as that may
 * not be known yet, just (sysid,tlid,dboid,replication_name) .
 *
 * Use BDR_LOCALID_FORMAT_ARGS to sub it in to format strings.
 */
#define BDR_LOCALID_FORMAT "bdr ("UINT64_FORMAT",%u,%u,%s)"

#define BDR_LOCALID_FORMAT_ARGS \
	GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId, EMPTY_REPLICATION_NAME

#define BDR_INIT_REPLICA_CMD "bdr_initial_load"
#define BDR_LIBRARY_NAME "bdr"
#define BDR_RESTORE_CMD "pg_restore"
#define BDR_DUMP_CMD "bdr_dump"

#define BDR_SUPERVISOR_DBNAME "bdr_supervisordb"

/*
 * Don't include libpq here, msvc infrastructure requires linking to libpq
 * otherwise.
 */
struct pg_conn;

/* Forward declarations */
struct TupleTableSlot; /* from executor/tuptable.h */
struct EState; /* from nodes/execnodes.h */
struct ScanKeyData; /* from access/skey.h for ScanKey */
enum LockTupleMode; /* from access/heapam.h */

typedef struct BdrFlushPosition
{
	dlist_node node;
	XLogRecPtr local_end;
	XLogRecPtr remote_end;
} BdrFlushPosition;

/*
 * Flags to indicate which fields are present in a begin record sent by the
 * output plugin.
 */
typedef enum BdrOutputBeginFlags
{
	BDR_OUTPUT_TRANSACTION_HAS_ORIGIN = 1
} BdrOutputBeginFlags;

/*
 * BDR conflict detection: type of conflict that was identified.
 *
 * Must correspond to bdr.bdr_conflict_type SQL enum and
 * bdr_conflict_type_get_datum (...)
 */
typedef enum BdrConflictType
{
	BdrConflictType_InsertInsert,
	BdrConflictType_InsertUpdate,
	BdrConflictType_UpdateUpdate,
	BdrConflictType_UpdateDelete,
	BdrConflictType_DeleteDelete,
	BdrConflictType_UnhandledTxAbort
} BdrConflictType;

/*
 * BDR conflict detection: how the conflict was resolved (if it was).
 *
 * Must correspond to bdr.bdr_conflict_resolution SQL enum and
 * bdr_conflict_resolution_get_datum(...)
 */
typedef enum BdrConflictResolution
{
	BdrConflictResolution_ConflictTriggerSkipChange,
	BdrConflictResolution_ConflictTriggerReturnedTuple,
	BdrConflictResolution_LastUpdateWins_KeepLocal,
	BdrConflictResolution_LastUpdateWins_KeepRemote,
	BdrConflictResolution_DefaultApplyChange,
	BdrConflictResolution_DefaultSkipChange,
	BdrConflictResolution_UnhandledTxAbort
} BdrConflictResolution;

typedef struct BDRConflictHandler
{
	Oid			handler_oid;
	BdrConflictType handler_type;
	uint64		timeframe;
}	BDRConflictHandler;

/* How detailed logging of DDL locks is */
enum BdrDDLLockTraceLevel {
	/* Everything */
	DDL_LOCK_TRACE_DEBUG,
	/* Report acquire/release on peers, not just node doing DDL */
	DDL_LOCK_TRACE_PEERS,
	/* When locks are acquired/released */
	DDL_LOCK_TRACE_ACQUIRE_RELEASE,
	/* Only statements requesting DDL lock */
	DDL_LOCK_TRACE_STATEMENT,
	/* No DDL lock tracing */
	DDL_LOCK_TRACE_NONE
};

/*
 * This structure is for caching relation specific information, such as
 * conflict handlers.
 */
typedef struct BDRRelation
{
	/* hash key */
	Oid			reloid;

	bool		valid;

	Relation	rel;

	BDRConflictHandler *conflict_handlers;
	size_t		conflict_handlers_len;

	/* ordered list of replication sets of length num_* */
	char	  **replication_sets;
	/* -1 for no configured set */
	int			num_replication_sets;

	bool		computed_repl_valid;
	bool		computed_repl_insert;
	bool		computed_repl_update;
	bool		computed_repl_delete;
} BDRRelation;

typedef struct BDRTupleData
{
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	bool		changed[MaxTupleAttributeNumber];
} BDRTupleData;

/*
 * BdrApplyWorker describes a BDR worker connection.
 *
 * This struct is stored in an array in shared memory, so it can't have any
 * pointers.
 */
typedef struct BdrApplyWorker
{
	/* oid of the database this worker is applying changes to */
	Oid			dboid;

	/* assigned perdb worker slot */
	struct BdrWorker *perdb;

	/*
	 * Identification for the remote db we're connecting to; used to
	 * find the appropriate bdr.connections row, etc.
	 */
	uint64		remote_sysid;
	TimeLineID	remote_timeline;
	Oid			remote_dboid;

	/*
	 * If not InvalidXLogRecPtr, stop replay at this point and exit.
	 *
	 * To save shmem space in apply workers, this is reset to InvalidXLogRecPtr
	 * if replay is successfully completed instead of setting a separate flag.
	 */
	XLogRecPtr replay_stop_lsn;

	/* Request that the remote forward all changes from other nodes */
	bool forward_changesets;

	/*
	 * The apply worker's latch from the PROC array, for use from other backends
	 *
	 * Must only be accessed with the bdr worker shmem control segment lock held.
	 */
	Latch			*proclatch;
} BdrApplyWorker;

/*
 * BDRPerdbCon describes a per-database worker, a static bgworker that manages
 * BDR for a given DB.
 */
typedef struct BdrPerdbWorker
{
	/* local database name to connect to */
	NameData		dbname;

	/* number of outgoing connections from this database */
	Size			nnodes;

	size_t			seq_slot;

	/*
	 * The perdb worker's latch from the PROC array, for use from other backends
	 *
	 * Must only be accessed with the bdr worker shmem control segment lock held.
	 */
	Latch			*proclatch;

	/* Oid of the database the worker is attached to - populated after start */
	Oid				database_oid;
} BdrPerdbWorker;

/*
 * Walsender worker. These are only allocated while a output plugin is active.
 */
typedef struct BdrWalsenderWorker
{
	struct WalSnd *walsender;
	struct ReplicationSlot *slot;

	/* Identification for the remote the connection comes from. */
	uint64		remote_sysid;
	TimeLineID	remote_timeline;
	Oid			remote_dboid;

} BdrWalsenderWorker;

/*
 * Type of BDR worker in a BdrWorker struct
 *
 * Note that the supervisor worker doesn't appear here, it has its own
 * dedicated entry in the shmem segment.
 */
typedef enum {
	/*
	 * This shm array slot is unused and may be allocated. Must be zero, as
	 * it's set by memset(...) during shm segment init.
	 */
	BDR_WORKER_EMPTY_SLOT = 0,
	/* This shm array slot contains data for a BdrApplyWorker */
	BDR_WORKER_APPLY,
	/* This is data for a per-database worker BdrPerdbWorker */
	BDR_WORKER_PERDB,
	/* This is data for a walsenders currently streaming data out */
	BDR_WORKER_WALSENDER
} BdrWorkerType;

/*
 * BDRWorker entries describe shared memory slots that keep track of
 * all BDR worker types. A slot may contain data for a number of different
 * kinds of worker; this union makes sure each slot is the same size and
 * is easily accessed via an array.
 */
typedef struct BdrWorker
{
	/* Type of worker. Also used to determine if this shm slot is free. */
	BdrWorkerType	worker_type;

	/* pid worker if running, or 0 */
	pid_t			worker_pid;

	/* proc entry of worker if running, or NULL */
	PGPROC		   *worker_proc;

	union data {
		BdrApplyWorker apply;
		BdrPerdbWorker perdb;
		BdrWalsenderWorker walsnd;
	} data;

} BdrWorker;

/* GUCs */
extern int	bdr_default_apply_delay;
extern int bdr_max_workers;
extern int bdr_max_databases;
extern char *bdr_temp_dump_directory;
extern bool bdr_log_conflicts_to_table;
extern bool bdr_conflict_logging_include_tuples;
extern bool bdr_permit_ddl_locking;
extern bool bdr_permit_unsafe_commands;
extern bool bdr_skip_ddl_locking;
extern bool bdr_do_not_replicate;
extern bool bdr_discard_mismatched_row_attributes;
extern int bdr_max_ddl_lock_delay;
extern int bdr_ddl_lock_timeout;
extern bool bdr_trace_replay;
extern int bdr_trace_ddl_locks_level;
extern char *bdr_extra_apply_connection_options;

static const char * const bdr_default_apply_connection_options =
        "connect_timeout=30 "
        "keepalives=1 "
        "keepalives_idle=20 "
        "keepalives_interval=20 "
        "keepalives_count=5 ";

/*
 * Header for the shared memory segment ref'd by the BdrWorkerCtl ptr,
 * containing bdr_max_workers entries of BdrWorkerCon .
 */
typedef struct BdrWorkerControl
{
	/* Must hold this lock when writing to BdrWorkerControl members */
	LWLockId     lock;
	/* Worker generation number, incremented on postmaster restart */
	uint16       worker_generation;
	/* Set/unset by bdr_apply_pause()/_replay(). */
	bool		 pause_apply;
	/* Is this the first startup of the supervisor? */
	bool		 is_supervisor_restart;
	/* Pause worker management (used in testing) */
	bool		worker_management_paused;
	/* Latch for the supervisor worker */
	Latch		*supervisor_latch;
	/* Array members, of size bdr_max_workers */
	BdrWorker    slots[FLEXIBLE_ARRAY_MEMBER];
} BdrWorkerControl;

extern BdrWorkerControl *BdrWorkerCtl;
extern BdrWorker		*bdr_worker_slot;

extern ResourceOwner bdr_saved_resowner;

/* DDL executor/filtering support */
extern bool in_bdr_replicate_ddl_command;

/* cached oids, setup by bdr_maintain_schema() */
extern Oid	BdrSchemaOid;
extern Oid	BdrNodesRelid;
extern Oid	BdrConnectionsRelid;
extern Oid	QueuedDDLCommandsRelid;
extern Oid  BdrConflictHistoryRelId;
extern Oid  BdrReplicationSetConfigRelid;
extern Oid	BdrLocksRelid;
extern Oid	BdrLocksByOwnerRelid;
extern Oid	QueuedDropsRelid;
extern Oid	BdrSequenceValuesRelid;
extern Oid	BdrSequenceElectionsRelid;
extern Oid	BdrVotesRelid;
extern Oid	BdrSeqamOid;
extern Oid  BdrSupervisorDbOid;

/* Structure representing bdr_nodes record */
typedef struct BDRNodeId
{
	uint64		sysid;
	TimeLineID	timeline;
	Oid			dboid;
} BDRNodeId;

typedef struct BDRNodeInfo
{
	/* hash key */
	BDRNodeId	id;

	/* is this entry valid */
	bool		valid;

	char		status;

	char	   *local_dsn;
	char	   *init_from_dsn;

	bool		read_only;
} BDRNodeInfo;

extern Oid bdr_lookup_relid(const char *relname, Oid schema_oid);

extern void bdr_sequencer_set_nnodes(Size nnodes);


/* apply support */
extern void bdr_fetch_sysid_via_node_id(RepNodeId node_id, uint64 *sysid,
										TimeLineID *tli, Oid *remote_dboid);
extern RepNodeId bdr_fetch_node_id_via_sysid(uint64 sysid, TimeLineID tli, Oid dboid);

/* Index maintenance, heap access, etc */
extern struct EState * bdr_create_rel_estate(Relation rel);
extern void UserTableUpdateIndexes(struct EState *estate,
								   struct TupleTableSlot *slot);
extern void UserTableUpdateOpenIndexes(struct EState *estate,
									   struct TupleTableSlot *slot);
extern void build_index_scan_keys(struct EState *estate,
								  struct ScanKeyData **scan_keys,
								  BDRTupleData *tup);
extern bool build_index_scan_key(struct ScanKeyData *skey, Relation rel,
								 Relation idxrel,
								 BDRTupleData *tup);
extern bool find_pkey_tuple(struct ScanKeyData *skey, BDRRelation *rel,
							Relation idxrel, struct TupleTableSlot *slot,
							bool lock, enum LockTupleMode mode);

/* conflict logging (usable in apply only) */

/*
 * Details of a conflict detected by an apply process, destined for logging
 * output and/or conflict triggers.
 *
 * Closely related to bdr.bdr_conflict_history SQL table.
 */
typedef struct BdrApplyConflict
{
	TransactionId			local_conflict_txid;
	XLogRecPtr				local_conflict_lsn;
	TimestampTz				local_conflict_time;
	const char			   *object_schema; /* unused if apply_error */
	const char			   *object_name;   /* unused if apply_error */
	uint64					remote_sysid;
	TimeLineID				remote_tli;
	Oid						remote_dboid;
	TransactionId			remote_txid;
	TimestampTz				remote_commit_time;
	XLogRecPtr				remote_commit_lsn;
	BdrConflictType			conflict_type;
	BdrConflictResolution	conflict_resolution;
	bool					local_tuple_null;
	Datum					local_tuple;    /* composite */
	TransactionId			local_tuple_xmin;
	uint64					local_tuple_origin_sysid; /* init to 0 if unknown */
	TimeLineID				local_tuple_origin_tli;
	Oid						local_tuple_origin_dboid;
	bool					remote_tuple_null;
	Datum					remote_tuple;   /* composite */
	ErrorData			   *apply_error;
} BdrApplyConflict;

extern void bdr_conflict_logging_startup(void);
extern void bdr_conflict_logging_cleanup(void);

extern BdrApplyConflict * bdr_make_apply_conflict(BdrConflictType conflict_type,
									BdrConflictResolution resolution,
									TransactionId remote_txid,
									BDRRelation *conflict_relation,
									struct TupleTableSlot *local_tuple,
									RepNodeId local_tuple_origin_id,
									struct TupleTableSlot *remote_tuple,
									struct ErrorData *apply_error);

extern void bdr_conflict_log_serverlog(BdrApplyConflict *conflict);
extern void bdr_conflict_log_table(BdrApplyConflict *conflict);

extern void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple);

/* sequence support */
extern void bdr_sequencer_shmem_init(int sequencers);
extern void bdr_sequencer_init(int seq_slot, Size nnodes);
extern void bdr_sequencer_lock(void);
extern bool bdr_sequencer_vote(void);
extern void bdr_sequencer_tally(void);
extern bool bdr_sequencer_start_elections(void);
extern void bdr_sequencer_fill_sequences(void);

extern void bdr_sequencer_wakeup(void);
extern void bdr_schedule_eoxact_sequencer_wakeup(void);

extern int bdr_sequencer_get_next_free_slot(void); //XXX PERDB temp


/* statistic functions */
extern void bdr_count_shmem_init(Size nnodes);
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

/* initialize a new bdr member */
extern void bdr_init_replica(BDRNodeInfo *local_node);

extern void bdr_maintain_schema(bool update_extensions);

/* shared memory management */
extern void bdr_shmem_init(void);

extern BdrWorker* bdr_worker_shmem_alloc(BdrWorkerType worker_type,
										 uint32 *ctl_idx);
extern void bdr_worker_shmem_free(BdrWorker* worker, BackgroundWorkerHandle *handle);
extern void bdr_worker_shmem_acquire(BdrWorkerType worker_type,
									 uint32 worker_idx,
									 bool free_at_rel);
extern void bdr_worker_shmem_release(void);

extern bool bdr_is_bdr_activated_db(Oid dboid);
extern BdrWorker *bdr_worker_get_entry(uint64 sysid,
									   TimeLineID timeline,
									   Oid dboid,
									   BdrWorkerType worker_type);

/* forbid commands we do not support currently (or never will) */
extern void init_bdr_commandfilter(void);
extern void bdr_commandfilter_always_allow_ddl(bool always_allow);

extern void bdr_executor_init(void);
extern void bdr_executor_always_allow_writes(bool always_allow);
extern void bdr_queue_ddl_command(char *command_tag, char *command);
extern void bdr_execute_ddl_command(char *cmdstr, char *perpetrator, bool tx_just_started);
extern void bdr_start_truncate(void);
extern void bdr_finish_truncate(void);

extern void bdr_locks_shmem_init(void);
extern void bdr_locks_check_dml(void);

/* background workers and supporting functions for them */
PGDLLEXPORT extern void bdr_apply_main(Datum main_arg);
PGDLLEXPORT extern void bdr_perdb_worker_main(Datum main_arg);
PGDLLEXPORT extern void bdr_supervisor_worker_main(Datum main_arg);

extern void bdr_bgworker_init(uint32 worker_arg, BdrWorkerType worker_type);
extern void bdr_supervisor_register(void);

extern Oid bdr_get_supervisordb_oid(bool missing_ok);

extern void bdr_sighup(SIGNAL_ARGS);
extern void bdr_sigterm(SIGNAL_ARGS);

extern int find_perdb_worker_slot(Oid dboid,
									 BdrWorker **worker_found);

extern void bdr_maintain_db_workers(void);

extern Datum bdr_connections_changed(PG_FUNCTION_ARGS);

/* Information functions */
extern int bdr_parse_version(const char * bdr_version_str, int *o_major,
							 int *o_minor, int *o_rev, int *o_subrev);

/* manipulation of bdr catalogs */
extern char bdr_nodes_get_local_status(uint64 sysid, TimeLineID tli,
									   Oid dboid);
extern BDRNodeInfo * bdr_nodes_get_local_info(uint64 sysid, TimeLineID tli,
										  Oid dboid);
extern void bdr_bdr_node_free(BDRNodeInfo *node);
extern void bdr_nodes_set_local_status(char status);
extern List* bdr_read_connection_configs(void);

extern Oid GetSysCacheOidError(int cacheId, Datum key1, Datum key2, Datum key3,
							   Datum key4);

extern bool bdr_get_node_identity_by_name(const char *node_name, uint64 *sysid,
										  TimeLineID *timeline, Oid *dboid);

#define GetSysCacheOidError2(cacheId, key1, key2) \
	GetSysCacheOidError(cacheId, key1, key2, 0, 0)

extern void
stringify_my_node_identity(char *sysid_str, Size sysid_str_size,
						char *timeline_str, Size timeline_str_size,
						char *dboid_str, Size dboid_str_size);

extern void
stringify_node_identity(char *sysid_str, Size sysid_str_size,
						char *timeline_str, Size timeline_str_size,
						char *dboid_str, Size dboid_str_size,
						uint64 sysid, TimeLineID timeline, Oid dboid);

extern void
bdr_copytable(PGconn *copyfrom_conn, PGconn *copyto_conn,
		const char * copyfrom_query, const char *copyto_query);

/* local node info cache (bdr_nodecache.c) */
void bdr_nodecache_invalidate(void);
bool bdr_local_node_read_only(void);
char bdr_local_node_status(void);

/* helpers shared by multiple worker types */
extern struct pg_conn* bdr_connect(const char *conninfo, Name appname,
								   uint64* remote_sysid_i,
								   TimeLineID *remote_tlid_i,
								   Oid *out_dboid_i);

extern struct pg_conn *
bdr_establish_connection_and_slot(const char *dsn,
								  const char *application_name_suffix,
								  Name out_slot_name,
								  uint64 *out_sysid,
								  TimeLineID *out_timeline,
								  Oid *out_dboid,
								  RepNodeId *out_replication_identifier,
	 							  char **out_snapshot);

extern PGconn* bdr_connect_nonrepl(const char *connstring,
		const char *appnamesuffix);

/* Helper for PG_ENSURE_ERROR_CLEANUP to close a PGconn */
extern void bdr_cleanup_conn_close(int code, Datum offset);

/* use instead of heap_open()/heap_close() */
extern BDRRelation *bdr_heap_open(Oid reloid, LOCKMODE lockmode);
extern void bdr_heap_close(BDRRelation * rel, LOCKMODE lockmode);
extern void bdr_heap_compute_replication_settings(
	BDRRelation *rel,
	int			num_replication_sets,
	char	  **replication_sets);
extern void BDRRelcacheHashInvalidateCallback(Datum arg, Oid relid);

extern void bdr_parse_relation_options(const char *label, BDRRelation *rel);
extern void bdr_parse_database_options(const char *label, bool *is_active);

/* conflict handlers API */
extern void bdr_conflict_handlers_init(void);

extern HeapTuple bdr_conflict_handlers_resolve(BDRRelation * rel,
											   const HeapTuple local,
											   const HeapTuple remote,
											   const char *command_tag,
											   BdrConflictType event_type,
											   uint64 timeframe, bool *skip);

/* replication set stuff */
void bdr_validate_replication_set_name(const char *name, bool allow_implicit);

/* Helpers to probe remote nodes */

typedef struct remote_node_info
{
	uint64 sysid;
	char *sysid_str;
	TimeLineID timeline;
	Oid dboid;
	char *variant;
	char *version;
	int version_num;
	int min_remote_version_num;
	bool is_superuser;
} remote_node_info;

extern void bdr_get_remote_nodeinfo_internal(PGconn *conn, remote_node_info *ri);

extern void free_remote_node_info(remote_node_info *ri);

extern void bdr_ensure_ext_installed(PGconn *pgconn);

extern void bdr_test_remote_connectback_internal(PGconn *conn,
		struct remote_node_info *ri, const char *my_dsn);

/*
 * Global to identify the type of BDR worker the current process is. Primarily
 * useful for assertions and debugging.
 */
extern BdrWorkerType bdr_worker_type;

#endif   /* BDR_H */
