/*-------------------------------------------------------------------------
 * logical.h
 *     PostgreSQL WAL to logical transformation
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICAL_H
#define LOGICAL_H

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "replication/output_plugin.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/*
 * Shared memory state of a single logical decoding slot
 */
typedef struct LogicalDecodingSlot
{
	/* lock, on same cacheline as effective_xmin */
	slock_t		mutex;

	/* on-disk xmin, updated first */
	TransactionId xmin;

	/* in-memory xmin, updated after syncing to disk */
	TransactionId effective_xmin;

	XLogRecPtr	  last_required_checkpoint;

	/* is this slot defined */
	bool          in_use;

	/* is somebody streaming out changes for this slot */
	bool          active;

	/* have we been aborted while ->active */
	bool          aborted;

	/*
	 * If we shutdown, crash, whatever where do we have to restart decoding
	 * from to get
	 * a) a valid snapshot
	 * b) the complete content for all in-progress xacts
	 */
	XLogRecPtr	  restart_decoding;

	/*
	 * Last location we know the client has confirmed to have safely received
	 * data to. No earlier data can be decoded after a restart/crash.
	 */
	XLogRecPtr	  confirmed_flush;

	/*
	 * When the client has confirmed flushes >= candidate_xmin_after we can
	 * a) advance our xmin
	 * b) increase restart_decoding_from
	 *
	 */
	XLogRecPtr	  candidate_lsn;
	TransactionId candidate_xmin;
	XLogRecPtr	  candidate_restart_decoding;

	/* database the slot is active on */
	Oid           database;

	/* slot identifier */
	NameData      name;

	/* plugin name */
	NameData      plugin;
} LogicalDecodingSlot;

/*
 * Shared memory control area for all of logical decoding
 */
typedef struct LogicalDecodingCtlData
{
	/*
	 * Xmin across all logical slots.
	 *
	 * Protected by ProcArrayLock.
	 */
	TransactionId xmin;

	LogicalDecodingSlot logical_slots[FLEXIBLE_ARRAY_MEMBER];		/* VARIABLE LENGTH ARRAY */
} LogicalDecodingCtlData;

/*
 * Pointers to shared memory
 */
extern LogicalDecodingCtlData *LogicalDecodingCtl;
extern LogicalDecodingSlot *MyLogicalDecodingSlot;

struct LogicalDecodingContext;

typedef void (*LogicalOutputPluginWriterWrite) (
	struct LogicalDecodingContext *lr,
	XLogRecPtr Ptr,
	TransactionId xid
	);

typedef LogicalOutputPluginWriterWrite LogicalOutputPluginWriterPrepareWrite;
/*
 * Output plugin callbacks
 */
typedef struct OutputPluginCallbacks {
	LogicalDecodeInitCB init_cb;
	LogicalDecodeBeginCB begin_cb;
	LogicalDecodeChangeCB change_cb;
	LogicalDecodeCommitCB commit_cb;
	LogicalDecodeCleanupCB cleanup_cb;
} OutputPluginCallbacks;

typedef struct LogicalDecodingContext
{
	struct XLogReaderState *reader;
	struct LogicalDecodingSlot *slot;
	struct ReorderBuffer *reorder;
	struct Snapstate *snapshot_builder;

	struct OutputPluginCallbacks callbacks;

	bool stop_after_consistent;

	/*
	 * User specified options
	 */
	List *output_plugin_options;

	/*
	 * User-Provided callback for writing/streaming out data.
	 */
	LogicalOutputPluginWriterPrepareWrite prepare_write;
	LogicalOutputPluginWriterWrite write;

	/*
	 * Output buffer.
	 */
	StringInfo out;

	/*
	 * Private data pointer for the creator of the logical decoding context.
	 */
	void *owner_private;

	/*
	 * Private data pointer of the output plugin.
	 */
	void *output_plugin_private;

	/*
	 * Private data pointer for the data writer.
	 */
	void *output_writer_private;
} LogicalDecodingContext;

/*
 * logical replication on-disk data
 */
#define LOGICAL_MAGIC	0x1051CA1		/* format identifier */

/* FIXME: rename */
typedef struct LogicalDecodingCheckpointData
{
	uint32 magic;
	LogicalDecodingSlot slot;
} LogicalDecodingCheckpointData;

#define InvalidRepNodeId 0
extern RepNodeId guc_replication_node_id;
extern RepNodeId guc_replication_origin_id;
extern XLogRecPtr replication_origin_lsn;


extern Size LogicalDecodingShmemSize(void);
extern void LogicalDecodingShmemInit(void);

extern void LogicalDecodingAcquireFreeSlot(const char *name, const char *plugin);
extern void LogicalDecodingReleaseSlot(void);
extern void LogicalDecodingReAcquireSlot(const char *name);
extern void LogicalDecodingFreeSlot(const char *name);

extern void ComputeLogicalXmin(void);

/* change logical xmin */
extern void IncreaseLogicalXminForSlot(XLogRecPtr lsn, TransactionId xmin);
extern void LogicalConfirmReceivedLocation(XLogRecPtr lsn);

extern void CheckLogicalReplicationRequirements(void);

extern void StartupLogicalReplication(XLogRecPtr checkPointRedo);

/* GUCs */
extern int	max_logical_slots;

extern LogicalDecodingContext *CreateLogicalDecodingContext(
	LogicalDecodingSlot *slot,
	bool is_init,
	List *output_plugin_options,
	XLogPageReadCB read_page,
	LogicalOutputPluginWriterPrepareWrite prepare_write,
	LogicalOutputPluginWriterWrite do_write);
extern bool LogicalDecodingContextReady(LogicalDecodingContext *ctx);
extern void FreeLogicalDecodingContext(LogicalDecodingContext *ctx);


#endif
