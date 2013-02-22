/*-------------------------------------------------------------------------
 * logical.c
 *	   PostgreSQL changeset extraction coordination
 *
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/logical.c
 *
 * NOTES
 *
 *    This file coordinates interaction between the various modules that
 *    together providethe changeset extraction, primarily by providing so
 *    called LogicalDecodingContexts. The goal is to encapsulate most of the
 *    internal complexity for consumers of changeset extraction, so they can
 *    create and consume a changestream with a low amount of code.
 *
 *    The idea is that a consumer provides three callbacks, one to read WAL,
 *    one to prepare a data write, and a final one for actually writing since
 *    their implementation depends on the type of consumer.  Check
 *    logicalfunc.c for an example implementations of a fairly simple consumer
 *    and a implementation of a WAL reading callback that's suitable for
 *    simpler consumers.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"

#include "access/xact.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"

#include "storage/proc.h"
#include "storage/procarray.h"

#include "utils/memutils.h"

RepNodeId	replication_origin_id = InvalidRepNodeId; /* assumed identity */
XLogRecPtr	replication_origin_lsn;
TimestampTz	replication_origin_timestamp;

/* data for errcontext callback */
typedef struct LogicalErrorCallbackState
{
	LogicalDecodingContext *ctx;
	const char *callback_name;
	XLogRecPtr	report_location;
} LogicalErrorCallbackState;

/* wrappers around output plugin callbacks */
static void output_plugin_error_callback(void *arg);
static void startup_slot_wrapper(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
								 bool is_init);
static void shutdown_slot_wrapper(LogicalDecodingContext *ctx);
static void commit_txn_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
							   XLogRecPtr commit_lsn);
static void begin_txn_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn);
static void change_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						   Relation relation, ReorderBufferChange *change);

static void LoadOutputPlugin(OutputPluginCallbacks *callbacks, char *plugin);

/*
 * Make sure the current settings & environment are capable of doing logical
 * changeset extraction.
 */
void
CheckLogicalDecodingRequirements(void)
{
	CheckSlotRequirements();

	if (wal_level < WAL_LEVEL_LOGICAL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("changeset extraction requires wal_level >= logical")));

	if (MyDatabaseId == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("changeset extraction requires to be connected to a database")));

	/* TODO: We got to change that someday soon.. */
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("changeset extraction cannot be used while in recovery")));

}

/*
 * Allocate a new decoding context.
 *
 * is_init denotes whether the slot is newly initialized
 * plugin sets the output plugin used when initializing the slot
 * output_plugin_options contains options passed to the output plugin
 * read_page, prepare_write, do_write are callbacks that have to be filled to
 *		perform the use-case dependent, actual, work.
 *
 * Needs to be called while in a memory context that's at least as long lived
 * as the the decoding context because further memory contexts will be created
 * inside it.
 *
 * Returns an initialized decoding context after calling the output plugin's
 * startup function.
 */
LogicalDecodingContext *
CreateDecodingContext(bool is_init,
					  char *plugin,
					  XLogRecPtr start_lsn,
					  List *output_plugin_options,
					  XLogPageReadCB read_page,
					  LogicalOutputPluginWriterPrepareWrite prepare_write,
					  LogicalOutputPluginWriterWrite do_write)
{
	MemoryContext context;
	MemoryContext old_context;
	TransactionId xmin_horizon = InvalidTransactionId;
	LogicalDecodingContext *ctx;
	ReplicationSlot *slot;

	/* shorter lines... */
	slot = MyReplicationSlot;

	/* first some sanity checks that are unlikely to be violated */
	if (MyReplicationSlot == NULL)
		elog(ERROR, "cannot perform logical decoding without a acquired slot");

	if (is_init && start_lsn != InvalidXLogRecPtr)
		elog(ERROR, "cannot initialize logical decoding at a specified LSN");

	if (is_init && plugin == NULL)
		elog(ERROR, "cannot initialize logical decoding without a specified plugin");

	/* make sure the passed slot is suitable, these are user facing errors */
	if (slot->data.database == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("cannot use replication slot created for streaming replication for changeset extraction"))));

	if (slot->data.database != MyDatabaseId)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("cannot use replication slot \"%s\" in database %u, it was created in %u",
						 NameStr(slot->data.name), slot->data.database, MyDatabaseId))));

	if (IsTransactionState() &&
		GetTopTransactionIdIfAny() != InvalidTransactionId)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot perform changeset extraction in transaction that has performed writes")));

	context = AllocSetContextCreate(CurrentMemoryContext,
									"Changeset Extraction Context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	old_context = MemoryContextSwitchTo(context);
	ctx = palloc0(sizeof(LogicalDecodingContext));

	ctx->context = context;

	/* If we're initializing a new slot, there's a bit more to do */
	if (is_init)
	{
		/* register output plugin name with slot */
		strncpy(NameStr(slot->data.plugin), plugin,
				NAMEDATALEN);
		NameStr(slot->data.plugin)[NAMEDATALEN - 1] = '\0';

		/*
		 * Let's start with enough information if we can, so log a standby
		 * snapshot and start decoding at exactly that position.
		 */
		if (!RecoveryInProgress())
		{
			XLogRecPtr flushptr;

			/* start at current insert position*/
			slot->data.restart_lsn = GetXLogInsertRecPtr();

			/* make sure we have enough information to start */
			flushptr = LogStandbySnapshot();

			/* and make sure it's fsynced to disk */
			XLogFlush(flushptr);
		}
		else
			slot->data.restart_lsn = GetRedoRecPtr();

		/* prevent WAL removal as fast as possible */
		ReplicationSlotsComputeRequiredLSN();

		/* ----
		 * This is a bit tricky: We need to determine a safe xmin horizon to
		 * start decoding from, to avoid starting from a running xacts record
		 * referring to xids whose rows have been vacuumed or pruned
		 * already. GetOldestSafeDecodingTransactionId() returns such a value,
		 * but without further interlock it's return value might immediately
		 * be out of date.
		 *
		 * So we have to acquire the ProcArrayLock to prevent computation of
		 * new xmin horizons by other backends, get the safe decoding xid, and
		 * inform the slot machinery about the new limit. Once that's done the
		 * ProcArrayLock can be be released as the slot machinery now is
		 * protecting against vacuum.
		 * ----
		 */
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

		slot->effective_catalog_xmin = GetOldestSafeDecodingTransactionId();
		slot->data.catalog_xmin = slot->effective_catalog_xmin;

		ReplicationSlotsComputeRequiredXmin(true);

		LWLockRelease(ProcArrayLock);

		/*
		 * tell the snapshot builder to only assemble snapshot once reaching
		 * the a running_xact's record with the respective xmin.
		 */
		xmin_horizon = slot->data.catalog_xmin;

		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
	}
	else
	{
		if (start_lsn == InvalidXLogRecPtr)
		{
			/* continue from last position */
			start_lsn = slot->data.confirmed_flush;
		}
		else if (start_lsn < slot->data.confirmed_flush)
		{
			/*
			 * It might seem like we should error out in this case, but it's
			 * pretty common for a client to acknowledge a LSN it doesn't have
			 * to do anything for, and thus didn't store persistently, because
			 * the xlog records didn't result in anyting relevant for
			 * changeset extraction. Clients have to be able to do that to
			 * support synchronous replication.
			 */
			start_lsn = slot->data.confirmed_flush;
			elog(DEBUG1, "cannot stream from %X/%X, minimum is %X/%X, forwarding",
				 (uint32)(start_lsn >> 32), (uint32)start_lsn,
				 (uint32)(slot->data.confirmed_flush >> 32),
				 (uint32)slot->data.confirmed_flush);
		}
	}

	/* load output plugins, so we detect a wrong output plugin now. */
	LoadOutputPlugin(&ctx->callbacks, NameStr(slot->data.plugin));

	/*
	 * Now that the slot's xmin has been set, we can announce ourselves as a
	 * logical decoding backend which doesn't need to be checked individually
	 * when computing the xmin horizon because the xmin is enforced via
	 * replication slots.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags |= PROC_IN_LOGICAL_DECODING;
	LWLockRelease(ProcArrayLock);

	ctx->slot = slot;

	ctx->reader = XLogReaderAllocate(read_page, ctx);
	ctx->reader->private_data = ctx;

	ctx->reorder = ReorderBufferAllocate();
	ctx->snapshot_builder =
		AllocateSnapshotBuilder(ctx->reorder, xmin_horizon, start_lsn);

	ctx->reorder->private_data = ctx;

	/* wrap output plugin callbacks, so we can add error context information */
	ctx->reorder->begin = begin_txn_wrapper;
	ctx->reorder->apply_change = change_wrapper;
	ctx->reorder->commit = commit_txn_wrapper;

	ctx->out = makeStringInfo();
	ctx->prepare_write = prepare_write;
	ctx->write = do_write;

	ctx->output_plugin_options = output_plugin_options;

	/* call output plugin initialization callback */
	if (ctx->callbacks.startup_cb != NULL)
		startup_slot_wrapper(ctx, &ctx->options, is_init);

	MemoryContextSwitchTo(old_context);

	ereport(LOG,
			(errmsg("starting changeset extraction in slot %s",
					NameStr(slot->data.name)),
			 errdetail("streaming transaction committing after %X/%X, reading WAL from %X/%X",
					   (uint32)(slot->data.confirmed_flush >> 32),
					   (uint32)slot->data.confirmed_flush,
					   (uint32)(slot->data.restart_lsn >> 32),
					   (uint32)slot->data.restart_lsn)));

	return ctx;
}

/*
 * Returns true if an consistent initial decoding snapshot has been built.
 */
bool
DecodingContextReady(LogicalDecodingContext *ctx)
{
	return SnapBuildCurrentState(ctx->snapshot_builder) == SNAPBUILD_CONSISTENT;
}

/*
 * Read from the decoding slot, until it is ready to start extracting changes.
 */
void
DecodingContextFindStartpoint(LogicalDecodingContext *ctx)
{
	XLogRecPtr	startptr;

	/* Initialize from where to start reading WAL. */
	startptr = ctx->slot->data.restart_lsn;

	elog(DEBUG1, "searching for intial changeset extraction starting point, starting at %X/%X",
		 (uint32)(ctx->slot->data.restart_lsn >> 32),
		 (uint32)ctx->slot->data.restart_lsn);

	/* Wait for a consistent starting point */
	for (;;)
	{
		XLogRecord *record;
		char	   *err = NULL;

		/*
		 * If the caller requires that interrupts be checked, the read_page
		 * callback should do so, as those will often wait.
		 */

		/* the read_page callback waits for new WAL */
		record = XLogReadRecord(ctx->reader, startptr, &err);
		if (err)
			elog(ERROR, "%s", err);

		Assert(record);

		startptr = InvalidXLogRecPtr;

		LogicalDecodingProcessRecord(ctx, record);

		/* only continue till we found a consistent spot */
		if (DecodingContextReady(ctx))
			break;
	}

	ctx->slot->data.confirmed_flush = ctx->reader->EndRecPtr;
}

/*
 * Free a previously allocated decoding context, invoking the shutdown
 * callback if necessary.
 */
void
FreeDecodingContext(LogicalDecodingContext *ctx)
{
	if (ctx->callbacks.shutdown_cb != NULL)
		shutdown_slot_wrapper(ctx);

	ReorderBufferFree(ctx->reorder);
	FreeSnapshotBuilder(ctx->snapshot_builder);
	XLogReaderFree(ctx->reader);
	MemoryContextDelete(ctx->context);
}

/*
 * Prepare a write using the context's output routine.
 */
void
OutputPluginPrepareWrite(struct LogicalDecodingContext *ctx, bool last_write)
{
	if (!ctx->accept_writes)
		elog(ERROR, "writes are only accepted in commit, begin and change callbacks");

	ctx->prepare_write(ctx, ctx->write_location, ctx->write_xid, last_write);
	ctx->prepared_write = true;
}

/*
 * Perform a write using the context's output routine.
 */
void
OutputPluginWrite(struct LogicalDecodingContext *ctx, bool last_write)
{
	if (!ctx->prepared_write)
		elog(ERROR, "OutputPluginPrepareWrite needs to be called before OutputPluginWrite");

	ctx->write(ctx, ctx->write_location, ctx->write_xid, last_write);
	ctx->prepared_write = false;
}

/*
 * Load the output plugin, lookup its output plugin init function, and check
 * that it provides the required callbacks.
 */
static void
LoadOutputPlugin(OutputPluginCallbacks *callbacks, char *plugin)
{
	LogicalOutputPluginInit plugin_init;

	plugin_init = (LogicalOutputPluginInit)
		load_external_function(plugin, "_PG_output_plugin_init", false, NULL);

	if (plugin_init == NULL)
		elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");

	/* ask the output plugin to fill the callback struct */
	plugin_init(callbacks);

	if (callbacks->begin_cb == NULL)
		elog(ERROR, "output plugins have to register a begin callback");
	if (callbacks->change_cb == NULL)
		elog(ERROR, "output plugins have to register a change callback");
	if (callbacks->commit_cb == NULL)
		elog(ERROR, "output plugins have to register a commit callback");
}

static void
output_plugin_error_callback(void *arg)
{
	LogicalErrorCallbackState *state = (LogicalErrorCallbackState *) arg;
	/* not all callbacks have an associated LSN  */
	if (state->report_location != InvalidXLogRecPtr)
		errcontext("slot \"%s\", output plugin \"%s\" during the %s callback, associated LSN %X/%X",
				   NameStr(state->ctx->slot->data.name),
				   NameStr(state->ctx->slot->data.plugin),
				   state->callback_name,
				   (uint32)(state->report_location >> 32),
				   (uint32)state->report_location);
	else
		errcontext("slot \"%s\", output plugin \"%s\" during the %s callback",
				   NameStr(state->ctx->slot->data.name),
				   NameStr(state->ctx->slot->data.plugin),
				   state->callback_name);
}

static void
startup_slot_wrapper(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback_name = "pg_decode_startup";
	state.report_location = InvalidXLogRecPtr;
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = false;

	/* do the actual work: call callback */
	ctx->callbacks.startup_cb(ctx, opt, is_init);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

static void
shutdown_slot_wrapper(LogicalDecodingContext *ctx)
{
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback_name = "pg_decode_shutdown";
	state.report_location = InvalidXLogRecPtr;
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = false;

	/* do the actual work: call callback */
	ctx->callbacks.shutdown_cb(ctx);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}


/*
 * Callbacks for ReorderBuffer which add in some more information and then call
 * output_plugin.h plugins.
 */
static void
begin_txn_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
	LogicalDecodingContext *ctx = cache->private_data;
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback_name = "pg_decode_begin_txn";
	state.report_location = txn->first_lsn;
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = true;
	ctx->write_xid = txn->xid;
	ctx->write_location = txn->first_lsn;

	/* do the actual work: call callback */
	ctx->callbacks.begin_cb(ctx, txn);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

static void
commit_txn_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
				   XLogRecPtr commit_lsn)
{
	LogicalDecodingContext *ctx = cache->private_data;
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback_name = "pg_decode_commit_txn";
	state.report_location = txn->final_lsn; /* beginning of commit record */
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = true;
	ctx->write_xid = txn->xid;
	ctx->write_location = txn->end_lsn; /* points to the end of the record */

	/* do the actual work: call callback */
	ctx->callbacks.commit_cb(ctx, txn, commit_lsn);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

static void
change_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
			   Relation relation, ReorderBufferChange *change)
{
	LogicalDecodingContext *ctx = cache->private_data;
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback_name = "pg_decode_change";
	state.report_location = change->lsn;
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = true;
	ctx->write_xid = txn->xid;
	/*
	 * report this change's lsn so replies from clients can give an up2date
	 * answer. This won't ever be enough (and shouldn't be!) to confirm
	 * receipt of this transaction, but it might allow another transaction's
	 * commit to be confirmed with one message.
	 */
	ctx->write_location = change->lsn;

	ctx->callbacks.change_cb(ctx, txn, relation, change);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

/*
 * Set the required catalog xmin horizon for historic snapshots in the current
 * replication slot.
 *
 * Note that in the most cases, we won't be able to immediately use the xmin
 * to increase the xmin horizon, we need to wait till the client has confirmed
 * receiving current_lsn with LogicalConfirmReceivedLocation().
 */
void
LogicalIncreaseXminForSlot(XLogRecPtr current_lsn, TransactionId xmin)
{
	bool	updated_xmin = false;

	Assert(MyReplicationSlot != NULL);

	SpinLockAcquire(&MyReplicationSlot->mutex);

	/*
	 * don't overwrite if we already have a newer xmin. This can
	 * happen if we restart decoding in a slot.
	 */
	if (TransactionIdPrecedesOrEquals(xmin, MyReplicationSlot->data.catalog_xmin))
	{
	}
	/*
	 * If the client has already confirmed up to this lsn, we directly
	 * can mark this as accepted. This can happen if we restart
	 * decoding in a slot.
	 */
	else if (current_lsn <= MyReplicationSlot->data.confirmed_flush)
	{
		MyReplicationSlot->candidate_catalog_xmin = xmin;
		MyReplicationSlot->candidate_xmin_lsn = current_lsn;

		/* our candidate can directly be used */
		updated_xmin = true;
	}
	/*
	 * Only increase if the previous values have been applied, otherwise we
	 * might never end up updating if the receiver acks too slowly.
	 */
	else if (MyReplicationSlot->candidate_xmin_lsn == InvalidXLogRecPtr)
	{
		MyReplicationSlot->candidate_catalog_xmin = xmin;
		MyReplicationSlot->candidate_xmin_lsn = current_lsn;
	}
	SpinLockRelease(&MyReplicationSlot->mutex);

	/* candidate already valid with the current flush position, apply */
	if (updated_xmin)
		LogicalConfirmReceivedLocation(MyReplicationSlot->data.confirmed_flush);
}

/*
 * Mark the minimal LSN (restart_lsn) we need to read to replay all
 * transactions that have not yet committed at current_lsn.
 *
 * Just like IncreaseRestartDecodingForSlot this nly takes effect when the
 * client has confirmed to have received current_lsn.
 */
void
LogicalIncreaseRestartDecodingForSlot(XLogRecPtr current_lsn, XLogRecPtr restart_lsn)
{
	bool	updated_lsn = false;

	Assert(MyReplicationSlot != NULL);
	Assert(restart_lsn != InvalidXLogRecPtr);
	Assert(current_lsn != InvalidXLogRecPtr);

	SpinLockAcquire(&MyReplicationSlot->mutex);

	/* don't overwrite if have a newer restart lsn*/
	if (restart_lsn <= MyReplicationSlot->data.restart_lsn)
	{
	}
	/*
	 * We might have already flushed far enough to directly accept this lsn, in
	 * this case there is no need to check for existing candidate LSNs
	 */
	else if (current_lsn <= MyReplicationSlot->data.confirmed_flush)
	{
		MyReplicationSlot->candidate_restart_valid = current_lsn;
		MyReplicationSlot->candidate_restart_lsn = restart_lsn;

		/* our candidate can directly be used */
		updated_lsn = true;
	}
	/*
	 * Only increase if the previous values have been applied, otherwise we
	 * might never end up updating if the receiver acks too slowly. A missed
	 * value here will just cause some extra effort after reconnecting.
	 */
	if (MyReplicationSlot->candidate_restart_valid == InvalidXLogRecPtr)
	{
		MyReplicationSlot->candidate_restart_valid = current_lsn;
		MyReplicationSlot->candidate_restart_lsn = restart_lsn;

		elog(DEBUG1, "got new restart lsn %X/%X at %X/%X",
			 (uint32) (restart_lsn >> 32), (uint32) restart_lsn,
			 (uint32) (current_lsn >> 32), (uint32) current_lsn);
	}
	else
	{
		elog(DEBUG1, "failed to increase restart lsn: proposed %X/%X, after %X/%X, current candidate %X/%X, current after %X/%X, flushed up to %X/%X",
			 (uint32) (restart_lsn >> 32), (uint32) restart_lsn,
			 (uint32) (current_lsn >> 32), (uint32) current_lsn,
			 (uint32) (MyReplicationSlot->candidate_restart_lsn >> 32),
			 (uint32) MyReplicationSlot->candidate_restart_lsn,
			 (uint32) (MyReplicationSlot->candidate_restart_valid >> 32),
			 (uint32) MyReplicationSlot->candidate_restart_valid,
			 (uint32) (MyReplicationSlot->data.confirmed_flush >> 32),
			 (uint32) MyReplicationSlot->data.confirmed_flush
			);
	}
	SpinLockRelease(&MyReplicationSlot->mutex);

	/* candidates are already valid with the current flush position, apply */
	if (updated_lsn)
		LogicalConfirmReceivedLocation(MyReplicationSlot->data.confirmed_flush);
}

/*
 * Handle a consumer's conformation having received all changes up to lsn.
 */
void
LogicalConfirmReceivedLocation(XLogRecPtr lsn)
{
	Assert(lsn != InvalidXLogRecPtr);

	/* Do an unlocked check for candidate_lsn first. */
	if (MyReplicationSlot->candidate_xmin_lsn != InvalidXLogRecPtr ||
		MyReplicationSlot->candidate_restart_valid != InvalidXLogRecPtr)
	{
		bool		updated_xmin = false;
		bool		updated_restart = false;

		/* use volatile pointer to prevent code rearrangement */
		volatile ReplicationSlot *slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);

		slot->data.confirmed_flush = lsn;

		/* if were past the location required for bumping xmin, do so */
		if (slot->candidate_xmin_lsn != InvalidXLogRecPtr &&
			slot->candidate_xmin_lsn <= lsn)
		{
			/*
			 * We have to write the changed xmin to disk *before* we change
			 * the in-memory value, otherwise after a crash we wouldn't know
			 * that some catalog tuples might have been removed already.
			 *
			 * Ensure that by first writing to ->xmin and only update
			 * ->effective_xmin once the new state is synced to disk. After a
			 * crash ->effective_xmin is set to ->xmin.
			 */
			if (TransactionIdIsValid(slot->candidate_catalog_xmin) &&
				slot->data.catalog_xmin != slot->candidate_catalog_xmin)
			{
				slot->data.catalog_xmin = slot->candidate_catalog_xmin;
				slot->candidate_catalog_xmin = InvalidTransactionId;
				slot->candidate_xmin_lsn = InvalidXLogRecPtr;
				updated_xmin = true;
			}
		}

		if (slot->candidate_restart_valid != InvalidXLogRecPtr &&
			slot->candidate_restart_valid <= lsn)
		{
			Assert(slot->candidate_restart_lsn != InvalidXLogRecPtr);

			slot->data.restart_lsn = slot->candidate_restart_lsn;
			slot->candidate_restart_lsn = InvalidXLogRecPtr;
			slot->candidate_restart_valid = InvalidXLogRecPtr;
			updated_restart = true;
		}

		SpinLockRelease(&slot->mutex);

		/* first write new xmin to disk, so we know whats up after a crash */
		if (updated_xmin || updated_restart)
		{
			ReplicationSlotMarkDirty();
			ReplicationSlotSave();
			elog(DEBUG1, "updated xmin: %u restart: %u", updated_xmin, updated_restart);
		}
		/*
		 * Now the new xmin is safely on disk, we can let the global value
		 * advance. We do not take ProcArrayLock or similar since we only
		 * advance xmin here and there's not much harm done by a concurrent
		 * computation missing that.
		 */
		if (updated_xmin)
		{
			SpinLockAcquire(&slot->mutex);
			slot->effective_catalog_xmin = slot->data.catalog_xmin;
			SpinLockRelease(&slot->mutex);

			ReplicationSlotsComputeRequiredXmin(false);
			ReplicationSlotsComputeRequiredLSN();
		}
	}
	else
	{
		volatile ReplicationSlot *slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);
		slot->data.confirmed_flush = lsn;
		SpinLockRelease(&slot->mutex);
	}
}
