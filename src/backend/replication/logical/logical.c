/*-------------------------------------------------------------------------
 *
 * logical.c
 *
 *     Logical decoding shared memory management
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/logical.c
 *
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/transam.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/fd.h"
#include "storage/copydir.h"

#include "utils/memutils.h"
#include "utils/syscache.h"

/* Control array for logical decoding */
LogicalDecodingCtlData *LogicalDecodingCtl = NULL;

/* My slot for logical rep in the shared memory array */
LogicalDecodingSlot *MyLogicalDecodingSlot = NULL;

/* user settable parameters */
int			max_logical_slots = 0;	/* the maximum number of logical slots */
RepNodeId	guc_replication_node_id = InvalidRepNodeId; /* local node id */
RepNodeId	guc_replication_origin_id = InvalidRepNodeId; /* assumed identity */

XLogRecPtr replication_origin_lsn;

static void LogicalSlotKill(int code, Datum arg);

/* persistency functions */
static void RestoreLogicalSlot(const char* name);
static void CreateLogicalSlot(LogicalDecodingSlot *slot);
static void SaveLogicalSlot(LogicalDecodingSlot *slot);
static void SaveLogicalSlotInternal(LogicalDecodingSlot *slot, const char *path);
static void DeleteLogicalSlot(LogicalDecodingSlot *slot);


/* Report shared-memory space needed by LogicalDecodingShmemInit */
Size
LogicalDecodingShmemSize(void)
{
	Size		size = 0;

	if (max_logical_slots == 0)
		return size;

	size = offsetof(LogicalDecodingCtlData, logical_slots);
	size = add_size(size,
					mul_size(max_logical_slots, sizeof(LogicalDecodingSlot)));

	return size;
}

/* Allocate and initialize walsender-related shared memory */
void
LogicalDecodingShmemInit(void)
{
	bool		found;

	if (max_logical_slots == 0)
		return;

	LogicalDecodingCtl = (LogicalDecodingCtlData *)
		ShmemInitStruct("Logical Decoding Ctl", LogicalDecodingShmemSize(),
						&found);

	if (!found)
	{
		int i;
		/* First time through, so initialize */
		MemSet(LogicalDecodingCtl, 0, LogicalDecodingShmemSize());

		LogicalDecodingCtl->xmin = InvalidTransactionId;

		for (i = 0; i < max_logical_slots; i++)
		{
			LogicalDecodingSlot *slot =
				&LogicalDecodingCtl->logical_slots[i];
			slot->xmin = InvalidTransactionId;
			slot->effective_xmin = InvalidTransactionId;
			SpinLockInit(&slot->mutex);
		}
	}
}

static void
LogicalSlotKill(int code, Datum arg)
{
	/* LOCK? */
	if(MyLogicalDecodingSlot && MyLogicalDecodingSlot->active)
	{
		MyLogicalDecodingSlot->active = false;
	}
	MyLogicalDecodingSlot = NULL;
}

/*
 * Set the xmin required for catalog timetravel for the specific decoding slot.
 */
void
IncreaseLogicalXminForSlot(XLogRecPtr lsn, TransactionId xmin)
{
	Assert(MyLogicalDecodingSlot != NULL);

	SpinLockAcquire(&MyLogicalDecodingSlot->mutex);

	/*
	 * Only increase if the previous values have been applied...
	 */
	if (!MyLogicalDecodingSlot->candidate_lsn != InvalidXLogRecPtr)
	{
		MyLogicalDecodingSlot->candidate_lsn = lsn;
		MyLogicalDecodingSlot->candidate_xmin = xmin;
		elog(LOG, "got new xmin %u at %X/%X", xmin,
			 (uint32)(lsn >> 32), (uint32)lsn); /* FIXME: log level */
	}
	SpinLockRelease(&MyLogicalDecodingSlot->mutex);
}

void
LogicalConfirmReceivedLocation(XLogRecPtr lsn)
{
	Assert(lsn != InvalidXLogRecPtr);

	/* Do an unlocked check for candidate_lsn first.*/
	if (MyLogicalDecodingSlot->candidate_lsn != InvalidXLogRecPtr)
	{
		bool updated_xmin = false;
		bool updated_restart = false;

		/* use volatile pointer to prevent code rearrangement */
		volatile LogicalDecodingSlot *slot = MyLogicalDecodingSlot;

		SpinLockAcquire(&slot->mutex);

		slot->confirmed_flush = lsn;

		/* if were past the location required for bumping xmin, do so */
		if (slot->candidate_lsn != InvalidXLogRecPtr &&
			slot->candidate_lsn < lsn)
		{
			/*
			 * We have to write the changed xmin to disk *before* we change the
			 * in-memory value, otherwise after a crash we wouldn't know that
			 * some catalog tuples might have been removed already.
			 *
			 * Ensure that by first writing to ->xmin and only update
			 * ->effective_xmin once the new state is fsynced to disk. After a
			 * crash ->effective_xmin is set to ->xmin.
			 */
			if (slot->xmin != slot->candidate_xmin)
			{
				slot->xmin = slot->candidate_xmin;
				updated_xmin = true;
			}

			if (slot->restart_decoding != slot->candidate_restart_decoding)
			{
				slot->restart_decoding = slot->candidate_restart_decoding;
				updated_restart = true;
			}

			slot->candidate_lsn = InvalidXLogRecPtr;
			slot->candidate_xmin = InvalidTransactionId;
			slot->candidate_restart_decoding = InvalidXLogRecPtr;
		}

		SpinLockRelease(&slot->mutex);

		/* first write new xmin to disk, so we know whats up after a crash */
		if (updated_xmin || updated_restart)
			/* cast away volatile, thats ok. */
			SaveLogicalSlot((LogicalDecodingSlot *) slot);

		/*
		 * now the new xmin is safely on disk, we can let the global value
		 * advance
		 */
		if (updated_xmin)
		{
			SpinLockAcquire(&slot->mutex);
			slot->effective_xmin = slot->xmin;
			SpinLockRelease(&slot->mutex);

			ComputeLogicalXmin();
		}
	}
	else
	{
		volatile LogicalDecodingSlot *slot = MyLogicalDecodingSlot;
		SpinLockAcquire(&slot->mutex);
		slot->confirmed_flush = lsn;
		SpinLockRelease(&slot->mutex);
	}
}

/*
 * Compute the xmin between all of the decoding slots and store it in
 * WalSndCtlData.
 */
void
ComputeLogicalXmin(void)
{
	int i;
	TransactionId xmin = InvalidTransactionId;
	LogicalDecodingSlot *slot;

	Assert(LogicalDecodingCtl);

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	for (i = 0; i < max_logical_slots; i++)
	{
		slot = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use &&
			TransactionIdIsValid(slot->effective_xmin) && (
				!TransactionIdIsValid(xmin) ||
				TransactionIdPrecedes(slot->effective_xmin, xmin))
			)
		{
			xmin = slot->effective_xmin;
		}
		SpinLockRelease(&slot->mutex);
	}
	LogicalDecodingCtl->xmin = xmin;
	LWLockRelease(ProcArrayLock);

	elog(LOG, "computed new global xmin for decoding: %u", xmin);
}

/*
 * Make sure the current settings & environment is capable of doing logical
 * replication.
 */
void
CheckLogicalReplicationRequirements(void)
{
	if (wal_level < WAL_LEVEL_LOGICAL)
		ereport(ERROR, (errmsg("logical replication requires wal_level=logical")));

	if (MyDatabaseId == InvalidOid)
		ereport(ERROR, (errmsg("logical replication requires to be connected to a database")));

	if (max_logical_slots == 0)
		ereport(ERROR, (errmsg("logical replication requires needs max_logical_slots > 0")));
}

/*
 * Search for a free slot, mark it as used and acquire a valid xmin horizon
 * value.
 */
void LogicalDecodingAcquireFreeSlot(const char *name, const char *plugin)
{
	LogicalDecodingSlot *slot;
	bool name_in_use;
	int i;

	Assert(!MyLogicalDecodingSlot);

	CheckLogicalReplicationRequirements();

	LWLockAcquire(LogicalReplicationCtlLock, LW_EXCLUSIVE);

	/* First, make sure the requested name is not in use. */

	name_in_use = false;
	for (i = 0; i < max_logical_slots && !name_in_use; i++)
	{
		LogicalDecodingSlot *s = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&s->mutex);
		if (s->in_use && strcmp(name, NameStr(s->name)) == 0)
			name_in_use = true;
		SpinLockRelease(&s->mutex);
	}

	if (name_in_use)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("There already is a logical slot named \"%s\"", name)));

	/* Find the first available (not in_use (=> not active)) slot. */

	slot = NULL;
	for (i = 0; i < max_logical_slots; i++)
	{
		LogicalDecodingSlot *s = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&s->mutex);
		if (!s->in_use)
		{
			Assert(!s->active);
			/* NOT releasing the lock yet */
			slot = s;
			break;
		}
		SpinLockRelease(&s->mutex);
	}

	LWLockRelease(LogicalReplicationCtlLock);

	if (!slot)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("couldn't find free logical slot. free one or increase max_logical_slots")));

	MyLogicalDecodingSlot = slot;

	slot->last_required_checkpoint = GetRedoRecPtr();
	slot->in_use = true;
	slot->active = true;
	slot->database = MyDatabaseId;
	/* XXX: do we want to use truncate identifier instead? */
	strncpy(NameStr(slot->plugin), plugin, NAMEDATALEN);
	NameStr(slot->plugin)[NAMEDATALEN-1] = '\0';
	strncpy(NameStr(slot->name), name, NAMEDATALEN);
	NameStr(slot->name)[NAMEDATALEN-1] = '\0';

	/* Arrange to clean up at exit/error */
	on_shmem_exit(LogicalSlotKill, 0);

	/* release slot so it can be examined by others */
	SpinLockRelease(&slot->mutex);

	/* XXX: verify that the specified plugin is valid */

	/*
	 * Acquire the current global xmin value and directly set the logical xmin
	 * before releasing the lock if necessary. We do this so wal decoding is
	 * guaranteed to have all catalog rows produced by xacts with an xid >
	 * walsnd->xmin available.
	 *
	 * We can't use ComputeLogicalXmin here as that acquires ProcArrayLock
	 * separately which would open a short window for the global xmin to
	 * advance above walsnd->xmin.
	 */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	slot->effective_xmin = GetOldestXmin(true, true, true);
	slot->xmin = slot->effective_xmin;

	if (!TransactionIdIsValid(LogicalDecodingCtl->xmin) ||
		NormalTransactionIdPrecedes(slot->effective_xmin, LogicalDecodingCtl->xmin))
		LogicalDecodingCtl->xmin = slot->effective_xmin;
	LWLockRelease(ProcArrayLock);

	Assert(slot->effective_xmin <= GetOldestXmin(true, true, false));

	CreateLogicalSlot(slot);
}

/*
 * Find an previously initiated slot and mark it as used again.
 */
void LogicalDecodingReAcquireSlot(const char *name)
{
	LogicalDecodingSlot *slot;
	int i;

	CheckLogicalReplicationRequirements();

	Assert(!MyLogicalDecodingSlot);

	for (i = 0; i < max_logical_slots; i++)
	{
		slot = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use && strcmp(name, NameStr(slot->name)) == 0)
		{
			MyLogicalDecodingSlot = slot;
			/* NOT releasing the lock yet */
			break;
		}
		SpinLockRelease(&slot->mutex);
	}

	if (!MyLogicalDecodingSlot)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("couldn't find logical slot \"%s\"", name)));

	slot = MyLogicalDecodingSlot;

	if (slot->active)
	{
		SpinLockRelease(&slot->mutex);
		MyLogicalDecodingSlot = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("slot already active")));
	}

	slot->active = true;
	/* now that we've marked it as active, we release our lock */
	SpinLockRelease(&slot->mutex);

	/* Don't let the user switch the database... */
	if (slot->database != MyDatabaseId)
	{
		SpinLockAcquire(&slot->mutex);
		slot->active = false;
		SpinLockRelease(&slot->mutex);

		ereport(ERROR,
		        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
		         (errmsg("START_LOGICAL_REPLICATION needs to be run in the same database as INIT_LOGICAL_REPLICATION"))));
	}

	/* Arrange to clean up at exit */
	on_shmem_exit(LogicalSlotKill, 0);

	SaveLogicalSlot(slot);
}

void
LogicalDecodingReleaseSlot(void)
{
	LogicalDecodingSlot *slot;

	CheckLogicalReplicationRequirements();

	slot = MyLogicalDecodingSlot;

	Assert(slot != NULL && slot->active);

	SpinLockAcquire(&slot->mutex);
	slot->active = false;
	SpinLockRelease(&slot->mutex);

	MyLogicalDecodingSlot = NULL;

	SaveLogicalSlot(slot);

	cancel_shmem_exit(LogicalSlotKill, 0);
}

void
LogicalDecodingFreeSlot(const char *name)
{
	LogicalDecodingSlot *slot = NULL;
	int i;

	CheckLogicalReplicationRequirements();

	for (i = 0; i < max_logical_slots; i++)
	{
		slot = &LogicalDecodingCtl->logical_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use && strcmp(name, NameStr(slot->name)) == 0)
		{
			/* NOT releasing the lock yet */
			break;
		}
		SpinLockRelease(&slot->mutex);
		slot = NULL;
	}

	if (!slot)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("couldn't find logical slot \"%s\"", name)));

	if (slot->active)
	{
		SpinLockRelease(&slot->mutex);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("cannot free active logical slot \"%s\"", name)));
	}

	/*
	 * Mark it as as active, so nobody can claim this slot while we are working
	 * on it. We don't want to hold the spinlock while doing stuff like
	 * fsyncing the state file to disk.
	 */
	slot->active = true;

	SpinLockRelease(&slot->mutex);

	/*
	 * Start critical section, we can't to be interrupted while on-disk/memory
	 * state aren't coherent.
	 */
	START_CRIT_SECTION();

	DeleteLogicalSlot(slot);

	/* ok, everything gone, after a crash we now wouldn't restore this slot */
	SpinLockAcquire(&slot->mutex);
	slot->active = false;
	slot->in_use = false;
	SpinLockRelease(&slot->mutex);

	END_CRIT_SECTION();

	/* slot is dead and doesn't nail the xmin anymore */
	ComputeLogicalXmin();
}

/*
 * Load replication state from disk into memory
 */
void
StartupLogicalReplication(XLogRecPtr checkPointRedo)
{
	DIR		   *logical_dir;
	struct dirent *logical_de;

	elog(LOG, "doing logical startup from %X/%X",
		 (uint32)(checkPointRedo >> 32), (uint32)checkPointRedo);

	/* restore all slots */
	logical_dir = AllocateDir("pg_llog");
	while ((logical_de = ReadDir(logical_dir, "pg_llog")) != NULL)
	{
		if (strcmp(logical_de->d_name, ".") == 0 ||
			strcmp(logical_de->d_name, "..") == 0)
			continue;

		/* one of our own directories */
		if (strcmp(logical_de->d_name, "snapshots") == 0)
			continue;

		if (strcmp(logical_de->d_name, "checkpoints") == 0)
			continue;

		/* we crashed while a slot was being setup or deleted, clean up */
		if (strcmp(logical_de->d_name, "new") == 0 ||
			strcmp(logical_de->d_name, "old") == 0)
		{
			char path[MAXPGPATH];
			sprintf(path, "pg_llog/%s", logical_de->d_name);

			if (!rmtree(path, true))
			{
				FreeDir(logical_dir);
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not remove directory \"%s\": %m",
						        path)));
			}
			continue;
		}

		RestoreLogicalSlot(logical_de->d_name);
	}
	FreeDir(logical_dir);

	if (max_logical_slots <= 0)
		return;

	/* FIXME: should we up minRecoveryLSN? */

	/* Now that we have recovered all the data, compute logical xmin */
	ComputeLogicalXmin();

	ReorderBufferStartup();
}

static void
CreateLogicalSlot(LogicalDecodingSlot *slot)
{
	char tmppath[MAXPGPATH];
	char path[MAXPGPATH];

	START_CRIT_SECTION();

	sprintf(tmppath, "pg_llog/new");
	sprintf(path, "pg_llog/%s", NameStr(slot->name));

	if (mkdir(tmppath, S_IRWXU) < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m",
						tmppath)));

	fsync_fname(tmppath, true);

	SaveLogicalSlotInternal(slot, tmppath);

	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename logical checkpoint from \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	fsync_fname(path, true);

	END_CRIT_SECTION();
}

static void
SaveLogicalSlot(LogicalDecodingSlot *slot)
{
	char path[MAXPGPATH];
	sprintf(path, "pg_llog/%s", NameStr(slot->name));
	SaveLogicalSlotInternal(slot, path);
}

static void
SaveLogicalSlotInternal(LogicalDecodingSlot *slot, const char *dir)
{
	char tmppath[MAXPGPATH];
	char path[MAXPGPATH];
	int fd;
	LogicalDecodingCheckpointData cp;

	/* silence valgrind :( */
	memset(&cp, 0, sizeof(LogicalDecodingCheckpointData));

	sprintf(tmppath, "%s/state.tmp", dir);
	sprintf(path, "%s/state", dir);

	START_CRIT_SECTION();

	fd = OpenTransientFile(tmppath,
						   O_CREAT | O_EXCL | O_WRONLY | PG_BINARY,
						   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create logical checkpoint file \"%s\": %m",
						tmppath)));

	cp.magic = LOGICAL_MAGIC;

	SpinLockAcquire(&slot->mutex);

	cp.slot.xmin = slot->xmin;
	cp.slot.effective_xmin = slot->effective_xmin;

	strcpy(NameStr(cp.slot.name), NameStr(slot->name));
	strcpy(NameStr(cp.slot.plugin), NameStr(slot->plugin));

	cp.slot.database = slot->database;
	cp.slot.last_required_checkpoint = slot->last_required_checkpoint;
	cp.slot.confirmed_flush = slot->confirmed_flush;
	cp.slot.restart_decoding = slot->restart_decoding;
	cp.slot.candidate_lsn = InvalidXLogRecPtr;
	cp.slot.candidate_xmin = InvalidTransactionId;
	cp.slot.candidate_restart_decoding = InvalidXLogRecPtr;
	cp.slot.in_use = slot->in_use;
	cp.slot.active = false;

	SpinLockRelease(&slot->mutex);

	if ((write(fd, &cp, sizeof(cp))) != sizeof(cp))
	{
		CloseTransientFile(fd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write logical checkpoint file \"%s\": %m",
						tmppath)));
	}

	/* fsync the file */
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync logical checkpoint \"%s\": %m",
						tmppath)));
	}

	CloseTransientFile(fd);

	/* rename to permanent file, fsync file and directory */
	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename logical checkpoint from \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	fsync_fname((char *)dir, true);
	fsync_fname(path, false);

	END_CRIT_SECTION();
}

static void
RestoreLogicalSlot(const char* name)
{
	LogicalDecodingCheckpointData cp;
	int i;
	char path[MAXPGPATH];
	int fd;
	bool restored = false;
	int readBytes;

	START_CRIT_SECTION();

	/* delete temp file if it exists */
	sprintf(path, "pg_llog/%s/state.tmp", name);
	if (unlink(path) < 0 && errno != ENOENT)
		ereport(PANIC, (errmsg("failed while unlinking %s",  path)));

	sprintf(path, "pg_llog/%s/state", name);

	elog(LOG, "restoring logical slot at %s", path);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);

	/*
	 * We do not need to handle this as we are rename()ing the directory into
	 * place only after we fsync()ed the state file.
	 */
	if (fd < 0)
		ereport(PANIC, (errmsg("could not open state file %s",  path)));

	readBytes = read(fd, &cp, sizeof(cp));
	if (readBytes != sizeof(cp))
	{
		int saved_errno = errno;
		CloseTransientFile(fd);
		errno = saved_errno;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read logical checkpoint file \"%s\": %m, read %d of %zu",
				        path, readBytes, sizeof(cp))));
	}

	CloseTransientFile(fd);

	if (cp.magic != LOGICAL_MAGIC)
		ereport(PANIC, (errmsg("Logical checkpoint has wrong magic %u instead of %u",
							   cp.magic, LOGICAL_MAGIC)));

	/* nothing can be active yet, don't lock anything */
	for (i = 0; i < max_logical_slots; i++)
	{
		LogicalDecodingSlot *slot;
		slot = &LogicalDecodingCtl->logical_slots[i];

		if (slot->in_use)
			continue;

		slot->xmin = cp.slot.xmin;
		/* XXX: after a crash, always use xmin, not effective_xmin */
		slot->effective_xmin = cp.slot.xmin;
		strcpy(NameStr(slot->name), NameStr(cp.slot.name));
		strcpy(NameStr(slot->plugin), NameStr(cp.slot.plugin));
		slot->database = cp.slot.database;
		slot->last_required_checkpoint = cp.slot.last_required_checkpoint;
		slot->restart_decoding = cp.slot.restart_decoding;
		slot->confirmed_flush = cp.slot.confirmed_flush;
		slot->candidate_lsn = InvalidXLogRecPtr;
		slot->candidate_xmin = InvalidTransactionId;
		slot->candidate_restart_decoding = InvalidXLogRecPtr;
		slot->in_use = true;
		slot->active = false;
		restored = true;

		/*
		 * FIXME: Do some validation here.
		 */
		break;
	}

	if (!restored)
		ereport(PANIC,
				(errmsg("too many logical slots active before shutdown, increase max_logical_slots and try again")));

	END_CRIT_SECTION();
}

static void
DeleteLogicalSlot(LogicalDecodingSlot *slot)
{
	char path[MAXPGPATH];
	char tmppath[] = "pg_llog/old";

	START_CRIT_SECTION();

	sprintf(path, "pg_llog/%s", NameStr(slot->name));

	if (rename(path, tmppath) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename logical checkpoint from \"%s\" to \"%s\": %m",
						path, tmppath)));
	}

	/* make sure no partial state is visible after a crash */
	fsync_fname(tmppath, true);
	fsync_fname("pg_llog", true);

	if (!rmtree(tmppath, true))
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\": %m",
						tmppath)));
	}

	END_CRIT_SECTION();
}


static void
LoadOutputPlugin(OutputPluginCallbacks *callbacks, char *plugin)
{
	/* lookup symbols in the shared libarary */

	/* optional */
	callbacks->init_cb = (LogicalDecodeInitCB)
		load_external_function(plugin, "pg_decode_init", false, NULL);

	/* required */
	callbacks->begin_cb = (LogicalDecodeBeginCB)
		load_external_function(plugin, "pg_decode_begin_txn", true, NULL);

	/* required */
	callbacks->change_cb = (LogicalDecodeChangeCB)
		load_external_function(plugin, "pg_decode_change", true, NULL);

	/* required */
	callbacks->commit_cb = (LogicalDecodeCommitCB)
		load_external_function(plugin, "pg_decode_commit_txn", true, NULL);

	/* optional */
	callbacks->cleanup_cb = (LogicalDecodeCleanupCB)
		load_external_function(plugin, "pg_decode_clean", false, NULL);
}


/*
 * Callbacks for ReorderBuffer which add in some more information and then call
 * output_plugin.h plugins.
 */
static void
begin_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn)
{
	LogicalDecodingContext *ctx = cache->private_data;
	ctx->callbacks.begin_cb(ctx, txn);
}

static void
commit_txn_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	LogicalDecodingContext *ctx = cache->private_data;

	ctx->callbacks.commit_cb(ctx, txn, commit_lsn);
}

static void
change_wrapper(ReorderBuffer* cache, ReorderBufferTXN* txn,
               Relation relation, ReorderBufferChange* change)
{
	LogicalDecodingContext *ctx = cache->private_data;
	ctx->callbacks.change_cb(ctx, txn, relation, change);
}

LogicalDecodingContext *
CreateLogicalDecodingContext(
	LogicalDecodingSlot *slot,
	bool is_init,
	List *output_plugin_options,
	XLogPageReadCB read_page,
	LogicalOutputPluginWriterPrepareWrite prepare_write,
	LogicalOutputPluginWriterWrite do_write)
{
	MemoryContext context  =
		AllocSetContextCreate(TopMemoryContext,
							  "ReorderBuffer",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext old_context = MemoryContextSwitchTo(context);

	LogicalDecodingContext *ctx =
		MemoryContextAllocZero(context, sizeof(LogicalDecodingContext));


	/* load output plugins first, so we detect a wrong output plugin early */
	LoadOutputPlugin(&ctx->callbacks, NameStr(slot->plugin));

	ctx->slot = slot;

	ctx->reader = XLogReaderAllocate(read_page, ctx);
	ctx->reader->private_data = ctx;

	ctx->reorder = ReorderBufferAllocate();
	ctx->snapshot_builder = AllocateSnapshotBuilder(ctx->reorder);
	ctx->snapshot_builder->initial_xmin_horizon = InvalidTransactionId;

	ctx->reorder->private_data = ctx;

	ctx->reorder->begin = begin_txn_wrapper;
	ctx->reorder->apply_change = change_wrapper;
	ctx->reorder->commit = commit_txn_wrapper;

	ctx->out = makeStringInfo();
	ctx->prepare_write = prepare_write;
	ctx->write = do_write;

	ctx->output_plugin_options = output_plugin_options;

	if (is_init)
	{
		ctx->snapshot_builder->initial_xmin_horizon = ctx->slot->xmin;
		ctx->stop_after_consistent = true;
	}
	else
	{
		ctx->stop_after_consistent = false;
	}

	/* call output plugin initialization callback */
	if (ctx->callbacks.init_cb != NULL)
		ctx->callbacks.init_cb(ctx, is_init);

	MemoryContextSwitchTo(old_context);

	return ctx;
}

void
FreeLogicalDecodingContext(LogicalDecodingContext *ctx)
{
	if (ctx->callbacks.cleanup_cb != NULL)
		ctx->callbacks.cleanup_cb(ctx);
}


/* has the initial snapshot found a consistent state? */
bool
LogicalDecodingContextReady(LogicalDecodingContext *ctx)
{
	return ctx->snapshot_builder->state == SNAPBUILD_CONSISTENT;
}
