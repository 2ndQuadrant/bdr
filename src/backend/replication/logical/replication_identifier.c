/*-------------------------------------------------------------------------
 *
 * replication_identifier.c
 *	  Logical Replication Node Identifier and replication progress persistency
 *	  support.
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/replication_identifier.c
 *
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "funcapi.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"

#include "nodes/execnodes.h"

#include "replication/replication_identifier.h"
#include "replication/logical.h"

#include "storage/fd.h"
#include "storage/copydir.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/*
 * Replay progress of a single remote node.
 */
typedef struct ReplicationState
{
	/*
	 * Local identifier for the remote node.
	 */
	RepNodeId	local_identifier;

	/*
	 * Location of the latest commit from the remote side.
	 */
	XLogRecPtr	remote_lsn;

	/*
	 * Remember the local lsn of the commit record so we can XLogFlush() to it
	 * during a checkpoint so we know the commit record actually is safe on
	 * disk.
	 */
	XLogRecPtr	local_lsn;
} ReplicationState;

/*
 * Base address into a shared memory array of replication states of size
 * max_replication_slots.
 * XXX: Should we use a separate variable to size this than max_replication_slots?
 */
static ReplicationState *ReplicationStates;

/*
 * Backend-local, cached element from ReplicationStates for use in a backend
 * replaying remote commits, so we don't have to search ReplicationStates for
 * the backends current RepNodeId.
 */
static ReplicationState *local_replication_state = NULL;

/* Magic for on disk files. */
#define REPLICATION_STATE_MAGIC (uint32)0x1257DADE

/* XXX: move to c.h? */
#ifndef UINT16_MAX
#define UINT16_MAX ((1<<16) - 1)
#else
#if UINT16_MAX != ((1<<16) - 1)
#error "uh, wrong UINT16_MAX?"
#endif
#endif

/*
 * Check for a persistent repication identifier identified by remotesysid,
 * remotetli, remotedb, riname, rilocaldb.
 *
 * Returns InvalidOid if the node isn't known yet.
 */
RepNodeId
GetReplicationIdentifier(char *riname, bool missing_ok)
{
	Form_pg_replication_identifier ident;
	Oid		riident = InvalidOid;
	HeapTuple tuple;
	Datum	riname_d;

	riname_d = CStringGetTextDatum(riname);

	tuple = SearchSysCache1(REPLIDREMOTE, riname_d);
	if (HeapTupleIsValid(tuple))
	{
		ident = (Form_pg_replication_identifier)GETSTRUCT(tuple);
		riident = ident->riident;
		ReleaseSysCache(tuple);
	}
	else if (!missing_ok)
		elog(ERROR, "cache lookup failed for replication identifier named %s",
			riname);

	return riident;
}

/*
 * Create a persistent replication identifier.
 *
 * Needs to be called in a transaction.
 */
RepNodeId
CreateReplicationIdentifier(char *riname)
{
	Oid		riident;
	HeapTuple tuple = NULL;
	Relation rel;
	Datum	riname_d;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;

	riname_d = CStringGetTextDatum(riname);

	Assert(IsTransactionState());

	/*
	 * We need the numeric replication identifiers to be 16bit wide, so we
	 * cannot rely on the normal oid allocation. So we simply scan
	 * pg_replication_identifier for the first unused id. That's not
	 * particularly efficient, but this should be an fairly infrequent
	 * operation - we can easily spend a bit more code when it turns out it
	 * should be faster.
	 *
	 * We handle concurrency by taking an exclusive lock (allowing reads!)
	 * over the table for the duration of the search. Because we use a "dirty
	 * snapshot" we can read rows that other in-progress sessions have
	 * written, even though they would be invisible with normal snapshots. Due
	 * to the exclusive lock there's no danger that new rows can appear while
	 * we're checking.
	 */
	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationIdentifierRelationId, ExclusiveLock);

	for (riident = InvalidOid + 1; riident < UINT16_MAX; riident++)
	{
		bool		nulls[Natts_pg_replication_identifier];
		Datum		values[Natts_pg_replication_identifier];
		bool		collides;
		CHECK_FOR_INTERRUPTS();

		ScanKeyInit(&key,
					Anum_pg_replication_riident,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(riident));

		scan = systable_beginscan(rel, ReplicationLocalIdentIndex,
								  true /* indexOK */,
								  &SnapshotDirty,
								  1, &key);

		collides = HeapTupleIsValid(systable_getnext(scan));

		systable_endscan(scan);

		if (!collides)
		{
			/*
			 * Ok, found an unused riident, insert the new row and do a CCI,
			 * so our callers can look it up if they want to.
			 */
			memset(&nulls, 0, sizeof(nulls));

			values[Anum_pg_replication_riident -1] = ObjectIdGetDatum(riident);
			values[Anum_pg_replication_riname - 1] = riname_d;

			tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
			simple_heap_insert(rel, tuple);
			CatalogUpdateIndexes(rel, tuple);
			CommandCounterIncrement();
			break;
		}
	}

	/* now release lock again,  */
	heap_close(rel, ExclusiveLock);

	if (tuple == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("no free replication id could be found")));

	heap_freetuple(tuple);
	return riident;
}


/*
 * Create a persistent replication identifier.
 *
 * Needs to be called in a transaction.
 */
void
DropReplicationIdentifier(RepNodeId riident)
{
	HeapTuple tuple = NULL;
	Relation rel;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;
	int			i;

	Assert(IsTransactionState());

	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationIdentifierRelationId, ExclusiveLock);

	/* cleanup the slot state info */
	for (i = 0; i < max_replication_slots; i++)
	{
		/* found our slot */
		if (ReplicationStates[i].local_identifier == riident)
		{
			memset(&ReplicationStates[i], 0, sizeof(ReplicationState));
			break;
		}
	}

	ScanKeyInit(&key,
				Anum_pg_replication_riident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(riident));

	scan = systable_beginscan(rel, ReplicationLocalIdentIndex,
							  true /* indexOK */,
							  &SnapshotDirty,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
		simple_heap_delete(rel, &tuple->t_self);

	systable_endscan(scan);

	CommandCounterIncrement();

	/* now release lock again,  */
	heap_close(rel, ExclusiveLock);
}


/*
 * Lookup pg_replication_identifier tuple via its riident.
 *
 * The result needs to be ReleaseSysCache'ed and is an invalid HeapTuple if
 * the lookup failed.
 */
void
GetReplicationInfoByIdentifier(RepNodeId riident, bool missing_ok, char **riname)
{
	HeapTuple tuple;
	Form_pg_replication_identifier ric;

	Assert(OidIsValid((Oid) riident));
	Assert(riident < UINT16_MAX);
	tuple = SearchSysCache1(REPLIDIDENT,
							ObjectIdGetDatum((Oid) riident));

	if (HeapTupleIsValid(tuple))
	{
		ric = (Form_pg_replication_identifier) GETSTRUCT(tuple);
		*riname = pstrdup(text_to_cstring(&ric->riname));
	}

	if (!HeapTupleIsValid(tuple) && !missing_ok)
		elog(ERROR, "cache lookup failed for replication identifier id: %u",
			 riident);

	ReleaseSysCache(tuple);
}

static void
CheckReplicationIdentifierPrerequisites(bool check_slots)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superusers can query or manipulate replication identifiers")));

	if (check_slots && max_replication_slots == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot query or manipulate replication identifiers when max_replication_slots = 0")));

}

Datum
pg_replication_identifier_get(PG_FUNCTION_ARGS)
{
	char *name;
	RepNodeId riident;

	CheckReplicationIdentifierPrerequisites(false);

	name = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(0)));
	riident = GetReplicationIdentifier(name, true);

	pfree(name);

	if (OidIsValid(riident))
		PG_RETURN_OID(riident);
	PG_RETURN_NULL();
}


Datum
pg_replication_identifier_create(PG_FUNCTION_ARGS)
{
	char *name;
	RepNodeId riident;

	CheckReplicationIdentifierPrerequisites(false);

	name = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(0)));
	riident = CreateReplicationIdentifier(name);

	pfree(name);

	PG_RETURN_OID(riident);
}

Datum
pg_replication_identifier_setup_replaying_from(PG_FUNCTION_ARGS)
{
	char *name;
	RepNodeId origin;

	CheckReplicationIdentifierPrerequisites(true);

	name = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(0)));
	origin = GetReplicationIdentifier(name, false);
	SetupCachedReplicationIdentifier(origin);

	replication_origin_id = origin;

	pfree(name);

	PG_RETURN_VOID();
}

Datum
pg_replication_identifier_is_replaying(PG_FUNCTION_ARGS)
{
	CheckReplicationIdentifierPrerequisites(true);

	PG_RETURN_BOOL(replication_origin_id != InvalidRepNodeId);
}

Datum
pg_replication_identifier_reset_replaying_from(PG_FUNCTION_ARGS)
{
	CheckReplicationIdentifierPrerequisites(true);

	TeardownCachedReplicationIdentifier();

	replication_origin_id = InvalidRepNodeId;

	PG_RETURN_VOID();
}


Datum
pg_replication_identifier_setup_tx_origin(PG_FUNCTION_ARGS)
{
	text	   *location = PG_GETARG_TEXT_P(0);
	char	   *locationstr;
	uint32		hi,
				lo;

	CheckReplicationIdentifierPrerequisites(true);

	locationstr = text_to_cstring(location);

	if (sscanf(locationstr, "%X/%X", &hi, &lo) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse transaction log location \"%s\"",
						locationstr)));

	if (local_replication_state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("need to setup the origin id first")));

	replication_origin_lsn = ((uint64) hi) << 32 | lo;
	replication_origin_timestamp = PG_GETARG_TIMESTAMPTZ(1);

	pfree(locationstr);

	PG_RETURN_VOID();
}

Datum
pg_get_replication_identifier_progress(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int			i;
#define REPLICATION_IDENTIFIER_PROGRESS_COLS 4

	CheckReplicationIdentifierPrerequisites(true);

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (tupdesc->natts != REPLICATION_IDENTIFIER_PROGRESS_COLS)
		elog(ERROR, "wrong function definition");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Iterate through all possible ReplicationStates, display if they are
	 * filled. Note that we do not take any locks, so slightly corrupted/out
	 * of date values are a possibility.
	 */
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationState *state;
		Datum		values[REPLICATION_IDENTIFIER_PROGRESS_COLS];
		bool		nulls[REPLICATION_IDENTIFIER_PROGRESS_COLS];
		char		location[MAXFNAMELEN];
		char		*riname;

		state = &ReplicationStates[i];

		/* unused slot, nothing to display */
		if (state->local_identifier == InvalidRepNodeId)
			continue;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[ 0] = ObjectIdGetDatum(state->local_identifier);

		GetReplicationInfoByIdentifier(state->local_identifier, true, &riname);

		/*
		 * We're not preventing the identifier to be dropped concurrently, so
		 * silently accept that it might be gone.
		 */
		if (!riname)
			continue;

		values[ 1] = CStringGetTextDatum(riname);

		snprintf(location, sizeof(location), "%X/%X",
				 (uint32) (state->remote_lsn >> 32), (uint32) state->remote_lsn);
		values[ 2] = CStringGetTextDatum(location);
		snprintf(location, sizeof(location), "%X/%X",
				 (uint32) (state->local_lsn >> 32), (uint32) state->local_lsn);
		values[ 3] = CStringGetTextDatum(location);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* free the strings we just allocated */
		pfree(DatumGetPointer(values[ 2]));
		pfree(DatumGetPointer(values[ 3]));
	}

	tuplestore_donestoring(tupstore);

#undef REPLICATION_IDENTIFIER_PROGRESS_COLS

	return (Datum) 0;
}

Datum
pg_replication_identifier_advance(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	text	   *remote_lsn = PG_GETARG_TEXT_P(1);
	text	   *local_lsn = PG_GETARG_TEXT_P(2);
	char	   *remote_lsn_str;
	char	   *local_lsn_str;
	XLogRecPtr remote_commit;
	XLogRecPtr local_commit;
	RepNodeId  node;
	uint32		rhi,
				lhi,
				rlo,
				llo;

	CheckReplicationIdentifierPrerequisites(true);

	node = GetReplicationIdentifier(text_to_cstring(name), false);

	remote_lsn_str = text_to_cstring(remote_lsn);
	local_lsn_str = text_to_cstring(local_lsn);

	if (sscanf(remote_lsn_str, "%X/%X", &rhi, &rlo) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse remote_lsn \"%s\"",
						remote_lsn_str)));

	if (sscanf(local_lsn_str, "%X/%X", &lhi, &llo) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse local_lsn \"%s\"",
						local_lsn_str)));

	remote_commit = ((uint64) (rhi)) << 32 | (uint64) rlo;
	local_commit = ((uint64) (lhi)) << 32 | (uint64) llo;

	AdvanceReplicationIdentifier(node, remote_commit, local_commit);

	pfree(remote_lsn_str);
	pfree(local_lsn_str);

	PG_RETURN_VOID();
}

Datum
pg_replication_identifier_drop(PG_FUNCTION_ARGS)
{
	char *name;
	RepNodeId riident;

	CheckReplicationIdentifierPrerequisites(false);

	name = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(0)));

	riident = GetReplicationIdentifier(name, false);
	if (OidIsValid(riident))
		DropReplicationIdentifier(riident);

	pfree(name);

	PG_RETURN_VOID();
}

Size
ReplicationIdentifierShmemSize(void)
{
	Size		size = 0;

	/*
	 * FIXME: max_replication_slots is the wrong thing to use here, here we keep
	 * the replay state of *remote* transactions.
	 */
	if (max_replication_slots == 0)
		return size;

	size = add_size(size,
					mul_size(max_replication_slots, sizeof(ReplicationState)));
	return size;
}

void
ReplicationIdentifierShmemInit(void)
{
	bool		found;

	if (max_replication_slots == 0)
		return;

	ReplicationStates = (ReplicationState *)
		ShmemInitStruct("ReplicationIdentifierState",
						ReplicationIdentifierShmemSize(),
						&found);

	if (!found)
	{
		MemSet(ReplicationStates, 0, ReplicationIdentifierShmemSize());
	}
}

/* ---------------------------------------------------------------------------
 * Perform a checkpoint of replication identifier's progress with respect to
 * the replayed remote_lsn. Make sure that all transactions we refer to in the
 * checkpoint (local_lsn) are actually on-disk. This might not yet be the case
 * if the transactions were originally committed asynchronously.
 *
 * We store checkpoints in the following format:
 * +-------+-------------------------+-------------------------+-----+
 * | MAGIC | struct ReplicationState | struct ReplicationState | ... | EOF
 * +-------+-------------------------+-------------------------+-----+
 *
 * So its just the magic, followed by the statically sized
 * ReplicationStates. Note that the maximum number of ReplicationStates is
 * determined by max_replication_slots.
 *
 * FIXME: Add a CRC32 to the end.
 * ---------------------------------------------------------------------------
 */
void
CheckPointReplicationIdentifier(XLogRecPtr ckpt)
{
	char tmppath[MAXPGPATH];
	char path[MAXPGPATH];
	int fd;
	int tmpfd;
	int i;
	uint32 magic = REPLICATION_STATE_MAGIC;

	if (max_replication_slots == 0)
		return;

	/*
	 * Write to a filename a LSN of the checkpoint's REDO pointer, so we can
	 * deal with the checkpoint failing after
	 * CheckPointReplicationIdentifier() finishing.
	 */
	sprintf(path, "pg_logical/checkpoints/%X-%X.ckpt",
			(uint32)(ckpt >> 32), (uint32)ckpt);
	sprintf(tmppath, "pg_logical/checkpoints/%X-%X.ckpt.tmp",
			(uint32)(ckpt >> 32), (uint32)ckpt);

	/* check whether file already exists */
	fd = OpenTransientFile(path,
						   O_RDONLY | PG_BINARY,
						   0);

	/* usual case, no checkpoint performed yet */
	if (fd < 0 && errno == ENOENT)
		;
	else if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not check replication state checkpoint \"%s\": %m",
						path)));
	/* already checkpointed before crash during a checkpoint or so */
	else
	{
		CloseTransientFile(fd);
		return;
	}

	/* make sure no old temp file is remaining */
	if (unlink(tmppath) < 0 && errno != ENOENT)
		ereport(PANIC, (errmsg("failed while unlinking %s", path)));

	/*
	 * no other backend can perform this at the same time, we're protected by
	 * CheckpointLock.
	 */
	tmpfd = OpenTransientFile(tmppath,
							  O_CREAT | O_EXCL | O_WRONLY | PG_BINARY,
							  S_IRUSR | S_IWUSR);
	if (tmpfd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create replication identifier checkpoint \"%s\": %m",
						tmppath)));

	/* write magic */
	if ((write(tmpfd, &magic, sizeof(magic))) !=
		sizeof(magic))
	{
		CloseTransientFile(tmpfd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write replication identifier checkpoint \"%s\": %m",
						tmppath)));
	}

	/* write actual data */
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationState local_state;

		if (ReplicationStates[i].local_identifier == InvalidRepNodeId)
			continue;

		local_state.local_identifier = ReplicationStates[i].local_identifier;
		local_state.remote_lsn = ReplicationStates[i].remote_lsn;
		local_state.local_lsn = InvalidXLogRecPtr;

		/* make sure we only write out a commit that's persistent */
		XLogFlush(ReplicationStates[i].local_lsn);

		if ((write(tmpfd, &local_state, sizeof(ReplicationState))) !=
			sizeof(ReplicationState))
		{
			CloseTransientFile(tmpfd);
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write replication identifier checkpoint \"%s\": %m",
							tmppath)));
		}
	}

	/* fsync the file */
	if (pg_fsync(tmpfd) != 0)
	{
		CloseTransientFile(tmpfd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync replication identifier checkpoint \"%s\": %m",
						tmppath)));
	}

	CloseTransientFile(tmpfd);

	/* rename to permanent file, fsync file and directory */
	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename replication identifier checkpoint from \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	fsync_fname("pg_logical/checkpoints", true);
	fsync_fname(path, false);
}

/*
 * Remove old replication identifier checkpoints that cannot possibly be
 * needed anymore for crash recovery.
 */
void
TruncateReplicationIdentifier(XLogRecPtr cutoff)
{
	DIR		   *snap_dir;
	struct dirent *snap_de;
	char		path[MAXPGPATH];

	snap_dir = AllocateDir("pg_logical/checkpoints");
	while ((snap_de = ReadDir(snap_dir, "pg_logical/checkpoints")) != NULL)
	{
		uint32		hi;
		uint32		lo;
		XLogRecPtr	lsn;
		struct stat statbuf;

		if (strcmp(snap_de->d_name, ".") == 0 ||
			strcmp(snap_de->d_name, "..") == 0)
			continue;

		snprintf(path, MAXPGPATH, "pg_logical/checkpoints/%s", snap_de->d_name);

		if (lstat(path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
		{
			elog(DEBUG1, "only regular files expected: %s", path);
			continue;
		}

		if (sscanf(snap_de->d_name, "%X-%X.ckpt", &hi, &lo) != 2)
		{
			ereport(LOG,
					(errmsg("could not parse filename \"%s\"", path)));
			continue;
		}

		lsn = ((uint64) hi) << 32 | lo;

		/* check whether we still need it */
		if (lsn < cutoff)
		{
			elog(DEBUG2, "removing replication identifier checkpoint %s", path);

			/*
			 * It's not particularly harmful, though strange, if we can't
			 * remove the file here. Don't prevent the checkpoint from
			 * completing, that'd be cure worse than the disease.
			 */
			if (unlink(path) < 0)
			{
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("could not unlink file \"%s\": %m",
								path)));
				continue;
			}
		}
		else
		{
			elog(DEBUG2, "keeping replication identifier checkpoint %s", path);
		}
	}
	FreeDir(snap_dir);
}

/*
 * Recover replication replay status from checkpoint data saved earlier by
 * CheckPointReplicationIdentifier.
 *
 * This only needs to be called at startup and *not* during every checkpoint
 * read during recovery (e.g. in HS or PITR from a base backup) afterwards. All
 * state thereafter can be recovered by looking at commit records.
 */
void
StartupReplicationIdentifier(XLogRecPtr ckpt)
{
	char path[MAXPGPATH];
	int fd;
	int readBytes;
	uint32 magic = REPLICATION_STATE_MAGIC;
	int last_state = 0;

	/* don't want to overwrite already existing state */
#ifdef USE_ASSERT_CHECKING
	static bool already_started = false;
	Assert(!already_started);
	already_started = true;
#endif

	if (max_replication_slots == 0)
		return;

	elog(LOG, "starting up replication identifier with ckpt at %X/%X",
		 (uint32)(ckpt >> 32), (uint32)ckpt);

	sprintf(path, "pg_logical/checkpoints/%X-%X.ckpt",
			(uint32)(ckpt >> 32), (uint32)ckpt);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);

	/*
	 * might have had max_replication_slots == 0 last run, or we just brought up a
	 * standby.
	 */
	if (fd < 0 && errno == ENOENT)
		return;
	else if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open replication state checkpoint \"%s\": %m",
						path)));

	/* verify magic, thats written even if nothing was active */
	readBytes = read(fd, &magic, sizeof(magic));
	if (readBytes != sizeof(magic))
		ereport(PANIC,
				(errmsg("could not read replication state checkpoint magic \"%s\": %m",
						path)));

	if (magic != REPLICATION_STATE_MAGIC)
		ereport(PANIC,
				(errmsg("replication checkpoint has wrong magic %u instead of %u",
						magic, REPLICATION_STATE_MAGIC)));

	/* recover individual states, until there are no more to be found */
	while (true)
	{
		ReplicationState local_state;
		readBytes = read(fd, &local_state, sizeof(local_state));

		/* no further data */
		if (readBytes == 0)
			break;

		if (readBytes < 0)
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read replication checkpoint file \"%s\": %m",
							path)));
		}

		if (readBytes != sizeof(local_state))
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read replication checkpoint file \"%s\": read %d of %zu",
							path, readBytes, sizeof(local_state))));
		}

		if (last_state == max_replication_slots)
			ereport(PANIC,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("no free replication state could be found, increase max_replication_slots")));

		/* copy data shared memory */
		ReplicationStates[last_state++] = local_state;

		elog(LOG, "recovered replication state of node %u to %X/%X",
			 local_state.local_identifier,
			 (uint32)(local_state.remote_lsn >> 32),
			 (uint32)local_state.remote_lsn);
	}

	CloseTransientFile(fd);
}

/*
 * Tell the replication identifier machinery that a commit from 'node' that
 * originated at the LSN remote_commit on the remote node was replayed
 * successfully and that we don't need to do so again. In combination with
 * setting up replication_origin_lsn and replication_origin_id that ensures we
 * won't loose knowledge about that after a crash if the the transaction had a
 * persistent effect (think of asynchronous commits).
 *
 * local_commit needs to be a local LSN of the commit so that we can make sure
 * uppon a checkpoint that enough WAL has been persisted to disk.
 */
void
AdvanceReplicationIdentifier(RepNodeId node,
							 XLogRecPtr remote_commit,
							 XLogRecPtr local_commit)
{
	int i;
	int free_slot = -1;
	ReplicationState *replication_state = NULL;

	/*
	 * XXX: should we restore into a hashtable and dump into shmem only after
	 * recovery finished?
	 */

	/* check whether slot already exists */
	for (i = 0; i < max_replication_slots; i++)
	{
		/* remember where to insert if necessary */
		if (ReplicationStates[i].local_identifier == InvalidRepNodeId &&
			free_slot == -1)
		{
			free_slot = i;
			continue;
		}

		/* not our slot */
		if (ReplicationStates[i].local_identifier != node)
			continue;

		/* ok, found slot */
		replication_state = &ReplicationStates[i];
		break;
	}

	if (replication_state == NULL && free_slot == -1)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no free replication state could be found for %u, increase max_replication_slots",
						node)));
	/* initialize new slot */
	else if (replication_state == NULL)
	{
		replication_state = &ReplicationStates[free_slot];
		Assert(replication_state->remote_lsn == InvalidXLogRecPtr);
		Assert(replication_state->local_lsn == InvalidXLogRecPtr);
		replication_state->local_identifier = node;
	}

	Assert(replication_state->local_identifier != InvalidRepNodeId);

	/*
	 * Due to - harmless - race conditions during a checkpoint we could see
	 * values here that are older than the ones we already have in
	 * memory. Don't overwrite those.
	 */
	if (replication_state->remote_lsn < remote_commit)
		replication_state->remote_lsn = remote_commit;
	if (replication_state->local_lsn < local_commit)
		replication_state->local_lsn = local_commit;
}


/*
 * Setup a replication identifier in the shared memory struct if it doesn't
 * already exists and cache access to the specific ReplicationSlot so the
 * array doesn't have to be searched when calling
 * AdvanceCachedReplicationIdentifier().
 *
 * Obviously only one such cached identifier can exist per process and the
 * current cached value can only be set again after the previous value is torn
 * down with TeardownCachedReplicationIdentifier().
 */
void
SetupCachedReplicationIdentifier(RepNodeId node)
{
	int i;
	int free_slot = -1;


	Assert(max_replication_slots > 0);

	if (local_replication_state != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot setup replication origin when one is already setup")));

	/*
	 * Search for either an existing slot for that identifier or a free one we
	 * can use.
	 */
	for (i = 0; i < max_replication_slots; i++)
	{
		/* remember where to insert if necessary */
		if (ReplicationStates[i].local_identifier == InvalidRepNodeId &&
			free_slot == -1)
		{
			free_slot = i;
			continue;
		}

		/* not our slot */
		if (ReplicationStates[i].local_identifier != node)
			continue;

		local_replication_state = &ReplicationStates[i];
	}


	if (local_replication_state == NULL && free_slot == -1)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no free replication state could be found for %u, increase max_replication_slots",
						node)));
	else if (local_replication_state == NULL)
	{
		local_replication_state = &ReplicationStates[free_slot];
		local_replication_state->local_identifier = node;
		Assert(local_replication_state->remote_lsn == InvalidXLogRecPtr);
		Assert(local_replication_state->local_lsn == InvalidXLogRecPtr);
	}

	Assert(local_replication_state->local_identifier != InvalidRepNodeId);
}

/*
 * Make currently cached replication identifier unavailable so a new one can
 * be setup with SetupCachedReplicationIdentifier().
 *
 * This function may only be called if a previous identifier was setup with
 * SetupCachedReplicationIdentifier().
 */
void
TeardownCachedReplicationIdentifier(void)
{
	Assert(max_replication_slots != 0);

	if (local_replication_state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot setup replication origin when one is already setup")));

	local_replication_state = NULL;
}

/*
 * Do the same work AdvanceReplicationIdentifier() does, just on a pre-cached
 * identifier. This is noticeably cheaper if you only ever work on a single
 * replication identifier.
 */
void
AdvanceCachedReplicationIdentifier(XLogRecPtr remote_commit,
								   XLogRecPtr local_commit)
{
	Assert(local_replication_state != NULL);
	if (local_replication_state->local_lsn < local_commit)
		local_replication_state->local_lsn = local_commit;
	if (local_replication_state->remote_lsn < remote_commit)
		local_replication_state->remote_lsn = remote_commit;
}

/*
 * Ask the machinery about the point up to which we successfully replayed
 * changes from a already setup & cached replication identifier.
 */
XLogRecPtr
RemoteCommitFromCachedReplicationIdentifier(void)
{
	Assert(local_replication_state != NULL);
	return local_replication_state->remote_lsn;
}
