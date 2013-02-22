/*-------------------------------------------------------------------------
 *
 * replication_identifier.c
 *	  Logical Replication Identifier and progress persistency support
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/replication_identifier.c
 *
 */

#include "postgres.h"

#include <unistd.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "replication/replication_identifier.h"
#include "replication/logical.h"
#include "storage/fd.h"
#include "storage/copydir.h"
#include "utils/syscache.h"
#include "utils/rel.h"

typedef struct ReplicationState
{
	RepNodeId	local_identifier;

	/*
	 * Latest commit from the remote side.
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
 * Base address into array of replication states of size max_logical_slots.
 */
static ReplicationState *ReplicationStates;

/*
 * Local ReplicationState so we don't have to search ReplicationStates for the
 * backends current RepNodeId.
 */
static ReplicationState *local_replication_state = NULL;

/*RSTATE */
#define REPLICATION_STATE_MAGIC (uint32)0x1257DADE

/*
 * Check for a persistent repication identifier identified by remotesysid,
 * remotetli, remotedb, riname, rilocaldb.
 *
 * Returns InvalidOid.
 */
RepNodeId
GetReplicationIdentifier(uint64 remotesysid, Oid remotetli, Oid remotedb,
                         Name riname, Oid rilocaldb)
{
	Oid riident = InvalidOid;
	HeapTuple tuple;
	Form_pg_replication_identifier ident;
	NameData sysid;

	sprintf(NameStr(sysid), UINT64_FORMAT "-%u", remotesysid, remotetli);

	tuple = SearchSysCache4(REPLIDREMOTE,
	                        NameGetDatum(&sysid),
	                        ObjectIdGetDatum(remotedb),
	                        ObjectIdGetDatum(rilocaldb),
	                        NameGetDatum(riname));
	if (HeapTupleIsValid(tuple))
	{
		ident = (Form_pg_replication_identifier)GETSTRUCT(tuple);
		riident = ident->riident;
		ReleaseSysCache(tuple);
	}
	return riident;
}

/*
 * Create a persistent replication identifier.
 *
 * Needs to be called in a transaction and doesn't call
 * CommandCounterIncrement().
 */
RepNodeId
CreateReplicationIdentifier(uint64 remotesysid, Oid remotetli, Oid remotedb,
                            Name riname, Oid rilocaldb)
{
	Oid riident;
	HeapTuple tuple = NULL;
	NameData sysid;
	Relation rel;

	Assert(IsTransactionState());

	sprintf(NameStr(sysid), UINT64_FORMAT "-%u", remotesysid, remotetli);

	/* lock table against modifications */
	rel = heap_open(ReplicationIdentifierRelationId, ExclusiveLock);

	/* XXX: should we start at FirstNormalObjectId ? */
	for (riident = InvalidOid + 1; riident <= UINT16_MAX; riident++)
	{
		bool		nulls[Natts_pg_replication_identifier];
		Datum		values[Natts_pg_replication_identifier];

		tuple = GetReplicationInfoByIdentifier(riident);

		if (tuple != NULL)
		{
			ReleaseSysCache(tuple);
			continue;
		}
		/* ok, found an unused riident */

		memset(&nulls, 0, sizeof(nulls));

		values[Anum_pg_replication_riident -1] = ObjectIdGetDatum(riident);
		values[Anum_pg_replication_riremotesysid - 1] = NameGetDatum(&sysid);
		values[Anum_pg_replication_rilocaldb - 1] = ObjectIdGetDatum(rilocaldb);
		values[Anum_pg_replication_riremotedb - 1] = ObjectIdGetDatum(remotedb);
		values[Anum_pg_replication_riname - 1] = NameGetDatum(riname);

		tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
		simple_heap_insert(rel, tuple);
		CatalogUpdateIndexes(rel, tuple);
		CommandCounterIncrement();
		break;
	}

	/*
	 * only release at end of transaction, so we don't have to worry about race
	 * conditions with other transactions trying to insert a new
	 * identifier. Acquiring a new identifier should be a fairly infrequent
	 * thing, so this seems fine.
	 */
	heap_close(rel, NoLock);

	if (tuple == NULL)
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		         errmsg("no free replication id could be found")));

	return riident;
}

/*
 * Lookup pg_replication_identifier tuple via its riident.
 *
 * The result needs to be ReleaseSysCache'ed
 */
HeapTuple
GetReplicationInfoByIdentifier(RepNodeId riident)
{
	HeapTuple tuple;

	Assert(OidIsValid((Oid) riident));
	Assert(riident < UINT16_MAX);
	tuple = SearchSysCache1(REPLIDIDENT,
	                        ObjectIdGetDatum((Oid) riident));
	return tuple;
}

Size
ReplicationIdentifierShmemSize(void)
{
	Size		size = 0;

	/*
	 * FIXME: max_logical_slots is the wrong thing to use here, here we keep
	 * the replay state of *remote* transactions.
	 */
	if (max_logical_slots == 0)
		return size;

	size = add_size(size,
					mul_size(max_logical_slots, sizeof(ReplicationState)));
	return size;
}

void
ReplicationIdentifierShmemInit(void)
{
	bool		found;

	if (max_logical_slots == 0)
		return;

	ReplicationStates = (ReplicationState *)
		ShmemInitStruct("ReplicationIdentifierState",
						ReplicationIdentifierShmemSize(),
						&found);

	if (!found)
	{
		int i;
		for (i = 0; i < max_logical_slots; i++)
		{
			MemSet(ReplicationStates, 0, ReplicationIdentifierShmemSize());
		}
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
 * determined by max_logical_slots.
 *
 * XXX: This doesn't yet work on a standby in contrast to a master doing crash
 * recovery, since the checkpoint data file won't be available there. Thats
 * fine for now since a standby can't perform replay on its own.
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

	if (max_logical_slots == 0)
		return;

	elog(LOG, "doing replication identifier checkpoint");

	sprintf(path, "pg_llog/checkpoints/%X-%X.ckpt",
			(uint32)(ckpt >> 32), (uint32)ckpt);
	sprintf(tmppath, "pg_llog/checkpoints/%X-%X.ckpt.tmp",
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
		ereport(PANIC, (errmsg("failed while unlinking %s",  path)));

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
	for (i = 0; i < max_logical_slots; i++)
	{
		ReplicationState local_state;

		if (ReplicationStates[i].local_identifier == InvalidRepNodeId)
			continue;

		local_state.local_identifier = ReplicationStates[i].local_identifier;
		local_state.remote_lsn = ReplicationStates[i].remote_lsn;
		local_state.local_lsn = InvalidXLogRecPtr;

		/* make sure we only write out a commit thats persistent */
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

	fsync_fname("pg_llog/checkpoints", true);
	fsync_fname(path, false);
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

	if (max_logical_slots == 0)
		return;

	elog(LOG, "starting up replication identifier with ckpt at %X/%X",
		 (uint32)(ckpt >> 32), (uint32)ckpt);

	sprintf(path, "pg_llog/checkpoints/%X-%X.ckpt",
			(uint32)(ckpt >> 32), (uint32)ckpt);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);

	/*
	 * might have had max_logical_slots == 0 last run, or we just brought up a
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
		ereport(PANIC, (errmsg("could not read replication state checkpoint magic \"%s\": %m",
							   path)));

	if (magic != REPLICATION_STATE_MAGIC)
		ereport(PANIC, (errmsg("replication checkpoint has wrong magic %u instead of %u",
							   magic, REPLICATION_STATE_MAGIC)));

	/* recover individual states, until there are no more to be found */
	while (true)
	{
		ReplicationState local_state;
		readBytes = read(fd, &local_state, sizeof(local_state));

		/* no further data */
		if (readBytes == 0)
			break;

		if (readBytes != sizeof(local_state))
		{
			int saved_errno = errno;
			CloseTransientFile(fd);
			errno = saved_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read replication checkpoint file \"%s\": %m, read %d of %zu",
							path, readBytes, sizeof(local_state))));
		}

		if (last_state == max_logical_slots)
			ereport(PANIC,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("no free replication state could be found, increase max_logical_slots")));

		/* copy data shared memory */
		ReplicationStates[last_state++] = local_state;

		elog(LOG, "recovered replication state of %u to %X/%X",
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
 * setting up replication_origin_lsn and guc_replication_origin_id that ensures
 * we won't loose knowledge about that after a crash if the the transaction had
 * a persistent effect (think of asynchronous commits).
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
	for (i = 0; i < max_logical_slots; i++)
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
		ereport(PANIC,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no free replication state could be found for %u, increase max_logical_slots",
						node)));
	/* initialize new slot */
	else if (replication_state == NULL)
	{
		replication_state = &ReplicationStates[free_slot];
		Assert(replication_state->remote_lsn == InvalidXLogRecPtr);
		Assert(replication_state->local_lsn == InvalidXLogRecPtr);
		replication_state->local_identifier = node;
	}

	/*
	 * due to - harmless - race conditions during a checkpoint we could see
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
 * already exists and cache access to the specific ReplicationSlot so the array
 * doesn't have to be searched when calling
 * AdvanceCachedReplicationIdentifier().
 *
 * Obviously only such cached identifier can exist per process and the current
 * cached value can only be set again after the prvious value is torn down with
 * TeardownCachedReplicationIdentifier.
 */
void
SetupCachedReplicationIdentifier(RepNodeId node)
{
	int i;
	int free_slot = -1;

	Assert(max_logical_slots != 0);
	Assert(local_replication_state == NULL);

	/*
	 * Aearch for either an existing slot for that identifier or a free one we
	 * can use.
	 */
	for (i = 0; i < max_logical_slots; i++)
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
		ereport(PANIC,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no free replication state could be found for %u, increase max_logical_slots",
						node)));
	else if (local_replication_state == NULL)
	{
		local_replication_state = &ReplicationStates[free_slot];
		local_replication_state->local_identifier = node;
		Assert(local_replication_state->remote_lsn == InvalidXLogRecPtr);
		Assert(local_replication_state->local_lsn == InvalidXLogRecPtr);
	}
}

/*
 * Make currently cached replication identifier unavailable so a new one can be
 * setup with SetupCachedReplicationIdentifier().
 *
 * This function may only be called if a previous identifier was cached.
 */
void
TeardownCachedReplicationIdentifier(RepNodeId node)
{
	Assert(max_logical_slots != 0);
	Assert(local_replication_state != NULL);

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
 * changes from a already setup & chaced replication identifier.
 */
XLogRecPtr RemoteCommitFromCachedReplicationIdentifier(void)
{
	Assert(local_replication_state != NULL);
	return local_replication_state->remote_lsn;
}
