#ifndef BDR_REPLICATION_ORIGIN_COMPAT_H
#define BDR_REPLICATION_ORIGIN_COMPAT_H

/*
 * Compatibility adapter for 9.4bdr's replication/replication_identifier.h to 
 * 9.5+'s replication/origin.h
 *
 * "replication identifier" was changed to "replication origin" in the 9.5
 * commit of replication origins.
 *
 * Internals and SQL-callables aren't exposed here.
 */

#include "replication/replication_identifier.h"

#define	InvalidRepOriginId					InvalidRepNodeId
#define DoNotReplicateId					DoNotReplicateRepNodeId

#define replorigin_session_origin			replication_origin_id
#define replorigin_session_origin_lsn		replication_origin_lsn
#define replorigin_session_origin_timestamp	replication_origin_timestamp

/* API for querying & manipulating replication identifiers */
static inline RepNodeId replorigin_by_name(char *name, bool missing_ok)
{
	return GetReplicationIdentifier(name, missing_ok);
}

static inline RepNodeId replorigin_create(char *name)
{
	return CreateReplicationIdentifier(name);
}

static inline void replorigin_by_oid(RepNodeId riident, bool missing_ok,
										   char **riname)
{
	GetReplicationInfoByIdentifier(riident, missing_ok, riname);
}

static inline void replorigin_drop(RepNodeId riident)
{
	DropReplicationIdentifier(riident);
}

static inline void replorigin_advance(RepNodeId node,
								 XLogRecPtr remote_commit,
								 XLogRecPtr local_commit,
								 bool go_backward, bool wal_log)
{
	Assert(!go_backward); /* not supported on 9.4bdr XXX CONFIRM */
	Assert(wal_log); /* not optional on 9.4bdr XXX CONFIRM */
	AdvanceReplicationIdentifier(node, remote_commit, local_commit);
}

static inline void replorigin_session_advance(XLogRecPtr remote_commit,
											   XLogRecPtr local_commit)
{
	AdvanceCachedReplicationIdentifier(remote_commit, local_commit);
}

static inline void replorigin_session_setup(RepNodeId node)
{
	SetupCachedReplicationIdentifier(node);
}

static inline void replorigin_session_reset(void)
{
	TeardownCachedReplicationIdentifier();
}

static inline XLogRecPtr replorigin_session_get_progress(bool flush)
{
	Assert(!flush); /* not supported on 9.4bdr */
	return RemoteCommitFromCachedReplicationIdentifier();
}

#endif
