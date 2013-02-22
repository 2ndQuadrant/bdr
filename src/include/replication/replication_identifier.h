/*-------------------------------------------------------------------------
 * replication_identifier.h
 *     XXX
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPLICATION_IDENTIFIER_H
#define REPLICATION_IDENTIFIER_H

#include "catalog/pg_replication_identifier.h"
#include "replication/logical.h"

extern RepNodeId GetReplicationIdentifier(uint64 remotesysid, Oid remotetli,
										  Oid remotedb, Name riname,
										  Oid rilocaldb);
extern RepNodeId CreateReplicationIdentifier(uint64 remotesysid, Oid remotetli,
											 Oid remotedb, Name riname,
											 Oid rilocaldb);

extern HeapTuple GetReplicationInfoByIdentifier(RepNodeId riident);

extern Size ReplicationIdentifierShmemSize(void);
extern void ReplicationIdentifierShmemInit(void);

extern void CheckPointReplicationIdentifier(XLogRecPtr ckpt);
extern void StartupReplicationIdentifier(XLogRecPtr ckpt);

extern void SetupCachedReplicationIdentifier(RepNodeId node);
extern void TeardownCachedReplicationIdentifier(RepNodeId node);

extern void AdvanceCachedReplicationIdentifier(XLogRecPtr remote_commit,
											   XLogRecPtr local_commit);

extern void AdvanceReplicationIdentifier(RepNodeId node,
										 XLogRecPtr remote_commit,
										 XLogRecPtr local_commit);

extern XLogRecPtr RemoteCommitFromCachedReplicationIdentifier(void);
#endif
