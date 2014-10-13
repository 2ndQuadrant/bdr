/*
 * bdr_replication_identifier.h
 *
 * BiDirectionalReplication
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * contrib/bdr/bdr_replication_identifier.h
 */
#ifndef BDR_REPLICATION_IDENTIFIER_H
#define BDR_REPLICATION_IDENTIFIER_H

#ifdef BUILDING_BDR

#include "replication/replication_identifier.h"

#else

#include "access/htup.h"
#include "access/xlogdefs.h"
#include "datatype/timestamp.h"

typedef uint16 RepNodeId;
#define InvalidRepNodeId 0

extern PGDLLIMPORT RepNodeId replication_origin_id;
extern PGDLLIMPORT TimestampTz replication_origin_timestamp;
extern PGDLLIMPORT XLogRecPtr replication_origin_lsn;

extern RepNodeId CreateReplicationIdentifier(char *name);
extern RepNodeId GetReplicationIdentifier(char *name, bool missing_ok);
extern void DropReplicationIdentifier(RepNodeId riident);
extern void AdvanceReplicationIdentifier(RepNodeId node,
							 XLogRecPtr remote_commit,
							 XLogRecPtr local_commit);
extern void SetupCachedReplicationIdentifier(RepNodeId node);
extern void AdvanceCachedReplicationIdentifier(XLogRecPtr remote_commit,
											   XLogRecPtr local_commit);

extern XLogRecPtr RemoteCommitFromCachedReplicationIdentifier(void);

extern HeapTuple GetReplicationInfoByIdentifier(RepNodeId riident, bool missing_ok);

/*
 * XXX: ugly
 * bdr_replication_identifier struct
 */
typedef struct {
	Oid		riident;
	text	riname;
	XLogRecPtr  riremote_lsn;
	XLogRecPtr  rilocal_lsn;
} FormData_pg_replication_identifier;
typedef FormData_pg_replication_identifier *Form_pg_replication_identifier;

#endif

#endif   /* BDR_REPLICATION_IDENTIFIER_H */
