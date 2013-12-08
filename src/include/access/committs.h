/*
 * committs.h
 *
 * PostgreSQL commit timestamp manager
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/committs.h
 */
#ifndef COMMITTS_H
#define COMMITTS_H

#include "access/xlog.h"
#include "datatype/timestamp.h"


extern PGDLLIMPORT bool	commit_ts_enabled;

typedef uint32 CommitExtraData;

extern void TransactionTreeSetCommitTimestamp(TransactionId xid, int nsubxids,
								  TransactionId *subxids,
								  TimestampTz timestamp,
								  CommitExtraData data,
								  bool do_xlog);
extern void TransactionIdGetCommitTsData(TransactionId xid, TimestampTz *ts,
							 CommitExtraData *data);
extern TransactionId GetLatestCommitTimestampData(TimestampTz *ts,
							 CommitExtraData *extra);

extern Size CommitTsShmemBuffers(void);
extern Size CommitTsShmemSize(void);
extern void CommitTsShmemInit(void);
extern void BootStrapCommitTs(void);
extern void StartupCommitTs(void);
extern void ShutdownCommitTs(void);
extern void CheckPointCommitTs(void);
extern void ExtendCommitTs(TransactionId newestXact);
extern void TruncateCommitTs(TransactionId oldestXact);
extern void SetCommitTsLimit(TransactionId oldestXact);

/* XLOG stuff */
#define COMMITTS_ZEROPAGE		0x00
#define COMMITTS_TRUNCATE		0x10
#define COMMITTS_SETTS			0x20

typedef struct xl_committs_set
{
	TimestampTz		timestamp;
	CommitExtraData	data;
	TransactionId	mainxid;
	int				nsubxids;
	TransactionId	subxids[FLEXIBLE_ARRAY_MEMBER];
} xl_committs_set;


extern void committs_redo(XLogRecPtr lsn, XLogRecord *record);
extern void committs_desc(StringInfo buf, uint8 xl_info, char *rec);

#endif   /* COMMITTS_H */
