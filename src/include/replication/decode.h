/*-------------------------------------------------------------------------
 * decode.h
 *     PostgreSQL WAL to logical transformation
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef DECODE_H
#define DECODE_H

#include "access/xlogreader.h"
#include "replication/reorderbuffer.h"
#include "replication/logical.h"

void DecodeRecordIntoReorderBuffer(LogicalDecodingContext *ctx,
								   XLogRecordBuffer* buf);

#endif
