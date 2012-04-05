/*-------------------------------------------------------------------------
 * output_plugin.h
 *     PostgreSQL Logical Decode Plugin Interface
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef OUTPUT_PLUGIN_H
#define OUTPUT_PLUGIN_H

#include "replication/reorderbuffer.h"

struct LogicalDecodingContext;

/*
 * Callback that gets called in a user-defined plugin.
 * 'private_data' can be set to some private data.
 *
 * Gets looked up via the library symbol pg_decode_init.
 */
typedef void (*LogicalDecodeInitCB) (
	struct LogicalDecodingContext*,
	bool is_init
	);

/*
 * Gets called for every BEGIN of a successful transaction.
 *
 * Return "true" if the message in "out" should get sent, false otherwise.
 *
 * Gets looked up via the library symbol pg_decode_begin_txn.
 */
typedef bool (*LogicalDecodeBeginCB) (
	struct LogicalDecodingContext*,
	ReorderBufferTXN *txn);

/*
 * Gets called for every change in a successful transaction.
 *
 * Return "true" if the message in "out" should get sent, false otherwise.
 *
 * Gets looked up via the library symbol pg_decode_change.
 */
typedef bool (*LogicalDecodeChangeCB) (
	struct LogicalDecodingContext*,
	ReorderBufferTXN *txn,
	Relation relation,
	ReorderBufferChange *change
	);

/*
 * Gets called for every COMMIT of a successful transaction.
 *
 * Return "true" if the message in "out" should get sent, false otherwise.
 *
 * Gets looked up via the library symbol pg_decode_commit_txn.
 */
typedef bool (*LogicalDecodeCommitCB) (
	struct LogicalDecodingContext*,
	ReorderBufferTXN *txn,
	XLogRecPtr commit_lsn);

/*
 * Gets called to cleanup the state of an output plugin
 *
 * Gets looked up via the library symbol pg_decode_cleanup.
 */
typedef void (*LogicalDecodeCleanupCB) (
	struct LogicalDecodingContext*
	);

#endif
