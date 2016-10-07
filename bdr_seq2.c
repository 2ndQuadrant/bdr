/* -------------------------------------------------------------------------
 *
 * bdr_seq2.c
 *		Replication!!!
 *
 * Replication???
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_seq2.c
 *
 * An implementation of global sequences that does not rely on the sequence
 * access method interface from postgres-9.4bdr.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/sequence.h"
#include "fmgr.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"

#include "bdr.h"

#define TIMESTAMP_BITS	40
#define NODEID_BITS		10
#define SEQUENCE_BITS	14
#define MAX_NODE_ID		((1 << NODEID_BITS) - 1)
#define MAX_SEQ_ID		((1 << SEQUENCE_BITS) - 1)
#define MAX_TIMESTAMP	(((int64)1 << TIMESTAMP_BITS) - 1)

/* Cache for nodeid so we don't have to read it for every nextval call. */
static int16	seq_nodeid = -1;

static int16 global_seq_get_nodeid(void);

Datum global_seq_nextval_oid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(global_seq_nextval_oid);

/*
 * We generate sequence number from postgres epoch in ms (40 bits),
 * node id (10 bits) and sequence (14 bits).
 *
 * This can handle milliseconds up to year 2042 (since we count the year from
 * 2016), 1024 nodes and 8192 sequences per millisecond (8M per second). Anyone
 * expecting more than that should consider using UUIDs.
 *
 * We wrap the input sequence. If more than 2048 values are generated in a
 * millisecond we could wrap.
 *
 * New variants of this sequence generator may be added by adding new
 * SQL callable functions with different epoch offset and bit ranges,
 * if users have different needs.
 */
Datum
global_seq_nextval_oid(PG_FUNCTION_ARGS)
{
	Oid		seqoid = PG_GETARG_OID(0);
	Datum	sequenced;
	int64	sequence;
	int64	nodeid;
	int64	timestamp;
	int64	res;
	const int64 seq_ts_epoch = 529111339634; /* Oct 7, 2016, when this code was written, in ms */

	/* timestamp is in milliseconds */
	timestamp = (GetCurrentIntegerTimestamp()/1000) - seq_ts_epoch;
	nodeid = global_seq_get_nodeid();
	sequenced = DirectFunctionCall1(nextval_oid, seqoid);
	sequence = DatumGetInt64(sequenced) % MAX_SEQ_ID;

	elog(NOTICE, "timestamp is "UINT64_FORMAT, timestamp);

	/*
	 * This is mainly a failsafe so that we don't generate corrupted
	 * sequence numbers if machine date is incorrect (or if somebody
	 * is still using this code after ~2042).
	 */
	if (timestamp < 0 || timestamp > MAX_TIMESTAMP)
		elog(ERROR, "cannot generate sequence, timestamp "UINT64_FORMAT" out of range 0 .. "UINT64_FORMAT,
			timestamp, MAX_TIMESTAMP);

	if (nodeid < 0 || nodeid > MAX_NODE_ID)
		elog(ERROR, "nodeid must be in range 0 .. %d", MAX_NODE_ID);

	Assert(sequence >= 0 && sequence < MAX_SEQ_ID);
	Assert((MAX_SEQ_ID + 1) % 2 == 0);
	Assert(TIMESTAMP_BITS + NODEID_BITS + SEQUENCE_BITS == 64);

	res = (timestamp << (64 - TIMESTAMP_BITS)) |
		  (nodeid << (64 - TIMESTAMP_BITS - NODEID_BITS)) |
		  sequence;

	PG_RETURN_INT64(res);
}

/*
 * TODO - vote for sequence nodeid?
 *
 * HACK for testing: use database oid.
 */
static int16
global_seq_assign_nodeid(void)
{
	elog(ERROR, "sequence ID not allocated (TODO)");
}

/*
 * Read the unique node id for this node.
 */
static int16
global_seq_read_nodeid(void)
{
	int seq_id = bdr_local_node_seq_id();

	if (seq_id == -1)
		seq_id = global_seq_assign_nodeid();

	if (seq_id < 0 || seq_id > MAX_NODE_ID)
		elog(ERROR, "node sequence ID out of range 0 .. %d", MAX_NODE_ID);

	return (int16)seq_id;
}

/*
 * Get sequence nodeid with caching.
 */
static int16
global_seq_get_nodeid(void)
{
	if (seq_nodeid == -1)
		seq_nodeid = global_seq_read_nodeid();

	return seq_nodeid;
}
