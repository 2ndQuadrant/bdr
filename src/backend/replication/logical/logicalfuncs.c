/*-------------------------------------------------------------------------
 *
 * logicalfuncs.c
 *
 *	   Support functions for using xlog decoding
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logicalfuncs.c
 *
 */

#include "postgres.h"

#include <unistd.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "storage/fd.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/snapbuild.h"

Datum		init_logical_replication(PG_FUNCTION_ARGS);
Datum		stop_logical_replication(PG_FUNCTION_ARGS);
Datum		pg_stat_get_logical_decoding_slots(PG_FUNCTION_ARGS);

/* FIXME: duplicate code with pg_xlogdump, similar to walsender.c */
static void
XLogRead(char *buf, XLogRecPtr startptr, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	static int	sendFile = -1;
	static XLogSegNo sendSegNo = 0;
	static uint32 sendOff = 0;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = recptr % XLogSegSize;

		if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo))
		{
			char		path[MAXPGPATH];

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo);

			XLogFilePath(path, ThisTimeLineID, sendSegNo);

			sendFile = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);

			if (sendFile < 0)
			{
				if (errno == ENOENT)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("requested WAL segment %s has already been removed",
									path)));
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\": %m",
									path)));
			}
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
			{
				char		path[MAXPGPATH];

				XLogFilePath(path, ThisTimeLineID, sendSegNo);

				ereport(ERROR,
						(errcode_for_file_access(),
				  errmsg("could not seek in log segment %s to offset %u: %m",
						 path, startoff)));
			}
			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (XLogSegSize - startoff))
			segbytes = XLogSegSize - startoff;
		else
			segbytes = nbytes;

		readbytes = read(sendFile, p, segbytes);
		if (readbytes <= 0)
		{
			char		path[MAXPGPATH];

			XLogFilePath(path, ThisTimeLineID, sendSegNo);

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from log segment %s, offset %u, length %lu: %m",
							path, sendOff, (unsigned long) segbytes)));
		}

		/* Update state for read */
		recptr += readbytes;

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

int
logical_read_local_xlog_page(XLogReaderState * state, XLogRecPtr targetPagePtr,
	int reqLen, XLogRecPtr targetRecPtr, char *cur_page, TimeLineID *pageTLI)
{
	XLogRecPtr	flushptr,
				loc;
	int			count;

	loc = targetPagePtr + reqLen;
	while (1)
	{
		flushptr = GetFlushRecPtr();
		if (loc <= flushptr)
			break;
		pg_usleep(1000L);
	}

	/* more than one block available */
	if (targetPagePtr + XLOG_BLCKSZ <= flushptr)
		count = XLOG_BLCKSZ;
	/* not enough data there */
	else if (targetPagePtr + reqLen > flushptr)
		return -1;
	/* part of the page available */
	else
		count = flushptr - targetPagePtr;

	/* FIXME: more sensible/efficient implementation */
	XLogRead(cur_page, targetPagePtr, XLOG_BLCKSZ);

	return count;
}

static void
DummyWrite(LogicalDecodingContext * ctx, XLogRecPtr lsn, TransactionId xid)
{
	elog(ERROR, "init_logical_replication shouldn't be writing anything");
}

Datum
init_logical_replication(PG_FUNCTION_ARGS)
{
	Name		name = PG_GETARG_NAME(0);
	Name		plugin = PG_GETARG_NAME(1);

	char		xpos[MAXFNAMELEN];

	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		result;
	Datum		values[2];
	bool		nulls[2];
	LogicalDecodingContext *ctx = NULL;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Acquire a logical replication slot */
	CheckLogicalReplicationRequirements();
	LogicalDecodingAcquireFreeSlot(NameStr(*name), NameStr(*plugin));

	/* make sure we don't end up with an unreleased slot */
	PG_TRY();
	{
		XLogRecPtr	startptr;

		/*
		 * Use the same initial_snapshot_reader, but with our own read_page
		 * callback that does not depend on walsender.
		 */
		MyLogicalDecodingSlot->last_required_checkpoint = GetRedoRecPtr();

		ctx = CreateLogicalDecodingContext(MyLogicalDecodingSlot, true, NIL,
					   logical_read_local_xlog_page, DummyWrite, DummyWrite);

		/* setup from where to read xlog */
		startptr = ctx->slot->last_required_checkpoint;
		/* Wait for a consistent starting point */
		for (;;)
		{
			XLogRecord *record;
			XLogRecordBuffer buf;
			char	   *err = NULL;

			/* the read_page callback waits for new WAL */
			record = XLogReadRecord(ctx->reader, startptr, &err);
			if (err)
				elog(ERROR, "%s", err);

			Assert(record);

			startptr = InvalidXLogRecPtr;

			buf.origptr = ctx->reader->ReadRecPtr;
			buf.record = *record;
			buf.record_data = XLogRecGetData(record);
			DecodeRecordIntoReorderBuffer(ctx, &buf);

			if (LogicalDecodingContextReady(ctx))
				break;
		}

		/* Extract the values we want */
		MyLogicalDecodingSlot->confirmed_flush = ctx->reader->EndRecPtr;
		snprintf(xpos, sizeof(xpos), "%X/%X",
				 (uint32) (MyLogicalDecodingSlot->confirmed_flush >> 32),
				 (uint32) MyLogicalDecodingSlot->confirmed_flush);
	}
	PG_CATCH();
	{
		LogicalDecodingReleaseSlot();
		PG_RE_THROW();
	}
	PG_END_TRY();

	values[0] = CStringGetTextDatum(NameStr(MyLogicalDecodingSlot->name));
	values[1] = CStringGetTextDatum(xpos);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	LogicalDecodingReleaseSlot();

	PG_RETURN_DATUM(result);
}

Datum
stop_logical_replication(PG_FUNCTION_ARGS)
{
	Name		name = PG_GETARG_NAME(0);

	CheckLogicalReplicationRequirements();
	LogicalDecodingFreeSlot(NameStr(*name));

	PG_RETURN_INT32(0);
}

/*
 * Return one row for each logical replication slot currently in use.
 */

Datum
pg_stat_get_logical_decoding_slots(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_LOGICAL_DECODING_SLOTS_COLS 6
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int			i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < max_logical_slots; i++)
	{
		LogicalDecodingSlot *slot = &LogicalDecodingCtl->logical_slots[i];
		Datum		values[PG_STAT_GET_LOGICAL_DECODING_SLOTS_COLS];
		bool		nulls[PG_STAT_GET_LOGICAL_DECODING_SLOTS_COLS];
		char		location[MAXFNAMELEN];
		const char *slot_name;
		const char *plugin;
		TransactionId xmin;
		XLogRecPtr	last_req;
		bool		active;
		Oid			database;

		SpinLockAcquire(&slot->mutex);
		if (!slot->in_use)
		{
			SpinLockRelease(&slot->mutex);
			continue;
		}
		else
		{
			xmin = slot->xmin;
			active = slot->active;
			database = slot->database;
			last_req = slot->last_required_checkpoint;
			slot_name = pstrdup(NameStr(slot->name));
			plugin = pstrdup(NameStr(slot->plugin));
		}
		SpinLockRelease(&slot->mutex);

		memset(nulls, 0, sizeof(nulls));

		snprintf(location, sizeof(location), "%X/%X",
				 (uint32) (last_req >> 32), (uint32) last_req);

		values[0] = CStringGetTextDatum(slot_name);
		values[1] = CStringGetTextDatum(plugin);
		values[2] = database;
		values[3] = BoolGetDatum(active);
		values[4] = TransactionIdGetDatum(xmin);
		values[5] = CStringGetTextDatum(location);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
