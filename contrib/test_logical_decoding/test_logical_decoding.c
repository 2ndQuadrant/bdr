#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/timeline.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/snapbuild.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "storage/fd.h"
#include "miscadmin.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

Datum init_logical_replication(PG_FUNCTION_ARGS);
Datum start_logical_replication(PG_FUNCTION_ARGS);
Datum stop_logical_replication(PG_FUNCTION_ARGS);

static Tuplestorestate *tupstore = NULL;
static TupleDesc tupdesc;

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
				char	path[MAXPGPATH];
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
			char	path[MAXPGPATH];
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

static int
test_read_page(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen,
			   XLogRecPtr targetRecPtr, char* cur_page, TimeLineID *pageTLI)
{
    XLogRecPtr flushptr, loc;
    int count;

	loc = targetPagePtr + reqLen;
	while (1) {
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
LogicalOutputPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid)
{
	resetStringInfo(ctx->out);
}

static void
LogicalOutputWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid)
{
	Datum values[3];
	bool nulls[3];
	char buf[60];

	sprintf(buf, "%X/%X", (uint32)(lsn >> 32), (uint32)lsn);

	memset(nulls, 0, sizeof(nulls));
	values[0] = CStringGetTextDatum(buf);
	values[1] = Int64GetDatum(xid);
	values[2] = CStringGetTextDatum(ctx->out->data);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

PG_FUNCTION_INFO_V1(init_logical_replication);

Datum
init_logical_replication(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);
	Name plugin = PG_GETARG_NAME(1);

	char		xpos[MAXFNAMELEN];

	TupleDesc   tupdesc;
	HeapTuple   tuple;
	Datum       result;
	Datum       values[2];
	bool        nulls[2];
	LogicalDecodingContext *ctx = NULL;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Acquire a logical replication slot */
	CheckLogicalReplicationRequirements();
	LogicalDecodingAcquireFreeSlot(NameStr(*name), NameStr(*plugin));

	/* make sure we don't end up with an unreleased slot */
	PG_TRY();
	{
		XLogRecPtr startptr;

		/*
		 * Use the same initial_snapshot_reader, but with our own read_page
		 * callback that does not depend on walsender.
		 */
		MyLogicalDecodingSlot->last_required_checkpoint = GetRedoRecPtr();

		ctx = CreateLogicalDecodingContext(MyLogicalDecodingSlot, true, NIL,
				test_read_page, LogicalOutputPrepareWrite, LogicalOutputWrite);

		/* setup from where to read xlog */
		startptr = ctx->slot->last_required_checkpoint;
		/* Wait for a consistent starting point */
		for (;;)
		{
			XLogRecord *record;
			XLogRecordBuffer buf;
			char *err = NULL;

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

PG_FUNCTION_INFO_V1(start_logical_replication);

Datum
start_logical_replication(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	XLogRecPtr now;
	XLogRecPtr startptr;
	XLogRecPtr rp;

	LogicalDecodingContext *ctx;

	ResourceOwner old_resowner = CurrentResourceOwner;
	ArrayType *arr;
	Size ndim;
	List *options = NIL;

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

	arr = PG_GETARG_ARRAYTYPE_P(2);
	ndim = ARR_NDIM(arr);


	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (ndim > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("start_logical_replication only accept one dimension of arguments")));
	}
	else if (array_contains_nulls(arr))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("start_logical_replication expects NOT NULL options")));
	}
	else if (ndim == 1)
	{
		int		nelems;
		Datum  *datum_opts;
		int		i;
		Assert(ARR_ELEMTYPE(arr) == TEXTOID);

		deconstruct_array(arr, TEXTOID, -1, false, 'i',
						  &datum_opts, NULL, &nelems);

		if (nelems % 2 != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("options need to be specified pairwise")));
		}

		for (i = 0; i < nelems; i += 2)
		{
			char *name =VARDATA(DatumGetTextP(datum_opts[i]));
			char *opt = VARDATA(DatumGetTextP(datum_opts[i+1]));
			options = lappend(options, makeDefElem(name, (Node*)makeString(opt)));
		}
	}

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * XXX: It's impolite to ignore our argument and keep decoding
	 * until the current position.
	 */
	now = GetFlushRecPtr();

	/*
	 * We need to create a normal_snapshot_reader, but adjust it to use
	 * our page_read callback, and also make its reorder buffer use our
	 * callback wrappers that don't depend on walsender.
	 */

	CheckLogicalReplicationRequirements();
	LogicalDecodingReAcquireSlot(NameStr(*name));

	ctx = CreateLogicalDecodingContext(MyLogicalDecodingSlot, false, options,
				test_read_page, LogicalOutputPrepareWrite, LogicalOutputWrite);
	ctx->snapshot_builder->transactions_after = MyLogicalDecodingSlot->confirmed_flush;

	startptr = MyLogicalDecodingSlot->last_required_checkpoint;

	elog(DEBUG1, "Starting logical replication from %X/%X to %X/%x",
		 (uint32)(MyLogicalDecodingSlot->last_required_checkpoint>>32),
		 (uint32)MyLogicalDecodingSlot->last_required_checkpoint,
		 (uint32)(now>>32), (uint32)now);

	CurrentResourceOwner = ResourceOwnerCreate(CurrentResourceOwner, "logical decoding");

	PG_TRY();
	{

		while ((startptr != InvalidXLogRecPtr && startptr < now) ||
		       (ctx->reader->EndRecPtr && ctx->reader->EndRecPtr < now))
		{
			XLogRecord *record;
			char *errm = NULL;

			record = XLogReadRecord(ctx->reader, startptr, &errm);
			if (errm)
				elog(ERROR, "%s", errm);

			startptr = InvalidXLogRecPtr;

			if (record != NULL)
			{
				XLogRecordBuffer buf;

				buf.origptr = ctx->reader->ReadRecPtr;
				buf.record = *record;
				buf.record_data = XLogRecGetData(record);

				/*
				 * The {begin_txn,change,commit_txn}_wrapper callbacks above
				 * will store the description into our tuplestore.
				 */
				DecodeRecordIntoReorderBuffer(ctx, &buf);

			}
		}
	}
	PG_CATCH();
	{
		LogicalDecodingReleaseSlot();
		PG_RE_THROW();
	}
	PG_END_TRY();

	rp = ctx->reader->EndRecPtr;
	if (rp >= now)
	{
		elog(DEBUG1, "Reached endpoint (wanted: %X/%X, got: %X/%X)",
			 (uint32)(now>>32), (uint32)now,
			 (uint32)(rp>>32), (uint32)rp);
	}

	tuplestore_donestoring(tupstore);

	CurrentResourceOwner = old_resowner;

	/* Next time, start where we left off */
	MyLogicalDecodingSlot->confirmed_flush = ctx->reader->EndRecPtr;

	LogicalDecodingReleaseSlot();

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(stop_logical_replication);

Datum
stop_logical_replication(PG_FUNCTION_ARGS)
{
	Name name = PG_GETARG_NAME(0);

	CheckLogicalReplicationRequirements();
	LogicalDecodingFreeSlot(NameStr(*name));

	PG_RETURN_INT32(0);
}
