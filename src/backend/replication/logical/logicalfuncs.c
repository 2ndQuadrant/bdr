/*-------------------------------------------------------------------------
 *
 * logicalfuncs.c
 *
 *	   Support functions for using changeset extraction and managing
 *	   logical replication slots via SQL.
 *
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logicalfuncs.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/pg_type.h"

#include "nodes/makefuncs.h"

#include "mb/pg_wchar.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/lsyscache.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"

#include "storage/fd.h"

/* private date for writing out data */
typedef struct DecodingOutputState {
	Tuplestorestate *tupstore;
	TupleDesc tupdesc;
	bool binary_output;
} DecodingOutputState;

/*
 * Prepare for a output plugin write.
 */
static void
LogicalOutputPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
						  bool last_write)
{
	resetStringInfo(ctx->out);
}

/*
 * Perform output plugin write into tuplestore.
 */
static void
LogicalOutputWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
				   bool last_write)
{
	Datum		values[3];
	bool		nulls[3];
	char		buf[60];
	DecodingOutputState *p;

	/* SQL Datums can only be of a limited length... */
	if (ctx->out->len > MaxAllocSize - VARHDRSZ)
		elog(ERROR, "too much output for sql interface");

	p = (DecodingOutputState *) ctx->output_writer_private;

	sprintf(buf, "%X/%X", (uint32) (lsn >> 32), (uint32) lsn);

	memset(nulls, 0, sizeof(nulls));
	values[0] = CStringGetTextDatum(buf);
	values[1] = TransactionIdGetDatum(xid);

	/*
	 * Assert ctx->out is in database encoding when we're writing textual
	 * output.
	 */
	if (!p->binary_output)
		Assert(pg_verify_mbstr(GetDatabaseEncoding(),
							   ctx->out->data, ctx->out->len,
							   false));

	/* ick, but cstring_to_text_with_len works for bytea perfectly fine */
	values[2] = PointerGetDatum(
		cstring_to_text_with_len(ctx->out->data, ctx->out->len));

	tuplestore_putvalues(p->tupstore, p->tupdesc, values, nulls);
}

/*
 * TODO: This is duplicate code with pg_xlogdump, similar to walsender.c, but
 * we currently don't have the infrastructure (elog!) to share it.
 */
static void
XLogRead(char *buf, TimeLineID tli, XLogRecPtr startptr, Size count)
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

			XLogFilePath(path, tli, sendSegNo);

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

				XLogFilePath(path, tli, sendSegNo);

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

			XLogFilePath(path, tli, sendSegNo);

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

static void
check_permissions(void)
{
	if (!superuser() && !has_rolreplication(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser or replication role to use changeset extraction"))));
}

/*
 * read_page callback for logical decoding contexts.
 *
 * Public because it would likely be very helpful for someone writing another
 * output method outside walsender, e.g. in a bgworker.
 *
 * TODO: The walsender has it's own version of this, but it relies on the
 * walsender's latch being set whenever WAL is flushed. No such infrastructure
 * exists for normal backends, so we have to do a check/sleep/repeat style of
 * loop for now..
 */
int
logical_read_local_xlog_page(XLogReaderState *state, XLogRecPtr targetPagePtr,
	int reqLen, XLogRecPtr targetRecPtr, char *cur_page, TimeLineID *pageTLI)
{
	XLogRecPtr	flushptr,
				loc;
	int			count;

	loc = targetPagePtr + reqLen;
	while (1)
	{
		/*
		 * TODO: we're going to have to do something more intelligent about
		 * timelines on standbys. Use readTimeLineHistory() and
		 * tliOfPointInHistory() to get the proper LSN? For now we'll catch
		 * that case earlier, but the code and TODO is left in here for when
		 * that changes.
		 */
		if (!RecoveryInProgress())
		{
			*pageTLI = ThisTimeLineID;
			flushptr = GetFlushRecPtr();
		}
		else
			flushptr = GetXLogReplayRecPtr(pageTLI);

		if (loc <= flushptr)
			break;

		CHECK_FOR_INTERRUPTS();
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

	XLogRead(cur_page, *pageTLI, targetPagePtr, XLOG_BLCKSZ);

	return count;
}

/*
 * SQL function for creating a new changeset extraction replication slot.
 */
Datum
pg_create_decoding_replication_slot(PG_FUNCTION_ARGS)
{
	Name		name = PG_GETARG_NAME(0);
	Name		plugin = PG_GETARG_NAME(1);

	char		xpos[MAXFNAMELEN];

	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		result;
	Datum		values[2];
	bool		nulls[2];

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	check_permissions();

	CheckLogicalDecodingRequirements();

	Assert(!MyReplicationSlot);

	/*
	 * Acquire a logical decoding slot, this will check for conflicting
	 * names.
	 */
	ReplicationSlotCreate(NameStr(*name), true);

	/* make sure we don't end up with an unreleased slot */
	PG_TRY();
	{
		LogicalDecodingContext *ctx = NULL;

		/*
		 * Create logical decoding context, to build the initial snapshot.
		 */
		ctx = CreateDecodingContext(
			true, NameStr(*plugin), InvalidXLogRecPtr, NIL,
			logical_read_local_xlog_page, NULL, NULL);

		/* build initial snapshot, might take a while */
		DecodingContextFindStartpoint(ctx);

		/* Extract the values we want */
		snprintf(xpos, sizeof(xpos), "%X/%X",
				 (uint32) (MyReplicationSlot->data.confirmed_flush >> 32),
				 (uint32) MyReplicationSlot->data.confirmed_flush);

		/* don't need the decoding context anymore */
		FreeDecodingContext(ctx);
	}
	PG_CATCH();
	{
		ReplicationSlotRelease();
		ReplicationSlotDrop(NameStr(*name));
		PG_RE_THROW();
	}
	PG_END_TRY();

	values[0] = CStringGetTextDatum(NameStr(MyReplicationSlot->data.name));
	values[1] = CStringGetTextDatum(xpos);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	ReplicationSlotRelease();

	PG_RETURN_DATUM(result);
}

/*
 * Helper function for the various SQL callable changeset extraction function
 * that output changes.
 */
static Datum
pg_decoding_slot_get_changes_guts(FunctionCallInfo fcinfo, bool confirm, bool binary)
{
	Name		name = PG_GETARG_NAME(0);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	XLogRecPtr	now;
	XLogRecPtr	startptr;

	LogicalDecodingContext *ctx;

	ResourceOwner old_resowner = CurrentResourceOwner;
	ArrayType  *arr;
	Size		ndim;
	List	   *options = NIL;
	DecodingOutputState *p;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* state to write output to */
	p = palloc(sizeof(DecodingOutputState));

	p->binary_output = binary;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &p->tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	check_permissions();

	CheckLogicalDecodingRequirements();

	arr = PG_GETARG_ARRAYTYPE_P(2);
	ndim = ARR_NDIM(arr);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (ndim > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s accepts only one dimension of arguments",
						get_func_name(fcinfo->flinfo->fn_oid))));
	}
	else if (array_contains_nulls(arr))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s does not accept NULL options",
						get_func_name(fcinfo->flinfo->fn_oid))));
	}
	else if (ndim == 1)
	{
		int			nelems;
		Datum	   *datum_opts;
		int			i;

		Assert(ARR_ELEMTYPE(arr) == TEXTOID);

		deconstruct_array(arr, TEXTOID, -1, false, 'i',
						  &datum_opts, NULL, &nelems);

		if (nelems % 2 != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s requires options to be passed in option, value pairs",
							get_func_name(fcinfo->flinfo->fn_oid))));
		}

		for (i = 0; i < nelems; i += 2)
		{
			char	   *name = TextDatumGetCString(datum_opts[i]);
			char	   *opt = TextDatumGetCString(datum_opts[i + 1]);

			options = lappend(options, makeDefElem(name, (Node *) makeString(opt)));
		}
	}

	p->tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = p->tupstore;
	rsinfo->setDesc = p->tupdesc;

	/*
	 * XXX: It's impolite to ignore our argument and keep decoding until the
	 * current position.
	 */
	if (!RecoveryInProgress())
		now = GetFlushRecPtr();
	else
		now = GetXLogReplayRecPtr(NULL);

	CheckLogicalDecodingRequirements();
	ReplicationSlotAcquire(NameStr(*name));

	PG_TRY();
	{
		ctx = CreateDecodingContext(false,
									NULL,
									MyReplicationSlot->data.confirmed_flush,
									options,
									logical_read_local_xlog_page,
									LogicalOutputPrepareWrite,
									LogicalOutputWrite);

		MemoryContextSwitchTo(oldcontext);

		/*
		 * Check whether the output pluggin writes textual output if that's
		 * what we need.
		 */
		if (!binary &&
			ctx->options.output_type != OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("output plugin uses binary output, but a textual interface is used")));

		ctx->output_writer_private = p;

		startptr = MyReplicationSlot->data.restart_lsn;

		CurrentResourceOwner = ResourceOwnerCreate(CurrentResourceOwner, "logical decoding");

		/* invalidate non-timetravel entries */
		InvalidateSystemCaches();

		while ((startptr != InvalidXLogRecPtr && startptr < now) ||
			   (ctx->reader->EndRecPtr && ctx->reader->EndRecPtr < now))
		{
			XLogRecord *record;
			char	   *errm = NULL;

			record = XLogReadRecord(ctx->reader, startptr, &errm);
			if (errm)
				elog(ERROR, "%s", errm);

			startptr = InvalidXLogRecPtr;

			/*
			 * The {begin_txn,change,commit_txn}_wrapper callbacks above will
			 * store the description into our tuplestore.
			 */
			if (record != NULL)
				LogicalDecodingProcessRecord(ctx, record);
		}
	}
	PG_CATCH();
	{
		ReplicationSlotRelease();

		/* clear all timetravel entries */
		InvalidateSystemCaches();

		PG_RE_THROW();
	}
	PG_END_TRY();

	tuplestore_donestoring(tupstore);

	CurrentResourceOwner = old_resowner;

	/*
	 * Next time, start where we left off. (Hunting things, the family
	 * business..)
	 */
	if (ctx->reader->EndRecPtr != InvalidXLogRecPtr && confirm)
		LogicalConfirmReceivedLocation(ctx->reader->EndRecPtr);

	/* free context, call shutdown callback */
	FreeDecodingContext(ctx);

	ReplicationSlotRelease();
	InvalidateSystemCaches();

	return (Datum) 0;
}

/*
 * SQL function returning the changestream as text, consuming the data.
 */
Datum
pg_decoding_slot_get_changes(PG_FUNCTION_ARGS)
{
	Datum ret = pg_decoding_slot_get_changes_guts(fcinfo, true, false);
	return ret;
}

/*
 * SQL function returning the changestream as text, only peeking ahead.
 */
Datum
pg_decoding_slot_peek_changes(PG_FUNCTION_ARGS)
{
	Datum ret = pg_decoding_slot_get_changes_guts(fcinfo, false, false);
	return ret;
}

/*
 * SQL function returning the changestream in binary, consuming the data.
 */
Datum
pg_decoding_slot_get_binary_changes(PG_FUNCTION_ARGS)
{
	Datum ret = pg_decoding_slot_get_changes_guts(fcinfo, true, true);
	return ret;
}

/*
 * SQL function returning the changestream in binary, only peeking ahead.
 */
Datum
pg_decoding_slot_peek_binary_changes(PG_FUNCTION_ARGS)
{
	Datum ret = pg_decoding_slot_get_changes_guts(fcinfo, false, true);
	return ret;
}
