/*-------------------------------------------------------------------------
 *
 * committs.c
 *		PostgreSQL commit timestamp manager
 *
 * This module is a pg_clog-like system that stores the commit timestamp
 * for each transaction.
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * CommitTs page is initialized to zeroes.  Also, one XLOG record is
 * generated for setting of values when the caller requests it; this allows
 * us to support values coming from places other than transaction commit.
 * Other writes of CommitTS come from recording of transaction commit in
 * xact.c, which generates its own XLOG records for these events and will
 * re-perform the status update on redo; so we need make no additional XLOG
 * entry here.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/committs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/committs.h"
#include "access/htup_details.h"
#include "access/slru.h"
#include "access/transam.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/*
 * Defines for CommitTs page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CommitTs page numbering also wraps around at
 * 0xFFFFFFFF/COMMITTS_XACTS_PER_PAGE, and CommitTs segment numbering at
 * 0xFFFFFFFF/COMMITTS_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCommitTs (see CommitTsPagePrecedes).
 */

/* We need 8+4 bytes per xact */
typedef struct CommitTimestampEntry
{
	TimestampTz		time;
	CommitExtraData	extra;
} CommitTimestampEntry;

#define SizeOfCommitTimestampEntry (offsetof(CommitTimestampEntry, extra) + \
									sizeof(CommitExtraData))

#define COMMITTS_XACTS_PER_PAGE \
	(BLCKSZ / SizeOfCommitTimestampEntry)

#define TransactionIdToCTsPage(xid)	\
	((xid) / (TransactionId) COMMITTS_XACTS_PER_PAGE)
#define TransactionIdToCTsEntry(xid)	\
	((xid) % (TransactionId) COMMITTS_XACTS_PER_PAGE)

/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData CommitTsCtlData;

#define CommitTsCtl (&CommitTsCtlData)

/*
 * We keep a cache of the last value set in shared memory.  This is protected
 * by CommitTsLock.
 */
typedef struct CommitTimestampShared
{
	TransactionId	xidLastCommit;
	CommitTimestampEntry dataLastCommit;
} CommitTimestampShared;

CommitTimestampShared	*commitTsShared;


/* GUC variables */
bool	commit_ts_enabled;

static void SetXidCommitTsInPage(TransactionId xid, int nsubxids,
					 TransactionId *subxids, TimestampTz committs,
					 CommitExtraData extra, int pageno);
static void TransactionIdSetCommitTs(TransactionId xid, TimestampTz committs,
						  CommitExtraData extra, int slotno);
static int	ZeroCommitTsPage(int pageno, bool writeXlog);
static bool CommitTsPagePrecedes(int page1, int page2);
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno);
static void WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, TimestampTz timestamp,
						 CommitExtraData data);


/*
 * TransactionTreeSetCommitTimestamp
 *
 * Record the final commit timestamp of transaction entries in the commit log
 * for a transaction and its subtransaction tree, as efficiently as possible.
 *
 * xid is the top level transaction id.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * The do_xlog parameter tells us whether to include a XLog record of this
 * or not.  Normal path through RecordTransactionCommit() will be related
 * to a transaction commit XLog record, and so should pass "false" here.
 * Other callers probably want to pass true, so that the given values persist
 * in case of crashes.
 */
void
TransactionTreeSetCommitTimestamp(TransactionId xid, int nsubxids,
								  TransactionId *subxids, TimestampTz timestamp,
								  CommitExtraData extra, bool do_xlog)
{
	int			i;
	TransactionId headxid;

	Assert(xid != InvalidTransactionId);

	if (!commit_ts_enabled)
		return;

	/*
	 * Comply with the WAL-before-data rule: if caller specified it wants
	 * this value to be recorded in WAL, do so before touching the data.
	 */
	if (do_xlog)
		WriteSetTimestampXlogRec(xid, nsubxids, subxids, timestamp, extra);

	/*
	 * We split the xids to set the timestamp to in groups belonging to the
	 * same SLRU page; the first element in each such set is its head.  The
	 * first group has the main XID as the head; subsequent sets use the
	 * first subxid not on the previous page as head.  This way, we only have
	 * to lock/modify each SLRU page once.
	 */
	for (i = 0, headxid = xid;;)
	{
		int			pageno = TransactionIdToCTsPage(headxid);
		int			j;

		for (j = i; j < nsubxids; j++)
		{
			if (TransactionIdToCTsPage(subxids[j]) != pageno)
				break;
		}
		/* subxids[i..j] are on the same page as the head */

		SetXidCommitTsInPage(headxid, j - i, subxids + i, timestamp, extra,
							 pageno);

		/* if we wrote out all subxids, we're done. */
		if (j + 1 >= nsubxids)
			break;

		/*
		 * Set the new head and skip over it, as well as over the subxids
		 * we just wrote.
		 */
		headxid = subxids[j];
		i += j - i + 1;
	}

	/*
	 * Update the cached value in shared memory
	 */
	LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
	commitTsShared->xidLastCommit = xid;
	commitTsShared->dataLastCommit.time = timestamp;
	commitTsShared->dataLastCommit.extra = extra;
	LWLockRelease(CommitTsLock);
}

/*
 * Record the commit timestamp of transaction entries in the commit log for all
 * entries on a single page.  Atomic only on this page.
 */
static void
SetXidCommitTsInPage(TransactionId xid, int nsubxids,
					 TransactionId *subxids, TimestampTz committs,
					 CommitExtraData extra, int pageno)
{
	int			slotno;
	int			i;

	LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CommitTsCtl, pageno, true, xid);

	TransactionIdSetCommitTs(xid, committs, extra, slotno);
	for (i = 0; i < nsubxids; i++)
		TransactionIdSetCommitTs(subxids[i], committs, extra, slotno);

	CommitTsCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(CommitTsControlLock);
}

/*
 * Sets the commit timestamp of a single transaction.
 *
 * Must be called with CommitTsControlLock held
 */
static void
TransactionIdSetCommitTs(TransactionId xid, TimestampTz committs,
						 CommitExtraData extra, int slotno)
{
	int			entryno = TransactionIdToCTsEntry(xid);
	CommitTimestampEntry *entry;

	entry = (CommitTimestampEntry *)
		(CommitTsCtl->shared->page_buffer[slotno] +
		 SizeOfCommitTimestampEntry * entryno);

	entry->time = committs;
	entry->extra = extra;
}

/*
 * Interrogate the commit timestamp of a transaction.
 */
void
TransactionIdGetCommitTsData(TransactionId xid, TimestampTz *ts,
							 CommitExtraData *data)
{
	int			pageno = TransactionIdToCTsPage(xid);
	int			entryno = TransactionIdToCTsEntry(xid);
	int			slotno;
	CommitTimestampEntry *entry;
	TransactionId oldestCommitTs;

	/* Return empty if module not enabled */
	if (!commit_ts_enabled)
	{
		if (ts)
			*ts = 0;
		if (data)
			*data = (CommitExtraData) 0;
		return;
	}

	/* Also return empty if the requested value is older than what we have */
	LWLockAcquire(CommitTsControlLock, LW_SHARED);
	oldestCommitTs = ShmemVariableCache->oldestCommitTs;
	LWLockRelease(CommitTsControlLock);

	if (!TransactionIdIsValid(oldestCommitTs) ||
		TransactionIdPrecedes(xid, oldestCommitTs))
	{
		if (ts)
			*ts = 0;
		if (data)
			*data = (CommitExtraData) 0;
		return;
	}

	/*
	 * Use an unlocked atomic read on our cached value in shared memory;
	 * if it's a hit, acquire a lock and read the data, after verifying
	 * that it's still what we initially read.  Otherwise, fall through
	 * to read from SLRU.
	 */
	if (commitTsShared->xidLastCommit == xid)
	{
		LWLockAcquire(CommitTsLock, LW_SHARED);
		if (commitTsShared->xidLastCommit == xid)
		{
			if (ts)
				*ts = commitTsShared->dataLastCommit.time;
			if (data)
				*data = commitTsShared->dataLastCommit.extra;
			LWLockRelease(CommitTsLock);
			return;
		}
		LWLockRelease(CommitTsLock);
	}

	/* lock is acquired by SimpleLruReadPage_ReadOnly */
	slotno = SimpleLruReadPage_ReadOnly(CommitTsCtl, pageno, xid);
	entry = (CommitTimestampEntry *)
		(CommitTsCtl->shared->page_buffer[slotno] +
		 SizeOfCommitTimestampEntry * entryno);

	if (ts)
		*ts = entry->time;

	if (data)
		*data = entry->extra;

	LWLockRelease(CommitTsControlLock);
}

/*
 * Return the Xid of the latest committed transaction.  (As far as this module
 * is concerned, anyway; it's up to the caller to ensure the value is useful
 * for its purposes.)
 *
 * ts and extra are filled with the corresponding data; they can be passed
 * as NULL if not wanted.
 */
TransactionId
GetLatestCommitTimestampData(TimestampTz *ts, CommitExtraData *extra)
{
	TransactionId	xid;

	/* Return empty if module not enabled */
	if (!commit_ts_enabled)
	{
		if (ts)
			*ts = InvalidTransactionId;
		if (extra)
			*extra = (CommitExtraData) 0;
		return InvalidTransactionId;
	}

	LWLockAcquire(CommitTsLock, LW_SHARED);
	xid = commitTsShared->xidLastCommit;
	if (ts)
		*ts = commitTsShared->dataLastCommit.time;
	if (extra)
		*extra = commitTsShared->dataLastCommit.extra;
	LWLockRelease(CommitTsLock);

	return xid;
}

/*
 * SQL-callable wrapper to obtain commit time of a transaction
 */
PG_FUNCTION_INFO_V1(pg_get_transaction_committime);
Datum
pg_get_transaction_committime(PG_FUNCTION_ARGS)
{
	TransactionId	xid = PG_GETARG_UINT32(0);
	TimestampTz		committs;

	TransactionIdGetCommitTsData(xid, &committs, NULL);

	PG_RETURN_TIMESTAMPTZ(committs);
}

PG_FUNCTION_INFO_V1(pg_get_transaction_extradata);
Datum
pg_get_transaction_extradata(PG_FUNCTION_ARGS)
{
	TransactionId	xid = PG_GETARG_UINT32(0);
	CommitExtraData	data;

	TransactionIdGetCommitTsData(xid, NULL, &data);

	PG_RETURN_INT32(data);
}

PG_FUNCTION_INFO_V1(pg_get_transaction_committime_data);
Datum
pg_get_transaction_committime_data(PG_FUNCTION_ARGS)
{
	TransactionId	xid = PG_GETARG_UINT32(0);
	TimestampTz		committs;
	CommitExtraData	data;
	Datum       values[2];
	bool        nulls[2];
	TupleDesc   tupdesc;
	HeapTuple	htup;

	/*
	 * Construct a tuple descriptor for the result row.  This must match this
	 * function's pg_proc entry!
	 */
	tupdesc = CreateTemplateTupleDesc(2, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "timestamp",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "extra",
					   INT4OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	/* and construct a tuple with our data */
	TransactionIdGetCommitTsData(xid, &committs, &data);

	values[0] = TimestampTzGetDatum(committs);
	nulls[0] = false;

	values[1] = Int32GetDatum(data);
	nulls[1] = false;

	htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

PG_FUNCTION_INFO_V1(pg_get_latest_transaction_committime_data);
Datum
pg_get_latest_transaction_committime_data(PG_FUNCTION_ARGS)
{
	TransactionId	xid;
	TimestampTz		committs;
	CommitExtraData	data;
	Datum       values[3];
	bool        nulls[3];
	TupleDesc   tupdesc;
	HeapTuple	htup;

	/*
	 * Construct a tuple descriptor for the result row.  This must match this
	 * function's pg_proc entry!
	 */
	tupdesc = CreateTemplateTupleDesc(3, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "xid",
					   XIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "timestamp",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "extra",
					   INT4OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	/* and construct a tuple with our data */
	xid = GetLatestCommitTimestampData(&committs, &data);

	values[0] = TransactionIdGetDatum(xid);
	nulls[0] = false;

	values[1] = TimestampTzGetDatum(committs);
	nulls[1] = false;

	values[2] = Int32GetDatum(data);
	nulls[2] = false;

	htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/*
 * Number of shared CommitTS buffers.
 *
 * We use a very similar logic as for the number of CLOG buffers; see comments
 * in CLOGShmemBuffers.
 */
Size
CommitTsShmemBuffers(void)
{
	return Min(16, Max(4, NBuffers / 1024));
}

/*
 * Initialization of shared memory for CommitTs
 */
Size
CommitTsShmemSize(void)
{
	return SimpleLruShmemSize(CommitTsShmemBuffers(), 0) +
		sizeof(CommitTimestampShared);
}

void
CommitTsShmemInit(void)
{
	bool	found;

	CommitTsCtl->PagePrecedes = CommitTsPagePrecedes;
	SimpleLruInit(CommitTsCtl, "CommitTs Ctl", CommitTsShmemBuffers(), 0,
				  CommitTsControlLock, "pg_committs");

	commitTsShared = ShmemInitStruct("CommitTs shared",
									 sizeof(CommitTimestampShared),
									 &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);

		commitTsShared->xidLastCommit = InvalidTransactionId;
		commitTsShared->dataLastCommit.time = 0;
		commitTsShared->dataLastCommit.extra = 0;
	}
	else
		Assert(found);
}

/*
 * This function must be called ONCE on system install.
 *
 * (The CommitTs directory is assumed to have been created by initdb, and
 * CommitTsShmemInit must have been called already.)
 */
void
BootStrapCommitTs(void)
{
	/*
	 * Nothing to do here at present, unlike most other SLRU modules; segments
	 * are created when the server is started with this module enabled.
	 * See StartupCommitTs.
	 */
}

/*
 * Initialize (or reinitialize) a page of CommitTs to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCommitTsPage(int pageno, bool writeXlog)
{
	int			slotno;

	slotno = SimpleLruZeroPage(CommitTsCtl, pageno);

	if (writeXlog)
		WriteZeroPageXlogRec(pageno);

	return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * This is in charge of creating the currently active segment, if it's not
 * already there.  The reason for this is that the server might have been
 * running with this module disabled for a while and thus might have skipped
 * the normal creation point.
 */
void
StartupCommitTs(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToCTsPage(xid);
	SlruCtl		ctl = CommitTsCtl;

	LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);

	/*
	 * Initialize our idea of the latest page number.
	 */
	CommitTsCtl->shared->latest_page_number = pageno;

	/*
	 * If this module is not currently enabled, make sure we don't hand back
	 * possibly-invalid data; also remove segments of old data.
	 */
	if (!commit_ts_enabled)
	{
		ShmemVariableCache->oldestCommitTs = InvalidTransactionId;
		LWLockRelease(CommitTsControlLock);

		TruncateCommitTs(ReadNewTransactionId());

		return;
	}

	/*
	 * If CommitTs is enabled, but it wasn't in the previous server run, we
	 * need to set the oldest value to the next Xid; that way, we will not try
	 * to read data that might not have been set.
	 *
	 * XXX does this have a problem if a server is started with commitTs
	 * enabled, then started with commitTs disabled, then restarted with it
	 * enabled again?  It doesn't look like it does, because there should be a
	 * checkpoint that sets the value to InvalidTransactionId at end of
	 * recovery; and so any chance of injecting new transactions without
	 * CommitTs values would occur after the oldestCommitTs has been set to
	 * Invalid temporarily.
	 */
	if (ShmemVariableCache->oldestCommitTs == InvalidTransactionId)
		ShmemVariableCache->oldestCommitTs = ReadNewTransactionId();

	/* Finally, create the current segment file, if necessary */
	if (!SimpleLruDoesPhysicalPageExist(ctl, pageno))
	{
		int		slotno;

		slotno = ZeroCommitTsPage(pageno, false);
		SimpleLruWritePage(CommitTsCtl, slotno);
		Assert(!CommitTsCtl->shared->page_dirty[slotno]);
	}

	LWLockRelease(CommitTsControlLock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCommitTs(void)
{
	/* Flush dirty CommitTs pages to disk */
	SimpleLruFlush(CommitTsCtl, false);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCommitTs(void)
{
	/* Flush dirty CommitTs pages to disk */
	SimpleLruFlush(CommitTsCtl, true);
}

/*
 * Make sure that CommitTs has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty CommitTs or xlog page to make room
 * in shared memory.
 */
void
ExtendCommitTs(TransactionId newestXact)
{
	int			pageno;

	/* nothing to do if module not enabled */
	if (!commit_ts_enabled)
		return;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToCTsEntry(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToCTsPage(newestXact);

	LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCommitTsPage(pageno, !InRecovery);

	LWLockRelease(CommitTsControlLock);
}

/*
 * Remove all CommitTs segments before the one holding the passed
 * transaction ID
 *
 * Note that we don't need to flush XLOG here.
 */
void
TruncateCommitTs(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToCTsPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!SlruScanDirectory(CommitTsCtl, SlruScanDirCbReportPresence, &cutoffPage))
		return;					/* nothing to remove */

	/* Write XLOG record */
	WriteTruncateXlogRec(cutoffPage);

	/* Now we can remove the old CommitTs segment(s) */
	SimpleLruTruncate(CommitTsCtl, cutoffPage);
}

/*
 * Set the earliest value for which commit TS can be consulted.
 */
void
SetCommitTsLimit(TransactionId oldestXact)
{
	/*
	 * Be careful not to overwrite values that are either further into the
	 * "future" or signal a disabled committs.
	 */
	LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);
	if (ShmemVariableCache->oldestCommitTs != InvalidTransactionId)
	{
		if (TransactionIdPrecedes(ShmemVariableCache->oldestCommitTs, oldestXact))
			ShmemVariableCache->oldestCommitTs = oldestXact;
	}
	else
		ShmemVariableCache->oldestCommitTs = oldestXact;
	LWLockRelease(CommitTsControlLock);
}

/*
 * Decide which of two CLOG page numbers is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CommitTsPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * COMMITTS_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * COMMITTS_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}


/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroPageXlogRec(int pageno)
{
	XLogRecData rdata;

	rdata.data = (char *) (&pageno);
	rdata.len = sizeof(int);
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	(void) XLogInsert(RM_COMMITTS_ID, COMMITTS_ZEROPAGE, &rdata);
}

/*
 * Write a TRUNCATE xlog record
 */
static void
WriteTruncateXlogRec(int pageno)
{
	XLogRecData rdata;

	rdata.data = (char *) (&pageno);
	rdata.len = sizeof(int);
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	XLogInsert(RM_COMMITTS_ID, COMMITTS_TRUNCATE, &rdata);
}

/*
 * Write a SETTS xlog record
 */
static void
WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, TimestampTz timestamp,
						 CommitExtraData data)
{
	XLogRecData	rdata;
	xl_committs_set	record;

	record.timestamp = timestamp;
	record.data = data;
	record.mainxid = mainxid;
	record.nsubxids = nsubxids;
	memcpy(record.subxids, subxids, sizeof(TransactionId) * nsubxids);

	rdata.data = (char *) &record;
	rdata.len = offsetof(xl_committs_set, subxids) +
		nsubxids * sizeof(TransactionId);
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	XLogInsert(RM_COMMITTS_ID, COMMITTS_SETTS, &rdata);
}


/*
 * CommitTS resource manager's routines
 */
void
committs_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	/* Backup blocks are not used in committs records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	if (info == COMMITTS_ZEROPAGE)
	{
		int			pageno;
		int			slotno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);

		slotno = ZeroCommitTsPage(pageno, false);
		SimpleLruWritePage(CommitTsCtl, slotno);
		Assert(!CommitTsCtl->shared->page_dirty[slotno]);

		LWLockRelease(CommitTsControlLock);
	}
	else if (info == COMMITTS_TRUNCATE)
	{
		int			pageno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		/*
		 * During XLOG replay, latest_page_number isn't set up yet; insert a
		 * suitable value to bypass the sanity test in SimpleLruTruncate.
		 */
		CommitTsCtl->shared->latest_page_number = pageno;

		SimpleLruTruncate(CommitTsCtl, pageno);
	}
	else if (info == COMMITTS_SETTS)
	{
		xl_committs_set *setts = (xl_committs_set *) XLogRecGetData(record);

		TransactionTreeSetCommitTimestamp(setts->mainxid, setts->nsubxids,
										  setts->subxids, setts->timestamp,
										  setts->data, false);
	}
	else
		elog(PANIC, "committs_redo: unknown op code %u", info);
}
