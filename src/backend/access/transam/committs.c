/*-------------------------------------------------------------------------
 *
 * committs.c
 *		PostgreSQL commit timestamp manager
 *
 * This module is a pg_clog-like system that stores the commit timestamp
 * for each transaction.
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * CLOG page is initialized to zeroes.	Other writes of CommitTS come from
 * recording of transaction commit in xact.c, which generates its own XLOG
 * records for these events and will re-perform the status update on redo;
 * so we need make no additional XLOG entry here.
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
#include "access/slru.h"
#include "access/transam.h"
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

/* We need eight bytes per xact */
#define COMMITTS_XACTS_PER_PAGE (BLCKSZ / sizeof(TimestampTz))

#define TransactionIdToCTsPage(xid)	\
	((xid) / (TransactionId) COMMITTS_XACTS_PER_PAGE)
#define TransactionIdToCTsEntry(xid)	\
	((xid) % (TransactionId) COMMITTS_XACTS_PER_PAGE)

/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData CommitTsCtlData;

#define CommitTsCtl (&CommitTsCtlData)

/* GUC variables */
bool	commit_ts_enabled;

static void SetXidCommitTsInPage(TransactionId xid, int nsubxids,
					 TransactionId *subxids, TimestampTz committs, int pageno);
static void TransactionIdSetCommitTs(TransactionId xid, TimestampTz committs,
						  int slotno);
static int	ZeroCommitTsPage(int pageno, bool writeXlog);
static bool CommitTsPagePrecedes(int page1, int page2);
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno);


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
 */
void
TransactionTreeSetCommitTimestamp(TransactionId xid, int nsubxids,
								  TransactionId *subxids, TimestampTz timestamp)
{
	int			i = 0;
	TransactionId headxid = xid;

	if (!commit_ts_enabled)
		return;

	/*
	 * We split the xids to set the timestamp to in groups belonging to the
	 * same SLRU page; the first element in each such set is its head.  The
	 * first group has the main XID as the head; subsequent sets use the
	 * first subxid not on the previous page as head.  This way, we only have
	 * to lock/modify each SLRU page once.
	 */
	for (;;)
	{
		int			pageno = TransactionIdToCTsPage(headxid);
		int			j;

		for (j = i; j < nsubxids; j++)
		{
			if (TransactionIdToCTsPage(subxids[j]) != pageno)
				break;
		}
		/* subxids[i..j] are on the same page as the head */

		SetXidCommitTsInPage(headxid, j - i, subxids + i, timestamp, pageno);

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
}

/*
 * Record the commit timestamp of transaction entries in the commit log for all
 * entries on a single page.  Atomic only on this page.
 */
static void
SetXidCommitTsInPage(TransactionId xid, int nsubxids,
					 TransactionId *subxids, TimestampTz committs, int pageno)
{
	int			slotno;
	int			i;

	LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CommitTsCtl, pageno, true, xid);

	TransactionIdSetCommitTs(xid, committs, slotno);
	for (i = 0; i < nsubxids; i++)
		TransactionIdSetCommitTs(subxids[i], committs, slotno);

	CommitTsCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(CommitTsControlLock);
}

/*
 * Sets the commit timestamp of a single transaction.
 *
 * Must be called with CommitTsControlLock held
 */
static void
TransactionIdSetCommitTs(TransactionId xid, TimestampTz committs, int slotno)
{
	int			entryno = TransactionIdToCTsEntry(xid);
	TimestampTz *timeptr;

	timeptr = (TimestampTz *) CommitTsCtl->shared->page_buffer[slotno];
	timeptr += entryno;

	*timeptr = committs;
}

/*
 * Interrogate the commit timestamp of a transaction.
 */
TimestampTz
TransactionIdGetCommitTimestamp(TransactionId xid)
{
	int			pageno = TransactionIdToCTsPage(xid);
	int			entryno = TransactionIdToCTsEntry(xid);
	int			slotno;
	TimestampTz *timeptr;
	TimestampTz	committs;

	if (!commit_ts_enabled)
		return 0;

	/*
	 * FIXME -- we need a more useful lower bound here.  For now, we use
	 * RecentGlobalXmin which is ensured to be sane if set.  (RecentXmin might
	 * be set to FirstNormalTransactionId if no snapshot has been taken by this
	 * session, so avoid that).
	 */
	if (!TransactionIdIsValid(RecentGlobalXmin) ||
		TransactionIdPrecedes(xid, RecentGlobalXmin))
		return 0;

	/* lock is acquired by SimpleLruReadPage_ReadOnly */

	slotno = SimpleLruReadPage_ReadOnly(CommitTsCtl, pageno, xid);
	timeptr = (TimestampTz *) CommitTsCtl->shared->page_buffer[slotno];
	timeptr += entryno;
	committs = *timeptr;

	LWLockRelease(CommitTsControlLock);

	return committs;
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

	committs = TransactionIdGetCommitTimestamp(xid);

	PG_RETURN_TIMESTAMPTZ(committs);
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
	return SimpleLruShmemSize(CommitTsShmemBuffers(), 0);
}

void
CommitTsShmemInit(void)
{
	CommitTsCtl->PagePrecedes = CommitTsPagePrecedes;
	SimpleLruInit(CommitTsCtl, "CommitTs Ctl", CommitTsShmemBuffers(), 0,
				  CommitTsControlLock, "pg_committs");
}

/*
 * This func must be called ONCE on system install.  It creates the initial
 * CommitTs segment.  (The CommitTs directory is assumed to have been created
 * by initdb, and CommitTsShmemInit must have been called already.)
 */
void
BootStrapCommitTs(void)
{
	int			slotno;

	LWLockAcquire(CommitTsControlLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCommitTsPage(0, false);

	/* Make sure it's written out */
	SimpleLruWritePage(CommitTsCtl, slotno);
	Assert(!CommitTsCtl->shared->page_dirty[slotno]);

	LWLockRelease(CommitTsControlLock);
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

	LWLockRelease(CommitTsControlLock);

	if (!commit_ts_enabled)
		return;

	if (!SimpleLruDoesPhysicalPageExist(ctl, pageno))
		SimpleLruZeroPage(ctl, pageno);
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
	XLogRecPtr	recptr;

	rdata.data = (char *) (&pageno);
	rdata.len = sizeof(int);
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	recptr = XLogInsert(RM_COMMITTS_ID, COMMITTS_TRUNCATE, &rdata);
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
	else
		elog(PANIC, "committs_redo: unknown op code %u", info);
}
