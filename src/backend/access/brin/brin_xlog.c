/*
 * brin_xlog.c
 *		XLog replay routines for BRIN indexes
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_xlog.c
 */
#include "postgres.h"

#include "access/brin_page.h"
#include "access/brin_pageops.h"
#include "access/brin_xlog.h"
#include "access/xlogutils.h"


/*
 * xlog replay routines
 */
static void
brin_xlog_createidx(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_brin_createidx *xlrec = (xl_brin_createidx *) XLogRecGetData(record);
	Buffer		buf;
	Page		page;

	/* create the index' metapage */
	buf = XLogInitBufferForRedo(record, 0);
	Assert(BufferIsValid(buf));
	page = (Page) BufferGetPage(buf);
	brin_metapage_init(page, xlrec->pagesPerRange, xlrec->version);
	PageSetLSN(page, lsn);
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

/*
 * Common part of an insert or update. Inserts the new tuple and updates the
 * revmap.
 */
static void
brin_xlog_insert_update(XLogReaderState *record,
						xl_brin_insert *xlrec)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffer;
	Page		page;
	XLogRedoAction action;

	/*
	 * If we inserted the first and only tuple on the page, re-initialize the
	 * page from scratch.
	 */
	if (XLogRecGetInfo(record) & XLOG_BRIN_INIT_PAGE)
	{
		buffer = XLogInitBufferForRedo(record, 0);
		page = BufferGetPage(buffer);
		brin_page_init(page, BRIN_PAGETYPE_REGULAR);
		action = BLK_NEEDS_REDO;
	}
	else
	{
		action = XLogReadBufferForRedo(record, 0, &buffer);
	}

	/* insert the index item into the page */
	if (action == BLK_NEEDS_REDO)
	{
		OffsetNumber offnum;
		BrinTuple  *tuple;
		Size		tuplen;

		tuple = (BrinTuple *) XLogRecGetBlockData(record, 0, &tuplen);

		Assert(tuple->bt_blkno == xlrec->heapBlk);

		page = (Page) BufferGetPage(buffer);
		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(page) + 1 < offnum)
			elog(PANIC, "brin_xlog_insert_update: invalid max offset number");

		offnum = PageAddItem(page, (Item) tuple, tuplen, offnum, true, false);
		if (offnum == InvalidOffsetNumber)
			elog(PANIC, "brin_xlog_insert_update: failed to add tuple");

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* update the revmap */
	action = XLogReadBufferForRedo(record, 1, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		ItemPointerData tid;
		BlockNumber blkno = BufferGetBlockNumber(buffer);

		ItemPointerSet(&tid, blkno, xlrec->offnum);
		page = (Page) BufferGetPage(buffer);

		brinSetHeapBlockItemptr(buffer, xlrec->pagesPerRange, xlrec->heapBlk,
								tid);
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* XXX no FSM updates here ... */
}

/*
 * replay a BRIN index insertion
 */
static void
brin_xlog_insert(XLogReaderState *record)
{
	xl_brin_insert *xlrec = (xl_brin_insert *) XLogRecGetData(record);

	brin_xlog_insert_update(record, xlrec);
}

/*
 * replay a BRIN index update
 */
static void
brin_xlog_update(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_brin_update *xlrec = (xl_brin_update *) XLogRecGetData(record);
	Buffer		buffer;
	XLogRedoAction action;

	/* First remove the old tuple */
	action = XLogReadBufferForRedo(record, 2, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		Page		page;
		OffsetNumber offnum;

		page = (Page) BufferGetPage(buffer);

		offnum = xlrec->oldOffnum;
		if (PageGetMaxOffsetNumber(page) + 1 < offnum)
			elog(PANIC, "brin_xlog_update: invalid max offset number");

		PageIndexDeleteNoCompact(page, &offnum, 1);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	/* Then insert the new tuple and update revmap, like in an insertion. */
	brin_xlog_insert_update(record, &xlrec->insert);

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * Update a tuple on a single page.
 */
static void
brin_xlog_samepage_update(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_brin_samepage_update *xlrec;
	Buffer		buffer;
	XLogRedoAction action;

	xlrec = (xl_brin_samepage_update *) XLogRecGetData(record);
	action = XLogReadBufferForRedo(record, 0, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		Size		tuplen;
		BrinTuple  *mmtuple;
		Page		page;
		OffsetNumber offnum;

		mmtuple = (BrinTuple *) XLogRecGetBlockData(record, 0, &tuplen);

		page = (Page) BufferGetPage(buffer);

		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(page) + 1 < offnum)
			elog(PANIC, "brin_xlog_samepage_update: invalid max offset number");

		PageIndexDeleteNoCompact(page, &offnum, 1);
		offnum = PageAddItem(page, (Item) mmtuple, tuplen, offnum, true, false);
		if (offnum == InvalidOffsetNumber)
			elog(PANIC, "brin_xlog_samepage_update: failed to add tuple");

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* XXX no FSM updates here ... */
}

/*
 * Replay a revmap page extension
 */
static void
brin_xlog_revmap_extend(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_brin_revmap_extend *xlrec;
	Buffer		metabuf;
	Buffer		buf;
	Page		page;
	BlockNumber targetBlk;
	XLogRedoAction action;

	xlrec = (xl_brin_revmap_extend *) XLogRecGetData(record);

	XLogRecGetBlockTag(record, 1, NULL, NULL, &targetBlk);
	Assert(xlrec->targetBlk == targetBlk);

	/* Update the metapage */
	action = XLogReadBufferForRedo(record, 0, &metabuf);
	if (action == BLK_NEEDS_REDO)
	{
		Page		metapg;
		BrinMetaPageData *metadata;

		metapg = BufferGetPage(metabuf);
		metadata = (BrinMetaPageData *) PageGetContents(metapg);

		Assert(metadata->lastRevmapPage == xlrec->targetBlk - 1);
		metadata->lastRevmapPage = xlrec->targetBlk;

		PageSetLSN(metapg, lsn);
		MarkBufferDirty(metabuf);
	}

	/*
	 * Re-init the target block as a revmap page.  There's never a full- page
	 * image here.
	 */

	buf = XLogInitBufferForRedo(record, 1);
	page = (Page) BufferGetPage(buf);
	brin_page_init(page, BRIN_PAGETYPE_REVMAP);

	PageSetLSN(page, lsn);
	MarkBufferDirty(buf);

	UnlockReleaseBuffer(buf);
	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

void
brin_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_BRIN_OPMASK)
	{
		case XLOG_BRIN_CREATE_INDEX:
			brin_xlog_createidx(record);
			break;
		case XLOG_BRIN_INSERT:
			brin_xlog_insert(record);
			break;
		case XLOG_BRIN_UPDATE:
			brin_xlog_update(record);
			break;
		case XLOG_BRIN_SAMEPAGE_UPDATE:
			brin_xlog_samepage_update(record);
			break;
		case XLOG_BRIN_REVMAP_EXTEND:
			brin_xlog_revmap_extend(record);
			break;
		default:
			elog(PANIC, "brin_redo: unknown op code %u", info);
	}
}
