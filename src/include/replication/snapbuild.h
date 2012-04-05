/*-------------------------------------------------------------------------
 *
 * snapbuild.h
 *	  Exports from replication/logical/snapbuild.c.
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * src/include/replication/snapbuild.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPBUILD_H
#define SNAPBUILD_H

#include "replication/reorderbuffer.h"

#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "access/htup.h"

typedef enum
{
	/*
	 * Initial state, we can't do much yet.
	 */
	SNAPBUILD_START,

	/*
	 * We have collected enough information to decode tuples in transactions
	 * that started after this.
	 *
	 * Once we reached this we start to collect changes. We cannot apply them
	 * yet because the might be based on transactions that were still running
	 * when we reached them yet.
	 */
	SNAPBUILD_FULL_SNAPSHOT,

	/*
	 * Found a point after hitting built_full_snapshot where all transactions
	 * that were running at that point finished. Till we reach that we hold
	 * off calling any commit callbacks.
	 */
	SNAPBUILD_CONSISTENT
}	SnapBuildState;

typedef enum
{
	SNAPBUILD_SKIP,
	SNAPBUILD_DECODE
}	SnapBuildAction;

typedef struct Snapstate
{
	/* how far are we along building our first full snapshot */
	SnapBuildState state;

	/* private memory context used to allocate memory for this module. */
	MemoryContext context;

	/* all transactions < than this have committed/aborted */
	TransactionId xmin;

	/* all transactions >= than this are uncommitted */
	TransactionId xmax;

	/*
	 * Don't replay commits from an LSN <= this LSN. This can be set
	 * externally but it will also be advanced (never retreat) from within
	 * snapbuild.c.
	 */
	XLogRecPtr	transactions_after;

	/*
	 * Don't start decoding WAL until the "xl_running_xacts" information
	 * indicates there are no running xids with a xid smaller than this.
	 */
	TransactionId initial_xmin_horizon;

	/*
	 * Snapshot thats valid to see all currently committed transactions that
	 * see catalog modifications.
	 */
	Snapshot	snapshot;

	/*
	 * LSN of the last location we are sure a snapshot has been serialized to.
	 */
	XLogRecPtr	last_serialized_snapshot;

	/* variable length data */

	/*
	 * Information about initially running transactions
	 *
	 * When we start building a snapshot there already may be transactions in
	 * progress.  Those are stored in running.xip.  We don't have enough
	 * information about those to decode their contents, so until they are
	 * finished (xcnt=0) we cannot switch to a CONSISTENT state.
	 */
	struct
	{
		/*
		 * As long as running.xcnt all XIDs < running.xmin and > running.xmax
		 * have to be checked whether they still are running.
		 */
		TransactionId xmin;
		TransactionId xmax;

		size_t		xcnt;	/* number of used xip entries */
		size_t		xcnt_space;	/* allocated size of xip */
		TransactionId *xip;	/* running xacts array, xidComparator-sorted */
	} running;

	/*
	 * Array of transactions which could have catalog changes that committed
	 * between xmin and xmax
	 */
	struct
	{
		/* number of committed transactions */
		size_t		xcnt;

		/* available space for committed transactions */
		size_t		xcnt_space;

		/*
		 * Until we reach a CONSISTENT state, we record commits of all
		 * transactions, not just the catalog changing ones. Record when that
		 * changes so we know we cannot export a snapshot safely anymore.
		 */
		bool		includes_all_transactions;

		/*
		 * Array of committed transactions that have modified the catalog.
		 *
		 * As this array is frequently modified we do *not* keep it in
		 * xidComparator order. Instead we sort the array when building &
		 * distributing a snapshot.
		 *
		 * XXX: That doesn't seem to be good reasoning anymore. Everytime we
		 * add something here after becoming consistent will also require
		 * distributing a snapshot. Storing them sorted would potentially make
		 * it easier to purge as well (but more complicated wrt wraparound?).
		 */
		TransactionId *xip;
	}			committed;

}	Snapstate;

extern Snapstate *AllocateSnapshotBuilder(ReorderBuffer * cache);

extern void FreeSnapshotBuilder(Snapstate * cache);

struct XLogRecordBuffer;

extern SnapBuildAction SnapBuildDecodeCallback(ReorderBuffer * cache, Snapstate * snapstate, struct XLogRecordBuffer * buf);

extern Relation LookupRelationByRelFileNode(RelFileNode *r);

extern bool SnapBuildHasCatalogChanges(Snapstate * snapstate, TransactionId xid,
						   RelFileNode *relfilenode);

extern void SnapBuildSnapDecRefcount(Snapshot snap);

extern const char *SnapBuildExportSnapshot(Snapstate * snapstate);
extern void SnapBuildClearExportedSnapshot(void);

#endif   /* SNAPBUILD_H */
