/*-------------------------------------------------------------------------
 *
 * tqual.h
 *	  POSTGRES "time qualification" definitions, ie, tuple visibility rules.
 *
 *	  Should be moved/renamed...	- vadim 07/28/98
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tqual.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TQUAL_H
#define TQUAL_H

#include "utils/snapshot.h"


/* Static variables representing various special snapshot semantics */
extern PGDLLIMPORT SnapshotData SnapshotNowData;
extern PGDLLIMPORT SnapshotData SnapshotSelfData;
extern PGDLLIMPORT SnapshotData SnapshotAnyData;
extern PGDLLIMPORT SnapshotData SnapshotToastData;

#define SnapshotNow			(&SnapshotNowData)
#define SnapshotSelf		(&SnapshotSelfData)
#define SnapshotAny			(&SnapshotAnyData)
#define SnapshotToast		(&SnapshotToastData)

/*
 * We don't provide a static SnapshotDirty variable because it would be
 * non-reentrant.  Instead, users of that snapshot type should declare a
 * local variable of type SnapshotData, and initialize it with this macro.
 */
#define InitDirtySnapshot(snapshotdata)  \
	((snapshotdata).satisfies = HeapTupleSatisfiesDirty)

/* This macro encodes the knowledge of which snapshots are MVCC-safe */
#define IsMVCCSnapshot(snapshot)  \
	((snapshot)->satisfies == HeapTupleSatisfiesMVCC || \
	 (snapshot)->satisfies == HeapTupleSatisfiesMVCCDuringDecoding)

/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid.
 *	Beware of multiple evaluations of snapshot argument.
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
#define HeapTupleSatisfiesVisibility(tuple, snapshot, buffer) \
	((*(snapshot)->satisfies) (tuple, snapshot, buffer))

/* Result codes for HeapTupleSatisfiesVacuum */
typedef enum
{
	HEAPTUPLE_DEAD,				/* tuple is dead and deletable */
	HEAPTUPLE_LIVE,				/* tuple is live (committed, no deleter) */
	HEAPTUPLE_RECENTLY_DEAD,	/* tuple is dead, but not deletable yet */
	HEAPTUPLE_INSERT_IN_PROGRESS,		/* inserting xact is still in progress */
	HEAPTUPLE_DELETE_IN_PROGRESS	/* deleting xact is still in progress */
} HTSV_Result;

/* These are the "satisfies" test routines for the various snapshot types */
extern bool HeapTupleSatisfiesMVCC(HeapTuple htup,
					   Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesNow(HeapTuple htup,
					  Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesSelf(HeapTuple htup,
					   Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesAny(HeapTuple htup,
					  Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesToast(HeapTuple htup,
						Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesDirty(HeapTuple htup,
						Snapshot snapshot, Buffer buffer);

/* Special "satisfies" routines with different APIs */
extern HTSU_Result HeapTupleSatisfiesUpdate(HeapTuple htup,
						 CommandId curcid, Buffer buffer);
extern HTSV_Result HeapTupleSatisfiesVacuum(HeapTuple htup,
						 TransactionId OldestXmin, Buffer buffer);
extern bool HeapTupleIsSurelyDead(HeapTuple htup,
					  TransactionId OldestXmin);

extern void HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
					 uint16 infomask, TransactionId xid);
extern bool HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple);

/*
 * Special "satisfies" routines used during decoding xlog from a different
 * point of lsn. Also used for timetravel SnapshotNow's.
 */
extern bool HeapTupleSatisfiesMVCCDuringDecoding(HeapTuple htup,
                                                 Snapshot snapshot, Buffer buffer);

/*
 * install the 'snapshot_now' snapshot as a timetravelling snapshot replacing
 * the normal SnapshotNow behaviour. This snapshot needs to have been created
 * by snapbuild.c otherwise you will see crashes!
 *
 * FIXME: We need something resembling the real SnapshotNow to handle things
 * like enum lookups from indices correctly.
 */
extern void SetupDecodingSnapshots(Snapshot snapshot_now, HTAB *tuplecids);
extern void RevertFromDecodingSnapshots(void);

/*
 * resolve combocids and overwritten cmin values
 *
 * To avoid leaking to much knowledge about the reorderbuffer this is
 * implemented in reorderbuffer.c not tqual.c.
 */
extern bool ResolveCminCmaxDuringDecoding(HTAB *tuplecid_data, HeapTuple htup,
										  Buffer buffer,
										  CommandId *cmin, CommandId *cmax);

#endif   /* TQUAL_H */
