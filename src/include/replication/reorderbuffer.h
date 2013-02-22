/*
 * reorderbuffer.h
 *
 * PostgreSQL logical replay "cache" management
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * src/include/replication/reorderbuffer.h
 */
#ifndef REORDERBUFFER_H
#define REORDERBUFFER_H

#include "access/htup_details.h"
#include "utils/hsearch.h"
#include "utils/rel.h"

#include "lib/ilist.h"

#include "storage/sinval.h"

#include "utils/snapshot.h"


typedef struct ReorderBuffer ReorderBuffer;

/* types of the change passed to a 'change' callback */
enum ReorderBufferChangeType
{
	REORDER_BUFFER_CHANGE_INSERT,
	REORDER_BUFFER_CHANGE_UPDATE,
	REORDER_BUFFER_CHANGE_DELETE
};

/* an individual tuple, stored in one chunk of memory */
typedef struct ReorderBufferTupleBuf
{
	/* position in preallocated list */
	slist_node	node;

	/* tuple, stored sequentially */
	HeapTupleData tuple;
	HeapTupleHeaderData header;
	char		data[MaxHeapTupleSize];
}	ReorderBufferTupleBuf;

/*
 * a single 'change', can be an insert (with one tuple), an update (old, new),
 * or a delete (old).
 *
 * The same struct is also used internally for other purposes but that should
 * never be visible outside reorderbuffer.c.
 */
typedef struct ReorderBufferChange
{
	XLogRecPtr	lsn;

	/* type of change */
	union
	{
		enum ReorderBufferChangeType action;
		/* do not leak internal enum values to the outside */
		int			action_internal;
	};

	RepNodeId origin_id;

	/*
	 * Context data for the change, which part of the union is valid depends
	 * on action/action_internal.
	 */
	union
	{
		/* old, new tuples when action == *_INSERT|UPDATE|DELETE */
		struct
		{
			/* relation that has been changed */
			RelFileNode relnode;
			/* valid for DELETE || UPDATE */
			ReorderBufferTupleBuf *oldtuple;
			/* valid for INSERT || UPDATE */
			ReorderBufferTupleBuf *newtuple;
		};

		/* new snapshot */
		Snapshot	snapshot;

		/* new command id for existing snapshot in a catalog changing tx */
		CommandId	command_id;

		/* new cid mapping for catalog changing transaction */
		struct
		{
			RelFileNode node;
			ItemPointerData tid;
			CommandId	cmin;
			CommandId	cmax;
			CommandId	combocid;
		}			tuplecid;
	};

	/*
	 * While in use this is how a change is linked into a transactions,
	 * otherwise it's the preallocated list.
	 */
	dlist_node	node;
}	ReorderBufferChange;

typedef struct ReorderBufferTXN
{
	/*
	 * The transactions transaction id, can be a toplevel or sub xid.
	 */
	TransactionId xid;

	/*
	 * LSN of the first wal record with knowledge about this xid.
	 */
	XLogRecPtr	lsn;

	/*
	 * LSN of the commit record
	 */
	XLogRecPtr	last_lsn;

	/*
	 * LSN of the last lsn at which snapshot information reside, so we can
	 * restart decoding from there and fully recover this transaction from
	 * WAL.
	 */
	XLogRecPtr	restart_decoding_lsn;

	/* origin of the change that caused this transaction */
	RepNodeId origin_id;

	/* did the TX have catalog changes */
	bool		does_timetravel;

	/*
	 * Base snapshot or NULL.
	 */
	Snapshot	base_snapshot;

	/*
	 * Do we know this is a subxact?
	 */
	bool		is_known_as_subxact;

	/*
	 * How many ReorderBufferChange's do we have in this txn.
	 *
	 * Changes in subtransactions are *not* included but tracked separately.
	 */
	Size		nentries;

	/*
	 * How many of the above entries are stored in memory in contrast to being
	 * spilled to disk.
	 */
	Size		nentries_mem;

	/*
	 * List of ReorderBufferChange structs, including new Snapshots and new
	 * CommandIds
	 */
	dlist_head	changes;

	/*
	 * List of (relation, ctid) => (cmin, cmax) mappings for catalog tuples.
	 * Those are always assigned to the toplevel transaction. (Keep track of
	 * #entries to create a hash of the right size)
	 */
	dlist_head	tuplecids;
	size_t		ntuplecids;

	/*
	 * On-demand built hash for looking up the above values.
	 */
	HTAB	   *tuplecid_hash;

	/*
	 * Hash containing (potentially partial) toast entries. NULL if no toast
	 * tuples have been found for the current change.
	 */
	HTAB	   *toast_hash;

	/*
	 * non-hierarchical list of subtransactions that are *not* aborted. Only
	 * used in toplevel transactions.
	 */
	dlist_head	subtxns;
	size_t		nsubtxns;

	/*
	 * Position in one of three lists: * list of subtransactions if we are
	 * *known* to be subxact * list of toplevel xacts (can be a as-yet unknown
	 * subxact) * list of preallocated ReorderBufferTXNs
	 */
	dlist_node	node;

	/*
	 * Stored cache invalidations. This is not a linked list because we get
	 * all the invalidations at once.
	 */
	SharedInvalidationMessage *invalidations;
	size_t		ninvalidations;

}	ReorderBufferTXN;


/* change callback signature */
typedef void (*ReorderBufferApplyChangeCB) (
														ReorderBuffer * cache,
													  ReorderBufferTXN * txn,
														Relation relation,
											   ReorderBufferChange * change);

/* begin callback signature */
typedef void (*ReorderBufferBeginCB) (
												  ReorderBuffer * cache,
												  ReorderBufferTXN * txn);

/* commit callback signature */
typedef void (*ReorderBufferCommitCB) (
												   ReorderBuffer * cache,
												   ReorderBufferTXN * txn,
												   XLogRecPtr commit_lsn);

struct ReorderBuffer
{
	/*
	 * xid => ReorderBufferTXN lookup table
	 */
	HTAB	   *by_txn;

	/*
	 * Transactions that could be a toplevel xact, ordered by LSN of the first
	 * record bearing that xid..
	 */
	dlist_head	toplevel_by_lsn;

	/*
	 * one-entry sized cache for by_txn. Very frequently the same txn gets
	 * looked up over and over again.
	 */
	TransactionId by_txn_last_xid;
	ReorderBufferTXN *by_txn_last_txn;

	/*
	 * Callacks to be called when a transactions commits.
	 */
	ReorderBufferBeginCB begin;
	ReorderBufferApplyChangeCB apply_change;
	ReorderBufferCommitCB commit;

	/*
	 * Pointer that will be passed untouched to the callbacks.
	 */
	void	   *private_data;

	/*
	 * Private memory context.
	 */
	MemoryContext context;

	/*
	 * Data structure slab cache.
	 *
	 * We allocate/deallocate some structures very frequently, to avoid bigger
	 * overhead we cache some unused ones here.
	 *
	 * The maximum number of cached entries is controlled by const variables
	 * ontop of reorderbuffer.c
	 */

	/* cached ReorderBufferTXNs */
	dlist_head	cached_transactions;
	Size		nr_cached_transactions;

	/* cached ReorderBufferChanges */
	dlist_head	cached_changes;
	Size		nr_cached_changes;

	/* cached ReorderBufferTupleBufs */
	slist_head	cached_tuplebufs;
	Size		nr_cached_tuplebufs;

	XLogRecPtr	current_restart_decoding_lsn;

	/* buffer for disk<->memory conversions */
	char	   *outbuf;
	Size		outbufsize;
};


ReorderBuffer *ReorderBufferAllocate(void);
void		ReorderBufferFree(ReorderBuffer *);

ReorderBufferTupleBuf *ReorderBufferGetTupleBuf(ReorderBuffer *);
void		ReorderBufferReturnTupleBuf(ReorderBuffer *, ReorderBufferTupleBuf * tuple);
ReorderBufferChange *ReorderBufferGetChange(ReorderBuffer *);
void		ReorderBufferReturnChange(ReorderBuffer *, ReorderBufferChange *);

void		ReorderBufferAddChange(ReorderBuffer *, TransactionId, XLogRecPtr lsn, ReorderBufferChange *);
void		ReorderBufferCommit(ReorderBuffer *, TransactionId, XLogRecPtr lsn, RepNodeId origin_id);
void		ReorderBufferAssignChild(ReorderBuffer *, TransactionId, TransactionId, XLogRecPtr lsn);
void		ReorderBufferCommitChild(ReorderBuffer *, TransactionId, TransactionId, XLogRecPtr lsn);
void		ReorderBufferAbort(ReorderBuffer *, TransactionId, XLogRecPtr lsn);

void		ReorderBufferSetBaseSnapshot(ReorderBuffer *, TransactionId, XLogRecPtr lsn, struct SnapshotData *snap);
void		ReorderBufferAddSnapshot(ReorderBuffer *, TransactionId, XLogRecPtr lsn, struct SnapshotData *snap);
void ReorderBufferAddNewCommandId(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
							 CommandId cid);
void ReorderBufferAddNewTupleCids(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
							 RelFileNode node, ItemPointerData pt,
						 CommandId cmin, CommandId cmax, CommandId combocid);
void ReorderBufferAddInvalidations(ReorderBuffer *, TransactionId, XLogRecPtr lsn,
							  Size nmsgs, SharedInvalidationMessage *msgs);
bool		ReorderBufferIsXidKnown(ReorderBuffer *, TransactionId xid);
void		ReorderBufferXidSetTimetravel(ReorderBuffer *, TransactionId xid, XLogRecPtr lsn);
bool		ReorderBufferXidDoesTimetravel(ReorderBuffer *, TransactionId xid);
bool		ReorderBufferXidHasBaseSnapshot(ReorderBuffer *, TransactionId xid);

ReorderBufferTXN *ReorderBufferGetOldestTXN(ReorderBuffer *);

void		ReorderBufferSetRestartPoint(ReorderBuffer *, XLogRecPtr ptr);

void		ReorderBufferStartup(void);

#endif
