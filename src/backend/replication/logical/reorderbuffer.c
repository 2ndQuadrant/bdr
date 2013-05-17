/*-------------------------------------------------------------------------
 *
 * reorderbuffer.c
 *
 * PostgreSQL logical replay "cache" management
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/reorderbuffer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"

#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"

#include "common/relpath.h"

#include "lib/binaryheap.h"

#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "replication/logical.h"

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/sinval.h"

#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/tqual.h"
#include "utils/syscache.h"

/*
 * For efficiency and simplicity reasons we want to keep Snapshots, CommandIds
 * and ComboCids in the same list with the user visible INSERT/UPDATE/DELETE
 * changes. We don't want to leak those internal values to external users
 * though (they would just use switch()...default:) because that would make it
 * harder to add to new user visible values.
 *
 * This needs to be synchronized with ReorderBufferChangeType! Adjust the
 * StaticAssertExpr's in ReorderBufferAllocate if you add anything!
 */
typedef enum
{
	REORDER_BUFFER_CHANGE_INTERNAL_INSERT,
	REORDER_BUFFER_CHANGE_INTERNAL_UPDATE,
	REORDER_BUFFER_CHANGE_INTERNAL_DELETE,
	REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT,
	REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID,
	REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID
}	ReorderBufferChangeTypeInternal;


/* entry for a hash table we use to map from xid to our transaction state */
typedef struct ReorderBufferTXNByIdEnt
{
	TransactionId xid;
	ReorderBufferTXN *txn;
}	ReorderBufferTXNByIdEnt;


/* data structures for (relfilenode, ctid) => (cmin, cmax) mapping */
typedef struct ReorderBufferTupleCidKey
{
	RelFileNode relnode;
	ItemPointerData tid;
}	ReorderBufferTupleCidKey;

typedef struct ReorderBufferTupleCidEnt
{
	ReorderBufferTupleCidKey key;
	CommandId	cmin;
	CommandId	cmax;
	CommandId	combocid;		/* just for debugging */
}	ReorderBufferTupleCidEnt;


/* k-way in-order change iteration support structures */
typedef struct ReorderBufferIterTXNEntry
{
	XLogRecPtr	lsn;
	ReorderBufferChange *change;
	ReorderBufferTXN *txn;
	int			fd;
	XLogSegNo	segno;
}	ReorderBufferIterTXNEntry;

typedef struct ReorderBufferIterTXNState
{
	binaryheap *heap;
	Size		nr_txns;
	dlist_head	old_change;
	ReorderBufferIterTXNEntry entries[FLEXIBLE_ARRAY_MEMBER];
}	ReorderBufferIterTXNState;


/* toast datastructures */
typedef struct ReorderBufferToastEnt
{
	Oid			chunk_id;		/* toast_table.chunk_id */
	int32		last_chunk_seq; /* toast_table.chunk_seq of the last chunk we
								 * have seen */
	Size		num_chunks;		/* number of chunks we've already seen */
	Size		size;			/* combined size of chunks seen */
	dlist_head	chunks;			/* linked list of chunks */
	struct varlena *reconstructed;		/* reconstructed varlena now pointed
										 * to in main tup */
}	ReorderBufferToastEnt;


/* number of changes kept in memory, per transaction */
const Size	max_memtries = 4096;

/* Size of the slab caches used for frequently allocated objects */
const Size	max_cached_changes = 4096 * 2;
const Size	max_cached_tuplebufs = 1024;		/* ~8MB */
const Size	max_cached_transactions = 512;


/* ---------------------------------------
 * primary reorderbuffer support routines
 * ---------------------------------------
 */
static ReorderBufferTXN *ReorderBufferGetTXN(ReorderBuffer * buffer);
static void ReorderBufferReturnTXN(ReorderBuffer * buffer, ReorderBufferTXN * txn);
static ReorderBufferTXN *ReorderBufferTXNByXid(ReorderBuffer * buffer,
					  TransactionId xid, bool create, bool *is_new,
					  XLogRecPtr lsn, bool create_as_top);

static void AssertTXNLsnOrder(ReorderBuffer * buffer);

/* ---------------------------------------
 * support functions for lsn-order iterating over the ->changes of a
 * transaction and its subtransactions
 *
 * used for iteration over the k-way heap merge of a transaction and its
 * subtransactions
 * ---------------------------------------
 */
static ReorderBufferIterTXNState *ReorderBufferIterTXNInit(ReorderBuffer * buffer, ReorderBufferTXN * txn);
static ReorderBufferChange *
			ReorderBufferIterTXNNext(ReorderBuffer * buffer, ReorderBufferIterTXNState * state);
static void ReorderBufferIterTXNFinish(ReorderBuffer * buffer,
						   ReorderBufferIterTXNState * state);
static void ReorderBufferExecuteInvalidations(ReorderBuffer * buffer, ReorderBufferTXN * txn);

/*
 * ---------------------------------------
 * Disk serialization support functions
 * ---------------------------------------
 */
static void ReorderBufferCheckSerializeTXN(ReorderBuffer * buffer, ReorderBufferTXN * txn);
static void ReorderBufferSerializeTXN(ReorderBuffer * buffer, ReorderBufferTXN * txn);
static void ReorderBufferSerializeChange(ReorderBuffer * buffer, ReorderBufferTXN * txn,
							 int fd, ReorderBufferChange * change);
static Size ReorderBufferRestoreChanges(ReorderBuffer * buffer, ReorderBufferTXN * txn,
							int *fd, XLogSegNo * segno);
static void ReorderBufferRestoreChange(ReorderBuffer * buffer, ReorderBufferTXN * txn,
						   char *change);
static void ReorderBufferRestoreCleanup(ReorderBuffer * buffer, ReorderBufferTXN * txn);

static void ReorderBufferFreeSnap(ReorderBuffer * buffer, Snapshot snap);
static Snapshot ReorderBufferCopySnap(ReorderBuffer * buffer, Snapshot orig_snap,
					  ReorderBufferTXN * txn, CommandId cid);

/* ---------------------------------------
 * toast reassembly support
 * ---------------------------------------
 */
static void ReorderBufferToastInitHash(ReorderBuffer * buffer, ReorderBufferTXN * txn);
static void ReorderBufferToastReset(ReorderBuffer * buffer, ReorderBufferTXN * txn);
static void ReorderBufferToastReplace(ReorderBuffer * buffer, ReorderBufferTXN * txn,
						  Relation relation, ReorderBufferChange * change);
static void ReorderBufferToastAppendChunk(ReorderBuffer * buffer, ReorderBufferTXN * txn,
							Relation relation, ReorderBufferChange * change);


/*
 * Allocate a new ReorderBuffer
 */
ReorderBuffer *
ReorderBufferAllocate(void)
{
	ReorderBuffer *buffer;
	HASHCTL		hash_ctl;
	MemoryContext new_ctx;

	StaticAssertExpr((int) REORDER_BUFFER_CHANGE_INTERNAL_INSERT == (int) REORDER_BUFFER_CHANGE_INSERT, "out of sync enums");
	StaticAssertExpr((int) REORDER_BUFFER_CHANGE_INTERNAL_UPDATE == (int) REORDER_BUFFER_CHANGE_UPDATE, "out of sync enums");
	StaticAssertExpr((int) REORDER_BUFFER_CHANGE_INTERNAL_DELETE == (int) REORDER_BUFFER_CHANGE_DELETE, "out of sync enums");

	new_ctx = AllocSetContextCreate(TopMemoryContext,
									"ReorderBuffer",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);

	buffer = (ReorderBuffer *) MemoryContextAlloc(new_ctx, sizeof(ReorderBuffer));

	memset(&hash_ctl, 0, sizeof(hash_ctl));

	buffer->context = new_ctx;

	hash_ctl.keysize = sizeof(TransactionId);
	hash_ctl.entrysize = sizeof(ReorderBufferTXNByIdEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = buffer->context;

	buffer->by_txn = hash_create("ReorderBufferByXid", 1000, &hash_ctl,
								 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	buffer->by_txn_last_xid = InvalidTransactionId;
	buffer->by_txn_last_txn = NULL;

	buffer->nr_cached_transactions = 0;
	buffer->nr_cached_changes = 0;
	buffer->nr_cached_tuplebufs = 0;

	buffer->outbuf = NULL;
	buffer->outbufsize = 0;

	dlist_init(&buffer->toplevel_by_lsn);
	dlist_init(&buffer->cached_transactions);
	dlist_init(&buffer->cached_changes);
	slist_init(&buffer->cached_tuplebufs);

	return buffer;
}

/*
 * Free a ReorderBuffer
 */
void
ReorderBufferFree(ReorderBuffer * buffer)
{
	/* FIXME: check for in-progress transactions */
	/* FIXME: clean up cached transaction */
	/* FIXME: clean up cached changes */
	/* FIXME: clean up cached tuplebufs */
	if (buffer->outbufsize > 0)
		pfree(buffer->outbuf);

	hash_destroy(buffer->by_txn);
	pfree(buffer);
}

/*
 * Get a unused, possibly preallocated, ReorderBufferTXN.
 */
static ReorderBufferTXN *
ReorderBufferGetTXN(ReorderBuffer * buffer)
{
	ReorderBufferTXN *txn;

	if (buffer->nr_cached_transactions > 0)
	{
		buffer->nr_cached_transactions--;
		txn = (ReorderBufferTXN *)
			dlist_container(ReorderBufferTXN, node,
						  dlist_pop_head_node(&buffer->cached_transactions));
	}
	else
	{
		txn = (ReorderBufferTXN *)
			MemoryContextAlloc(buffer->context, sizeof(ReorderBufferTXN));
	}

	memset(txn, 0, sizeof(ReorderBufferTXN));

	dlist_init(&txn->changes);
	dlist_init(&txn->tuplecids);
	dlist_init(&txn->subtxns);

	return txn;
}

/*
 * Free an ReorderBufferTXN. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnTXN(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	/* clean the lookup cache if we were cached (quite likely) */
	if (buffer->by_txn_last_xid == txn->xid)
	{
		buffer->by_txn_last_xid = InvalidTransactionId;
		buffer->by_txn_last_txn = NULL;
	}

	if (txn->tuplecid_hash != NULL)
	{
		hash_destroy(txn->tuplecid_hash);
		txn->tuplecid_hash = NULL;
	}

	if (txn->invalidations)
	{
		pfree(txn->invalidations);
		txn->invalidations = NULL;
	}

	if (buffer->nr_cached_transactions < max_cached_transactions)
	{
		buffer->nr_cached_transactions++;
		dlist_push_head(&buffer->cached_transactions, &txn->node);
	}
	else
	{
		pfree(txn);
	}
}

/*
 * Get a unused, possibly preallocated, ReorderBufferChange.
 */
ReorderBufferChange *
ReorderBufferGetChange(ReorderBuffer * buffer)
{
	ReorderBufferChange *change;

	if (buffer->nr_cached_changes)
	{
		buffer->nr_cached_changes--;
		change = (ReorderBufferChange *)
			dlist_container(ReorderBufferChange, node,
							dlist_pop_head_node(&buffer->cached_changes));
	}
	else
	{
		change = (ReorderBufferChange *)
			MemoryContextAlloc(buffer->context, sizeof(ReorderBufferChange));
	}

	memset(change, 0, sizeof(ReorderBufferChange));
	return change;
}

/*
 * Free an ReorderBufferChange. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnChange(ReorderBuffer * buffer, ReorderBufferChange * change)
{
	switch ((ReorderBufferChangeTypeInternal) change->action_internal)
	{
		case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
		case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
		case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
			if (change->newtuple)
			{
				ReorderBufferReturnTupleBuf(buffer, change->newtuple);
				change->newtuple = NULL;
			}

			if (change->oldtuple)
			{
				ReorderBufferReturnTupleBuf(buffer, change->oldtuple);
				change->oldtuple = NULL;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
			if (change->snapshot)
			{
				ReorderBufferFreeSnap(buffer, change->snapshot);
				change->snapshot = NULL;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
			break;
	}

	if (buffer->nr_cached_changes < max_cached_changes)
	{
		buffer->nr_cached_changes++;
		dlist_push_head(&buffer->cached_changes, &change->node);
	}
	else
	{
		pfree(change);
	}
}


/*
 * Get a unused, possibly preallocated, ReorderBufferTupleBuf
 */
ReorderBufferTupleBuf *
ReorderBufferGetTupleBuf(ReorderBuffer * buffer)
{
	ReorderBufferTupleBuf *tuple;

	if (buffer->nr_cached_tuplebufs)
	{
		buffer->nr_cached_tuplebufs--;
		tuple = slist_container(ReorderBufferTupleBuf, node,
							 slist_pop_head_node(&buffer->cached_tuplebufs));
#ifdef USE_ASSERT_CHECKING
		memset(tuple, 0xdeadbeef, sizeof(ReorderBufferTupleBuf));
#endif
	}
	else
	{
		tuple = (ReorderBufferTupleBuf *)
			MemoryContextAlloc(buffer->context, sizeof(ReorderBufferTupleBuf));
	}

	return tuple;
}

/*
 * Free an ReorderBufferTupleBuf. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnTupleBuf(ReorderBuffer * buffer, ReorderBufferTupleBuf * tuple)
{
	if (buffer->nr_cached_tuplebufs < max_cached_tuplebufs)
	{
		buffer->nr_cached_tuplebufs++;
		slist_push_head(&buffer->cached_tuplebufs, &tuple->node);
	}
	else
	{
		pfree(tuple);
	}
}

/*
 * Return the ReorderBufferTXN from the given buffer, specified by Xid.
 * If create is true, and a transaction doesn't already exist, create it
 * (with the given LSN, and as top transaction if that's specified);
 * when this happens, is_new is set to true.
 */
static ReorderBufferTXN *
ReorderBufferTXNByXid(ReorderBuffer * buffer, TransactionId xid, bool create,
					  bool *is_new, XLogRecPtr lsn, bool create_as_top)
{
	ReorderBufferTXN *txn;
	ReorderBufferTXNByIdEnt *ent;
	bool		found;

	Assert(!create || lsn != InvalidXLogRecPtr);

	/*
	 * Check the one-entry lookup cache first
	 */
	if (TransactionIdIsValid(buffer->by_txn_last_xid) &&
		buffer->by_txn_last_xid == xid)
	{
		txn = buffer->by_txn_last_txn;

		if (txn != NULL)
		{
			/* found it, and it's valid */
			if (is_new)
				*is_new = false;
			return txn;
		}

		/*
		 * cached as non-existant, and asked not to create? Then nothing else
		 * to do.
		 */
		if (!create)
			return NULL;
		/* otherwise fall through to create it */
	}

	/*
	 * If the cache wasn't hit or it yielded an "does-not-exist" and we want
	 * to create an entry.
	 */

	/* search the lookup table */
	ent = (ReorderBufferTXNByIdEnt *)
		hash_search(buffer->by_txn,
					(void *) &xid,
					create ? HASH_ENTER : HASH_FIND,
					&found);
	if (found)
		txn = ent->txn;
	else if (create)
	{
		/* initialize the new entry, if creation was requested */
		Assert(ent != NULL);

		ent->txn = ReorderBufferGetTXN(buffer);
		ent->txn->xid = xid;
		txn = ent->txn;
		txn->lsn = lsn;
		txn->restart_decoding_lsn = buffer->current_restart_decoding_lsn;

		if (create_as_top)
		{
			dlist_push_tail(&buffer->toplevel_by_lsn, &txn->node);
			AssertTXNLsnOrder(buffer);
		}
	}
	else
		txn = NULL;				/* not found and not asked to create */

	/* update cache */
	buffer->by_txn_last_xid = xid;
	buffer->by_txn_last_txn = txn;

	if (is_new)
		*is_new = !found;

	Assert(!create || !!txn);
	return txn;
}

/*
 * Queue a change into a transaction so it can be replayed upon commit.
 */
void
ReorderBufferAddChange(ReorderBuffer * buffer, TransactionId xid, XLogRecPtr lsn,
					   ReorderBufferChange * change)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, true, NULL, lsn, true);

	change->lsn = lsn;
	Assert(InvalidXLogRecPtr != lsn);
	dlist_push_tail(&txn->changes, &change->node);
	txn->nentries++;
	txn->nentries_mem++;

	ReorderBufferCheckSerializeTXN(buffer, txn);
}

static void
AssertTXNLsnOrder(ReorderBuffer * buffer)
{
#ifdef USE_ASSERT_CHECKING
	dlist_iter	iter;
	XLogRecPtr	last_lsn = InvalidXLogRecPtr;

	dlist_foreach(iter, &buffer->toplevel_by_lsn)
	{
		ReorderBufferTXN *cur_txn;

		cur_txn = dlist_container(ReorderBufferTXN, node, iter.cur);
		Assert(cur_txn->lsn != InvalidXLogRecPtr);

		if (cur_txn->last_lsn != InvalidXLogRecPtr)
			Assert(cur_txn->lsn <= cur_txn->last_lsn);

		if (last_lsn != InvalidXLogRecPtr)
			Assert(last_lsn < cur_txn->lsn);

		Assert(!cur_txn->is_known_as_subxact);
		last_lsn = cur_txn->lsn;
	}
#endif
}

ReorderBufferTXN *
ReorderBufferGetOldestTXN(ReorderBuffer * buffer)
{
	ReorderBufferTXN *txn;

	if (dlist_is_empty(&buffer->toplevel_by_lsn))
		return NULL;

	AssertTXNLsnOrder(buffer);

	txn = dlist_head_element(ReorderBufferTXN, node, &buffer->toplevel_by_lsn);

	Assert(!txn->is_known_as_subxact);
	Assert(txn->lsn != InvalidXLogRecPtr);
	return txn;
}

void
ReorderBufferSetRestartPoint(ReorderBuffer * buffer, XLogRecPtr ptr)
{
	buffer->current_restart_decoding_lsn = ptr;
}

void
ReorderBufferAssignChild(ReorderBuffer * buffer, TransactionId xid,
						 TransactionId subxid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;
	ReorderBufferTXN *subtxn;
	bool		new_sub;

	txn = ReorderBufferTXNByXid(buffer, xid, true, NULL, lsn, true);
	subtxn = ReorderBufferTXNByXid(buffer, subxid, true, &new_sub, lsn, false);

	if (new_sub)
	{
		/*
		 * we assign subtransactions to top level transaction even if we don't
		 * have data for it yet, assignment records frequently reference xids
		 * that have not yet produced any records. Knowing those aren't top
		 * level xids allows us to make processing cheaper in some places.
		 */
		dlist_push_tail(&txn->subtxns, &subtxn->node);
		txn->nsubtxns++;
	}
	else if (!subtxn->is_known_as_subxact)
	{
		subtxn->is_known_as_subxact = true;

		/* remove from lsn order list of top-level transactions */
		dlist_delete(&subtxn->node);

		/* add to toplevel transaction */
		dlist_push_tail(&txn->subtxns, &subtxn->node);
		txn->nsubtxns++;
	}
}

/*
 * Associate a subtransaction with its toplevel transaction at commit
 * time. There may be no further changes added after this.
 */
void
ReorderBufferCommitChild(ReorderBuffer * buffer, TransactionId xid,
						 TransactionId subxid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;
	ReorderBufferTXN *subtxn;
	bool		top_is_new;

	subtxn = ReorderBufferTXNByXid(buffer, subxid, false, NULL,
								   InvalidXLogRecPtr, false);

	/*
	 * No need to do anything if that subtxn didn't contain any changes
	 */
	if (!subtxn)
		return;

	/*
	 * FIXME: Using the subtxn lsn as top lsn isn't great (if we're creating)!
	 */
	txn = ReorderBufferTXNByXid(buffer, xid, true, &top_is_new, lsn, true);

	subtxn->last_lsn = lsn;

	Assert(!top_is_new || !subtxn->is_known_as_subxact);

	if (!subtxn->is_known_as_subxact)
	{
		subtxn->is_known_as_subxact = true;

		/* remove from lsn order list of top-level transactions */
		dlist_delete(&subtxn->node);

		/* add to subtransaction list */
		dlist_push_tail(&txn->subtxns, &subtxn->node);
		txn->nsubtxns++;
	}
}


/*
 * Support for efficiently iterating over a transaction's and its
 * subtransactions' changes.
 *
 * We do by doing a k-way merge between transactions/subtransactions. For that
 * we model the current heads of the different transactions as a binary heap so
 * we easily know which (sub-)transaction has the change with the smallest lsn
 * next.
 *
 * We assume the changes in individual transactions are already sorted by LSN.
 */

/*
 * Binary heap comparison function.
 */
static int
ReorderBufferIterCompare(Datum a, Datum b, void *arg)
{
	ReorderBufferIterTXNState *state = (ReorderBufferIterTXNState *) arg;
	XLogRecPtr	pos_a = state->entries[DatumGetInt32(a)].lsn;
	XLogRecPtr	pos_b = state->entries[DatumGetInt32(b)].lsn;

	if (pos_a < pos_b)
		return 1;
	else if (pos_a == pos_b)
		return 0;
	return -1;
}

/*
 * Allocate & initialize an iterator which iterates in lsn order over a
 * transaction and all its subtransactions.
 */
static ReorderBufferIterTXNState *
ReorderBufferIterTXNInit(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	Size		nr_txns = 0;
	ReorderBufferIterTXNState *state;
	dlist_iter	cur_txn_i;
	int32		off;

	/*
	 * Calculate the size of our heap: one element for every transaction that
	 * contains changes.  (Besides the transactions already in the reorder
	 * buffer, we count the one we were directly passed.)
	 */
	if (txn->nentries > 0)
		nr_txns++;

	dlist_foreach(cur_txn_i, &txn->subtxns)
	{
		ReorderBufferTXN *cur_txn;

		cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);

		if (cur_txn->nentries > 0)
			nr_txns++;
	}

	/*
	 * XXX: Add fastpath for the rather common nr_txns=1 case, no need to
	 * allocate/build a heap in that case.
	 */

	/* allocate iteration state */
	state = (ReorderBufferIterTXNState *)
		MemoryContextAllocZero(buffer->context,
							   sizeof(ReorderBufferIterTXNState) +
							   sizeof(ReorderBufferIterTXNEntry) * nr_txns);

	state->nr_txns = nr_txns;
	dlist_init(&state->old_change);

	for (off = 0; off < state->nr_txns; off++)
	{
		state->entries[off].fd = -1;
		state->entries[off].segno = 0;
	}

	/* allocate heap */
	state->heap = binaryheap_allocate(state->nr_txns, ReorderBufferIterCompare,
									  state);

	/*
	 * Now insert items into the binary heap, unordered.  (We will run a heap
	 * assembly step at the end; this is more efficient.)
	 */

	off = 0;

	/* add toplevel transaction if it contains changes */
	if (txn->nentries > 0)
	{
		ReorderBufferChange *cur_change;

		if (txn->nentries != txn->nentries_mem)
			ReorderBufferRestoreChanges(buffer, txn, &state->entries[off].fd,
										&state->entries[off].segno);

		cur_change = dlist_head_element(ReorderBufferChange, node,
										&txn->changes);

		state->entries[off].lsn = cur_change->lsn;
		state->entries[off].change = cur_change;
		state->entries[off].txn = txn;

		binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
	}

	/* add subtransactions if they contain changes */
	dlist_foreach(cur_txn_i, &txn->subtxns)
	{
		ReorderBufferTXN *cur_txn;

		cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);

		if (cur_txn->nentries > 0)
		{
			ReorderBufferChange *cur_change;

			if (txn->nentries != txn->nentries_mem)
				ReorderBufferRestoreChanges(buffer, cur_txn,
											&state->entries[off].fd,
											&state->entries[off].segno);

			cur_change = dlist_head_element(ReorderBufferChange, node,
											&cur_txn->changes);

			state->entries[off].lsn = cur_change->lsn;
			state->entries[off].change = cur_change;
			state->entries[off].txn = cur_txn;

			binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
		}
	}

	/* assemble a valid binary heap */
	binaryheap_build(state->heap);

	return state;
}

static void
ReorderBufferRestoreCleanup(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	XLogSegNo	first;
	XLogSegNo	cur;
	XLogSegNo	last;

	XLByteToSeg(txn->lsn, first);
	XLByteToSeg(txn->last_lsn, last);

	for (cur = first; cur <= last; cur++)
	{
		char		path[MAXPGPATH];
		XLogRecPtr	recptr;

		XLogSegNoOffsetToRecPtr(cur, 0, recptr);

		sprintf(path, "pg_llog/%s/xid-%u-lsn-%X-%X.snap",
				NameStr(MyLogicalDecodingSlot->name), txn->xid,
				(uint32) (recptr >> 32), (uint32) recptr);
		if (unlink(path) != 0 && errno != ENOENT)
			elog(FATAL, "could not unlink file \"%s\": %m", path);
	}
}

/*
 * Return the next change when iterating over a transaction and its
 * subtransaction.
 *
 * Returns NULL when no further changes exist.
 */
static ReorderBufferChange *
ReorderBufferIterTXNNext(ReorderBuffer * buffer, ReorderBufferIterTXNState * state)
{
	ReorderBufferChange *change;
	ReorderBufferIterTXNEntry *entry;
	int32		off;

	/* nothing there anymore */
	if (state->heap->bh_size == 0)
		return NULL;

	off = DatumGetInt32(binaryheap_first(state->heap));
	entry = &state->entries[off];

	if (!dlist_is_empty(&entry->txn->subtxns))
		elog(LOG, "tx with subtxn %u", entry->txn->xid);

	/* free memory we might have "leaked" in the previous *Next call */
	if (!dlist_is_empty(&state->old_change))
	{
		change = dlist_container(ReorderBufferChange, node,
								 dlist_pop_head_node(&state->old_change));
		ReorderBufferReturnChange(buffer, change);
		Assert(dlist_is_empty(&state->old_change));
	}

	change = entry->change;

	/*
	 * update heap with information about which transaction has the next
	 * relevant change in LSN order
	 */

	/* there are in-memory changes */
	if (dlist_has_next(&entry->txn->changes, &entry->change->node))
	{
		dlist_node *next = dlist_next_node(&entry->txn->changes, &change->node);
		ReorderBufferChange *next_change =
		dlist_container(ReorderBufferChange, node, next);

		/* txn stays the same */
		state->entries[off].lsn = next_change->lsn;
		state->entries[off].change = next_change;

		binaryheap_replace_first(state->heap, Int32GetDatum(off));
		return change;
	}

	/* try to load changes from disk */
	if (entry->txn->nentries != entry->txn->nentries_mem)
	{
		/*
		 * Ugly: restoring changes will reuse *Change records, thus delete the
		 * current one from the per-tx list and only free in the next call.
		 */
		dlist_delete(&change->node);
		dlist_push_tail(&state->old_change, &change->node);

		if (ReorderBufferRestoreChanges(buffer, entry->txn, &entry->fd,
										&state->entries[off].segno))
		{
			/* successfully restored changes from disk */
			ReorderBufferChange *next_change =
			dlist_head_element(ReorderBufferChange, node,
							   &entry->txn->changes);

			elog(DEBUG2, "restored %zu/%zu changes from disk",
				 entry->txn->nentries_mem, entry->txn->nentries);
			Assert(entry->txn->nentries_mem);
			/* txn stays the same */
			state->entries[off].lsn = next_change->lsn;
			state->entries[off].change = next_change;
			binaryheap_replace_first(state->heap, Int32GetDatum(off));

			return change;
		}
	}

	/* ok, no changes there anymore, remove */
	binaryheap_remove_first(state->heap);

	return change;
}

/*
 * Deallocate the iterator
 */
static void
ReorderBufferIterTXNFinish(ReorderBuffer * buffer,
						   ReorderBufferIterTXNState * state)
{
	int32		off;

	for (off = 0; off < state->nr_txns; off++)
	{
		if (state->entries[off].fd != -1)
			CloseTransientFile(state->entries[off].fd);
	}

	/* free memory we might have "leaked" in the last *Next call */
	if (!dlist_is_empty(&state->old_change))
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node,
								 dlist_pop_head_node(&state->old_change));
		ReorderBufferReturnChange(buffer, change);
		Assert(dlist_is_empty(&state->old_change));
	}

	binaryheap_free(state->heap);
	pfree(state);
}

/*
 * Cleanup the contents of a transaction, usually after the transaction
 * committed or aborted.
 */
static void
ReorderBufferCleanupTXN(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	bool		found;
	dlist_mutable_iter iter;

	/* cleanup subtransactions & their changes */
	dlist_foreach_modify(iter, &txn->subtxns)
	{
		ReorderBufferTXN *subtxn;

		subtxn = dlist_container(ReorderBufferTXN, node, iter.cur);
		Assert(subtxn->is_known_as_subxact);

		/*
		 * subtransactions are always associated to the toplevel TXN, even if
		 * they originally were happening inside another subtxn, so we won't
		 * ever recurse more than one level here.
		 */
		ReorderBufferCleanupTXN(buffer, subtxn);
	}

	/* cleanup changes in the toplevel txn */
	dlist_foreach_modify(iter, &txn->changes)
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, iter.cur);

		ReorderBufferReturnChange(buffer, change);
	}

	/*
	 * cleanup the tuplecids we stored timetravel access. They are always
	 * stored in the toplevel transaction.
	 */
	dlist_foreach_modify(iter, &txn->tuplecids)
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, iter.cur);
		Assert(change->action_internal == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);
		ReorderBufferReturnChange(buffer, change);
	}

	if (txn->base_snapshot != NULL)
	{
		SnapBuildSnapDecRefcount(txn->base_snapshot);
		txn->base_snapshot = NULL;
	}

	/* delete from LSN ordered list of toplevel TXNs */
	if (!txn->is_known_as_subxact)
		dlist_delete(&txn->node);

	/* now remove reference from buffer */
	hash_search(buffer->by_txn,
				(void *) &txn->xid,
				HASH_REMOVE,
				&found);
	Assert(found);

	/* remove entries spilled to disk */
	if (txn->nentries != txn->nentries_mem)
		ReorderBufferRestoreCleanup(buffer, txn);

	/* deallocate */
	ReorderBufferReturnTXN(buffer, txn);
}

/*
 * Build a hash with a (relfilenode, ctid) -> (cmin, cmax) mapping for use by
 * tqual.c's HeapTupleSatisfiesMVCCDuringDecoding.
 */
static void
ReorderBufferBuildTupleCidHash(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	dlist_iter	iter;
	HASHCTL		hash_ctl;

	if (!txn->does_timetravel || dlist_is_empty(&txn->tuplecids))
		return;

	memset(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = sizeof(ReorderBufferTupleCidKey);
	hash_ctl.entrysize = sizeof(ReorderBufferTupleCidEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = buffer->context;

	/*
	 * create the hash with the exact number of to-be-stored tuplecids from
	 * the start
	 */
	txn->tuplecid_hash =
		hash_create("ReorderBufferTupleCid", txn->ntuplecids, &hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	dlist_foreach(iter, &txn->tuplecids)
	{
		ReorderBufferTupleCidKey key;
		ReorderBufferTupleCidEnt *ent;
		bool		found;
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, iter.cur);

		Assert(change->action_internal == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);

		/* be careful about padding */
		memset(&key, 0, sizeof(ReorderBufferTupleCidKey));

		key.relnode = change->tuplecid.node;

		ItemPointerCopy(&change->tuplecid.tid,
						&key.tid);

		ent = (ReorderBufferTupleCidEnt *)
			hash_search(txn->tuplecid_hash,
						(void *) &key,
						HASH_ENTER | HASH_FIND,
						&found);
		if (!found)
		{
			ent->cmin = change->tuplecid.cmin;
			ent->cmax = change->tuplecid.cmax;
			ent->combocid = change->tuplecid.combocid;
		}
		else
		{
			Assert(ent->cmin == change->tuplecid.cmin);
			Assert(ent->cmax == InvalidCommandId ||
				   ent->cmax == change->tuplecid.cmax);

			/*
			 * if the tuple got valid in this transaction and now got deleted
			 * we already have a valid cmin stored. The cmax will be
			 * InvalidCommandId though.
			 */
			ent->cmax = change->tuplecid.cmax;
		}
	}
}

/*
 * Copy a provided snapshot so we can modify it privately. This is needed so
 * that catalog modifying transactions can look into intermediate catalog
 * states.
 */
static Snapshot
ReorderBufferCopySnap(ReorderBuffer * buffer, Snapshot orig_snap,
					  ReorderBufferTXN * txn, CommandId cid)
{
	Snapshot	snap;
	dlist_iter	iter;
	int			i = 0;
	Size		size;

	size = sizeof(SnapshotData) +
		sizeof(TransactionId) * orig_snap->xcnt +
		sizeof(TransactionId) * (txn->nsubtxns + 1);

	elog(DEBUG1, "copying a non-transaction-specific snapshot into timetravel tx %u", txn->xid);

	snap = MemoryContextAllocZero(buffer->context, size);
	memcpy(snap, orig_snap, sizeof(SnapshotData));

	snap->copied = true;
	snap->active_count = 0;
	snap->regd_count = 0;
	snap->xip = (TransactionId *) (snap + 1);

	memcpy(snap->xip, orig_snap->xip, sizeof(TransactionId) * snap->xcnt);

	/*
	 * ->subxip contains all txids that belong to our transaction which we
	 * need to check via cmin/cmax. Thats why we store the toplevel
	 * transaction in there as well.
	 */
	snap->subxip = snap->xip + snap->xcnt;
	snap->subxip[i++] = txn->xid;
	snap->subxcnt = txn->nsubtxns + 1;

	dlist_foreach(iter, &txn->subtxns)
	{
		ReorderBufferTXN *sub_txn;

		sub_txn = dlist_container(ReorderBufferTXN, node, iter.cur);
		snap->subxip[i++] = sub_txn->xid;
	}

	/* sort so we can bsearch() later */
	qsort(snap->subxip, snap->subxcnt, sizeof(TransactionId), xidComparator);

	/* store the specified current CommandId */
	snap->curcid = cid;

	return snap;
}

/*
 * Free a previously ReorderBufferCopySnap'ed snapshot
 */
static void
ReorderBufferFreeSnap(ReorderBuffer * buffer, Snapshot snap)
{
	if (snap->copied)
		pfree(snap);
	else
		SnapBuildSnapDecRefcount(snap);
}

/*
 * Commit a transaction and replay all actions that previously have been
 * ReorderBufferAddChange'd in the toplevel TX or any of the subtransactions
 * assigned via ReorderBufferCommitChild.
 */
void
ReorderBufferCommit(ReorderBuffer * buffer, TransactionId xid, XLogRecPtr lsn,
					RepNodeId origin, TimestampTz commit_time)
{
	ReorderBufferTXN *txn;
	ReorderBufferIterTXNState *iterstate = NULL;
	ReorderBufferChange *change;
	CommandId	command_id = FirstCommandId;
	Snapshot	snapshot_now;
	Relation	relation = NULL;

	txn = ReorderBufferTXNByXid(buffer, xid, false, NULL, InvalidXLogRecPtr,
								false);

	/* empty transaction */
	if (!txn)
		return;

	txn->last_lsn = lsn;
	txn->origin_id = origin;
	txn->commit_time = commit_time;

	/* serialize the last bunch of changes if we need start earlier anyway */
	if (txn->nentries_mem != txn->nentries)
		ReorderBufferSerializeTXN(buffer, txn);

	/*
	 * If this transaction didn't have any real changes in our database, it's
	 * OK not to have a snapshot.
	 */
	if (txn->base_snapshot == NULL)
		return;

	snapshot_now = txn->base_snapshot;

	ReorderBufferBuildTupleCidHash(buffer, txn);

	/* setup initial snapshot */
	SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);

	PG_TRY();
	{
		buffer->begin(buffer, txn);

		iterstate = ReorderBufferIterTXNInit(buffer, txn);
		while ((change = ReorderBufferIterTXNNext(buffer, iterstate)))
		{
			switch ((ReorderBufferChangeTypeInternal) change->action_internal)
			{
				case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
				case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
				case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
					Assert(snapshot_now);
					relation = LookupRelationByRelFileNode(&change->relnode);
					if (relation == NULL)
					{
						elog(ERROR, "could not lookup relation %s",
							 relpathperm(change->relnode, MAIN_FORKNUM));
					}

					if (RelationIsLogicallyLogged(relation))
					{
						/* user-triggered change */
						if (relation->rd_rel->relkind == RELKIND_SEQUENCE)
						{
						}
						else if (!IsToastRelation(relation))
						{
							ReorderBufferToastReplace(buffer, txn, relation, change);
							buffer->apply_change(buffer, txn, relation, change);
							ReorderBufferToastReset(buffer, txn);
						}
						/* we're not interested in toast deletions */
						else if (change->action == REORDER_BUFFER_CHANGE_INSERT)
						{
							/*
							 * need to reassemble change in memory, ensure it
							 * doesn't get reused till we're done.
							 */
							dlist_delete(&change->node);
							ReorderBufferToastAppendChunk(buffer, txn, relation,
														  change);
						}

					}
					RelationClose(relation);
					break;
				case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
					/* XXX: we could skip snapshots in non toplevel txns */

					/* get rid of the old */
					RevertFromDecodingSnapshots();

					if (snapshot_now->copied)
					{
						ReorderBufferFreeSnap(buffer, snapshot_now);
						snapshot_now =
							ReorderBufferCopySnap(buffer, change->snapshot,
												  txn, command_id);
					}

					/*
					 * restored from disk, we need to be careful not to double
					 * free. We could introduce refcounting for that, but for
					 * now this seems infrequent enough not to care.
					 */
					else if (change->snapshot->copied)
					{
						snapshot_now =
							ReorderBufferCopySnap(buffer, change->snapshot,
												  txn, command_id);
					}
					else
					{
						snapshot_now = change->snapshot;
					}


					/* and start with the new one */
					SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);
					break;

				case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
					if (!snapshot_now->copied)
					{
						/* we don't use the global one anymore */
						snapshot_now = ReorderBufferCopySnap(buffer, snapshot_now,
															 txn, command_id);
					}

					command_id = Max(command_id, change->command_id);

					if (command_id != InvalidCommandId)
					{
						snapshot_now->curcid = command_id;

						RevertFromDecodingSnapshots();
						SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);
					}

					/*
					 * everytime the CommandId is incremented, we could see
					 * new catalog contents
					 */
					ReorderBufferExecuteInvalidations(buffer, txn);

					break;

				case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
					elog(ERROR, "tuplecid value in normal queue");
					break;
			}
		}

		ReorderBufferIterTXNFinish(buffer, iterstate);

		/* call commit callback */
		buffer->commit(buffer, txn, lsn);


		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 true, true);

		AtEOXact_RelationCache(true);

		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_LOCKS,
							 true, true);

		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_AFTER_LOCKS,
							 true, true);

		/* cleanup */
		RevertFromDecodingSnapshots();

		ReorderBufferExecuteInvalidations(buffer, txn);

		if (snapshot_now->copied)
			ReorderBufferFreeSnap(buffer, snapshot_now);

		ReorderBufferCleanupTXN(buffer, txn);
	}
	PG_CATCH();
	{
		if (iterstate)
			ReorderBufferIterTXNFinish(buffer, iterstate);

		RevertFromDecodingSnapshots();

		/* XXX: more cleanup needed */

		if (snapshot_now->copied)
			ReorderBufferFreeSnap(buffer, snapshot_now);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Abort a transaction that possibly has previous changes. Needs to be done
 * independently for toplevel and subtransactions.
 */
void
ReorderBufferAbort(ReorderBuffer * buffer, TransactionId xid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, false, NULL, InvalidXLogRecPtr,
								false);

	/* no changes in this commit */
	if (!txn)
		return;

	txn->last_lsn = lsn;

	ReorderBufferCleanupTXN(buffer, txn);
}

/*
 * Check whether a transaction is already known in this module
 */
bool
ReorderBufferIsXidKnown(ReorderBuffer * buffer, TransactionId xid)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, false, NULL, InvalidXLogRecPtr,
								false);
	return txn != NULL;
}

/*
 * Add a new snapshot to this transaction that is only used after lsn 'lsn'.
 */
void
ReorderBufferAddSnapshot(ReorderBuffer * buffer, TransactionId xid,
						 XLogRecPtr lsn, Snapshot snap)
{
	ReorderBufferChange *change = ReorderBufferGetChange(buffer);

	change->snapshot = snap;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT;

	ReorderBufferAddChange(buffer, xid, lsn, change);
}

/*
 * Setup the base snapshot of a transaction. That is the snapshot that is used
 * to decode all changes until either this transaction modifies the catalog or
 * another catalog modifying transaction commits.
 */
void
ReorderBufferSetBaseSnapshot(ReorderBuffer * buffer, TransactionId xid,
							 XLogRecPtr lsn, Snapshot snap)
{
	ReorderBufferTXN *txn;
	bool		is_new;

	txn = ReorderBufferTXNByXid(buffer, xid, true, &is_new, lsn, true);
	Assert(txn->base_snapshot == NULL);

	txn->base_snapshot = snap;
}

/*
 * Access the catalog with this CommandId at this point in the changestream.
 *
 * May only be called for command ids > 1
 */
void
ReorderBufferAddNewCommandId(ReorderBuffer * buffer, TransactionId xid,
							 XLogRecPtr lsn, CommandId cid)
{
	ReorderBufferChange *change = ReorderBufferGetChange(buffer);

	change->command_id = cid;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID;

	ReorderBufferAddChange(buffer, xid, lsn, change);
}


/*
 * Add new (relfilenode, tid) -> (cmin, cmax) mappings.
 */
void
ReorderBufferAddNewTupleCids(ReorderBuffer * buffer, TransactionId xid,
							 XLogRecPtr lsn, RelFileNode node,
							 ItemPointerData tid, CommandId cmin,
							 CommandId cmax, CommandId combocid)
{
	ReorderBufferChange *change = ReorderBufferGetChange(buffer);
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, true, NULL, lsn, true);

	change->tuplecid.node = node;
	change->tuplecid.tid = tid;
	change->tuplecid.cmin = cmin;
	change->tuplecid.cmax = cmax;
	change->tuplecid.combocid = combocid;
	change->lsn = lsn;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID;

	dlist_push_tail(&txn->tuplecids, &change->node);
	txn->ntuplecids++;
}

/*
 * Setup the invalidation of the toplevel transaction.
 *
 * This needs to be done before ReorderBufferCommit is called!
 */
void
ReorderBufferAddInvalidations(ReorderBuffer * buffer, TransactionId xid,
							  XLogRecPtr lsn, Size nmsgs,
							  SharedInvalidationMessage *msgs)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, true, NULL, lsn, true);

	if (txn->ninvalidations != 0)
		elog(ERROR, "only ever add one set of invalidations");

	txn->ninvalidations = nmsgs;
	txn->invalidations = (SharedInvalidationMessage *)
		MemoryContextAlloc(buffer->context,
						   sizeof(SharedInvalidationMessage) * nmsgs);
	memcpy(txn->invalidations, msgs, sizeof(SharedInvalidationMessage) * nmsgs);
}

/*
 * Apply all invalidations we know. Possibly we only need parts at this point
 * in the changestream but we don't know which those are.
 */
static void
ReorderBufferExecuteInvalidations(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	int			i;

	for (i = 0; i < txn->ninvalidations; i++)
		LocalExecuteInvalidationMessage(&txn->invalidations[i]);
}

/*
 * Mark a transaction as doing timetravel.
 */
void
ReorderBufferXidSetTimetravel(ReorderBuffer * buffer, TransactionId xid,
							  XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, true, NULL, lsn, true);

	txn->does_timetravel = true;
}

/*
 * Query whether a transaction is already *known* to be doing timetravel. This
 * can be wrong until directly before the commit!
 */
bool
ReorderBufferXidDoesTimetravel(ReorderBuffer * buffer, TransactionId xid)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, false, NULL, InvalidXLogRecPtr,
								false);
	if (!txn)
		return false;

	return txn->does_timetravel;
}

/*
 * Have we already added the first snapshot?
 */
bool
ReorderBufferXidHasBaseSnapshot(ReorderBuffer * buffer, TransactionId xid)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(buffer, xid, false, NULL, InvalidXLogRecPtr,
								false);

	if (!txn)
		return false;
	return txn->base_snapshot != NULL;
}

static void
ReorderBufferSerializeReserve(ReorderBuffer * buffer, Size sz)
{
	if (!buffer->outbufsize)
	{
		buffer->outbuf = MemoryContextAlloc(buffer->context, sz);
		buffer->outbufsize = sz;
	}
	else if (buffer->outbufsize < sz)
	{
		buffer->outbuf = repalloc(buffer->outbuf, sz);
		buffer->outbufsize = sz;
	}
}

typedef struct ReorderBufferDiskChange
{
	Size		size;
	ReorderBufferChange change;
	/* data follows */
}	ReorderBufferDiskChange;

/*
 * Persistency support
 */
static void
ReorderBufferSerializeChange(ReorderBuffer * buffer, ReorderBufferTXN * txn,
							 int fd, ReorderBufferChange * change)
{
	ReorderBufferDiskChange *ondisk;
	Size		sz = sizeof(ReorderBufferDiskChange);

	ReorderBufferSerializeReserve(buffer, sz);

	ondisk = (ReorderBufferDiskChange *) buffer->outbuf;
	memcpy(&ondisk->change, change, sizeof(ReorderBufferChange));

	switch ((ReorderBufferChangeTypeInternal) change->action_internal)
	{
		case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
			{
				char	   *data;
				Size		oldlen = 0;
				Size		newlen = 0;

				if (change->oldtuple)
					oldlen = offsetof(ReorderBufferTupleBuf, data)
						+ change->oldtuple->tuple.t_len
						- offsetof(HeapTupleHeaderData, t_bits);

				if (change->newtuple)
					newlen = offsetof(ReorderBufferTupleBuf, data)
						+ change->newtuple->tuple.t_len
						- offsetof(HeapTupleHeaderData, t_bits);

				sz += oldlen;
				sz += newlen;

				/* make sure we have enough space */
				ReorderBufferSerializeReserve(buffer, sz);

				data = ((char *) buffer->outbuf) + sizeof(ReorderBufferDiskChange);
				/* might have been reallocated above */
				ondisk = (ReorderBufferDiskChange *) buffer->outbuf;

				if (oldlen)
				{
					memcpy(data, change->oldtuple, oldlen);
					data += oldlen;
					Assert(&change->oldtuple->header == change->oldtuple->tuple.t_data);
				}

				if (newlen)
				{
					memcpy(data, change->newtuple, newlen);
					data += newlen;
					Assert(&change->newtuple->header == change->newtuple->tuple.t_data);
				}
				break;
			}
		case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
			{
				char	   *data;

				sz += sizeof(SnapshotData) +
					sizeof(TransactionId) * change->snapshot->xcnt +
					sizeof(TransactionId) * change->snapshot->subxcnt
					;

				/* make sure we have enough space */
				ReorderBufferSerializeReserve(buffer, sz);
				data = ((char *) buffer->outbuf) + sizeof(ReorderBufferDiskChange);
				/* might have been reallocated above */
				ondisk = (ReorderBufferDiskChange *) buffer->outbuf;

				memcpy(data, change->snapshot, sizeof(SnapshotData));
				data += sizeof(SnapshotData);

				if (change->snapshot->xcnt)
				{
					memcpy(data, change->snapshot->xip,
						   sizeof(TransactionId) + change->snapshot->xcnt);
					data += sizeof(TransactionId) + change->snapshot->xcnt;
				}

				if (change->snapshot->subxcnt)
				{
					memcpy(data, change->snapshot->subxip,
						   sizeof(TransactionId) + change->snapshot->subxcnt);
					data += sizeof(TransactionId) + change->snapshot->subxcnt;
				}
				break;
			}
		case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
			/* ReorderBufferChange contains everything important */
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
			/* ReorderBufferChange contains everything important */
			break;
	}

	ondisk->size = sz;

	if (write(fd, buffer->outbuf, ondisk->size) != ondisk->size)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to xid data file \"%u\": %m",
						txn->xid)));
	}

	Assert(ondisk->change.action_internal == change->action_internal);
}

static void
ReorderBufferCheckSerializeTXN(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	/* FIXME subtxn handling? */
	if (txn->nentries_mem >= max_memtries)
	{
		ReorderBufferSerializeTXN(buffer, txn);
		Assert(txn->nentries_mem == 0);
	}
}

static void
ReorderBufferSerializeTXN(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	dlist_iter	subtxn_i;
	dlist_mutable_iter change_i;
	int			fd = -1;
	XLogSegNo	curOpenSegNo = 0;
	Size		spilled = 0;
	char		path[MAXPGPATH];

	elog(DEBUG2, "spill %zu transactions in tx %u to disk",
		 txn->nentries_mem, txn->xid);

	/* do the same to all child TXs */
	dlist_foreach(subtxn_i, &txn->subtxns)
	{
		ReorderBufferTXN *subtxn;

		subtxn = dlist_container(ReorderBufferTXN, node, subtxn_i.cur);
		ReorderBufferSerializeTXN(buffer, subtxn);
	}

	/* serialize changestream */
	dlist_foreach_modify(change_i, &txn->changes)
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, change_i.cur);

		/*
		 * store in segment in which it belongs by start lsn, don't split over
		 * multiple segments tho
		 */
		if (fd == -1 || XLByteInSeg(change->lsn, curOpenSegNo))
		{
			XLogRecPtr	recptr;

			if (fd != -1)
				CloseTransientFile(fd);

			XLByteToSeg(change->lsn, curOpenSegNo);
			XLogSegNoOffsetToRecPtr(curOpenSegNo, 0, recptr);

			sprintf(path, "pg_llog/%s/xid-%u-lsn-%X-%X.snap",
					NameStr(MyLogicalDecodingSlot->name), txn->xid,
					(uint32) (recptr >> 32), (uint32) recptr);

			/* open segment, create it if necessary */
			fd = OpenTransientFile(path,
								   O_CREAT | O_WRONLY | O_APPEND | PG_BINARY,
								   S_IRUSR | S_IWUSR);

			if (fd < 0)
				ereport(ERROR, (errmsg("could not open reorderbuffer file %s for writing: %m", path)));
		}

		ReorderBufferSerializeChange(buffer, txn, fd, change);
		dlist_delete(&change->node);
		ReorderBufferReturnChange(buffer, change);

		spilled++;
	}

	Assert(spilled == txn->nentries_mem);
	Assert(dlist_is_empty(&txn->changes));
	txn->nentries_mem = 0;

	if (fd != -1)
		CloseTransientFile(fd);

	/* issue write barrier */
	/* serialize main transaction state */
}

static Size
ReorderBufferRestoreChanges(ReorderBuffer * buffer, ReorderBufferTXN * txn,
							int *fd, XLogSegNo * segno)
{
	Size		restored = 0;
	XLogSegNo	last_segno;
	dlist_mutable_iter cleanup_iter;

	Assert(txn->lsn != InvalidXLogRecPtr);
	Assert(txn->last_lsn != InvalidXLogRecPtr);

	/* free current entries, so we have memory for more */
	dlist_foreach_modify(cleanup_iter, &txn->changes)
	{
		ReorderBufferChange *cleanup =
		dlist_container(ReorderBufferChange, node, cleanup_iter.cur);

		dlist_delete(&cleanup->node);
		ReorderBufferReturnChange(buffer, cleanup);
	}
	txn->nentries_mem = 0;
	Assert(dlist_is_empty(&txn->changes));

	XLByteToSeg(txn->last_lsn, last_segno);

	while (restored < max_memtries && *segno <= last_segno)
	{
		int			readBytes;
		ReorderBufferDiskChange *ondisk;

		if (*fd == -1)
		{
			XLogRecPtr	recptr;
			char		path[MAXPGPATH];

			/* first time in */
			if (*segno == 0)
			{
				XLByteToSeg(txn->lsn, *segno);
				elog(LOG, "initial restoring from %zu to %zu",
					 *segno, last_segno);
			}

			Assert(*segno != 0 || dlist_is_empty(&txn->changes));
			XLogSegNoOffsetToRecPtr(*segno, 0, recptr);

			sprintf(path, "pg_llog/%s/xid-%u-lsn-%X-%X.snap",
					NameStr(MyLogicalDecodingSlot->name), txn->xid,
					(uint32) (recptr >> 32), (uint32) recptr);

			elog(LOG, "opening file %s", path);

			*fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
			if (*fd < 0 && errno == ENOENT)
			{
				*fd = -1;
				(*segno)++;
				continue;
			}
			else if (*fd < 0)
				ereport(ERROR, (errmsg("could not open reorderbuffer file %s for reading: %m", path)));

		}

		ReorderBufferSerializeReserve(buffer, sizeof(ReorderBufferDiskChange));


		/*
		 * read the statically sized part of a change which has information
		 * about the total size. If we couldn't read a record, we're at the
		 * end of this file.
		 */

		readBytes = read(*fd, buffer->outbuf, sizeof(ReorderBufferDiskChange));

		/* eof */
		if (readBytes == 0)
		{
			CloseTransientFile(*fd);
			*fd = -1;
			(*segno)++;
			continue;
		}
		else if (readBytes < 0)
			elog(ERROR, "read failed: %m");
		else if (readBytes != sizeof(ReorderBufferDiskChange))
			elog(ERROR, "incomplete read, read %d instead of %zu",
				 readBytes, sizeof(ReorderBufferDiskChange));

		ondisk = (ReorderBufferDiskChange *) buffer->outbuf;

		ReorderBufferSerializeReserve(buffer, sizeof(ReorderBufferDiskChange) + ondisk->size);
		ondisk = (ReorderBufferDiskChange *) buffer->outbuf;

		readBytes = read(*fd, buffer->outbuf + sizeof(ReorderBufferDiskChange),
						 ondisk->size - sizeof(ReorderBufferDiskChange));

		if (readBytes < 0)
			elog(ERROR, "read2 failed: %m");
		else if (readBytes != ondisk->size - sizeof(ReorderBufferDiskChange))
			elog(ERROR, "incomplete read2, read %d instead of %zu",
				 readBytes, ondisk->size - sizeof(ReorderBufferDiskChange));

		/*
		 * ok, read a full change from disk, now restore it into proper
		 * in-memory format
		 */
		ReorderBufferRestoreChange(buffer, txn, buffer->outbuf);
		restored++;
	}

	return restored;
}

/*
 * Convert change from its on-disk format to in-memory format and queue it onto
 * the TXN's ->changes list.
 */
static void
ReorderBufferRestoreChange(ReorderBuffer * buffer, ReorderBufferTXN * txn,
						   char *data)
{
	ReorderBufferDiskChange *ondisk;
	ReorderBufferChange *change;

	ondisk = (ReorderBufferDiskChange *) data;

	change = ReorderBufferGetChange(buffer);

	/* copy static part */
	memcpy(change, &ondisk->change, sizeof(ReorderBufferChange));

	data += sizeof(ReorderBufferDiskChange);

	/* restore individual stuff */
	switch ((ReorderBufferChangeTypeInternal) change->action_internal)
	{
		case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
			if (change->newtuple)
			{
				Size		len = offsetof(ReorderBufferTupleBuf, data)
				+ ((ReorderBufferTupleBuf *) data)->tuple.t_len
				- offsetof(HeapTupleHeaderData, t_bits);

				change->newtuple = ReorderBufferGetTupleBuf(buffer);
				memcpy(change->newtuple, data, len);
				change->newtuple->tuple.t_data = &change->newtuple->header;

				data += len;
			}

			if (change->oldtuple)
			{
				Size		len = offsetof(ReorderBufferTupleBuf, data)
				+ ((ReorderBufferTupleBuf *) data)->tuple.t_len
				- offsetof(HeapTupleHeaderData, t_bits);

				change->oldtuple = ReorderBufferGetTupleBuf(buffer);
				memcpy(change->oldtuple, data, len);
				change->oldtuple->tuple.t_data = &change->oldtuple->header;
				data += len;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
			{
				Snapshot	oldsnap = (Snapshot) data;
				Size		size = sizeof(SnapshotData) +
				sizeof(TransactionId) * oldsnap->xcnt +
				sizeof(TransactionId) * (oldsnap->subxcnt + 0)
						   ;

				Assert(change->snapshot != NULL);

				change->snapshot = MemoryContextAllocZero(buffer->context, size);

				memcpy(change->snapshot, data, size);
				change->snapshot->xip = (TransactionId *)
					(((char *) change->snapshot) + sizeof(SnapshotData));
				change->snapshot->subxip =
					change->snapshot->xip + change->snapshot->xcnt + 0;
				change->snapshot->copied = true;
				break;
			}
			/* nothing needs to be done */
		case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
		case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
			break;
	}

	dlist_push_tail(&txn->changes, &change->node);
	txn->nentries_mem++;
}

/*
 * Delete all data spilled to disk.
 */
void
ReorderBufferStartup(void)
{
	DIR		   *logical_dir;
	struct dirent *logical_de;

	DIR		   *spill_dir;
	struct dirent *spill_de;

	logical_dir = AllocateDir("pg_llog");
	while ((logical_de = ReadDir(logical_dir, "pg_llog")) != NULL)
	{
		char		path[MAXPGPATH];

		if (strcmp(logical_de->d_name, ".") == 0 ||
			strcmp(logical_de->d_name, "..") == 0)
			continue;

		/* one of our own directories */
		if (strcmp(logical_de->d_name, "snapshots") == 0)
			continue;

		/*
		 * ok, has to be a surviving logical slot, iterate and delete
		 * everythign starting with xid-*
		 */
		sprintf(path, "pg_llog/%s", logical_de->d_name);

		spill_dir = AllocateDir(path);
		while ((spill_de = ReadDir(spill_dir, "pg_llog")) != NULL)
		{
			if (strcmp(spill_de->d_name, ".") == 0 ||
				strcmp(spill_de->d_name, "..") == 0)
				continue;

			if (strncmp(spill_de->d_name, "xid", 3) == 0)
			{
				sprintf(path, "pg_llog/%s/%s", logical_de->d_name,
						spill_de->d_name);

				if (unlink(path) != 0)
					ereport(PANIC,
							(errcode_for_file_access(),
						  errmsg("could not remove xid data file \"%s\": %m",
								 path)));
			}
			/* XXX: WARN? */
		}
		FreeDir(spill_dir);
	}
	FreeDir(logical_dir);
}

/*
 * toast support
 */

/*
 * copied stuff from tuptoaster.c. Perhaps there should be toast_internal.h?
 */
#define VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr)	\
do { \
	varattrib_1b_e *attre = (varattrib_1b_e *) (attr); \
	Assert(VARATT_IS_EXTERNAL(attre)); \
	Assert(VARSIZE_EXTERNAL(attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL); \
	memcpy(&(toast_pointer), VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
} while (0)

#define VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
	((toast_pointer).va_extsize < (toast_pointer).va_rawsize - VARHDRSZ)

/*
 * Initialize per tuple toast reconstruction support.
 */
static void
ReorderBufferToastInitHash(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	HASHCTL		hash_ctl;

	Assert(txn->toast_hash == NULL);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(ReorderBufferToastEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = buffer->context;
	txn->toast_hash = hash_create("ReorderBufferToastHash", 5, &hash_ctl,
								  HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * Per toast-chunk handling for toast reconstruction
 *
 * Appends a toast chunk so we can reconstruct it when the tuple "owning" the
 * toasted Datum comes along.
 */
static void
ReorderBufferToastAppendChunk(ReorderBuffer * buffer, ReorderBufferTXN * txn,
							  Relation relation, ReorderBufferChange * change)
{
	ReorderBufferToastEnt *ent;
	bool		found;
	int32		chunksize;
	bool		isnull;
	Pointer		chunk;
	TupleDesc	desc = RelationGetDescr(relation);
	Oid			chunk_id;
	Oid			chunk_seq;

	if (txn->toast_hash == NULL)
		ReorderBufferToastInitHash(buffer, txn);

	Assert(IsToastRelation(relation));

	chunk_id = DatumGetObjectId(fastgetattr(&change->newtuple->tuple, 1, desc, &isnull));
	Assert(!isnull);
	chunk_seq = DatumGetInt32(fastgetattr(&change->newtuple->tuple, 2, desc, &isnull));
	Assert(!isnull);

	ent = (ReorderBufferToastEnt *)
		hash_search(txn->toast_hash,
					(void *) &chunk_id,
					HASH_ENTER,
					&found);

	if (!found)
	{
		Assert(ent->chunk_id == chunk_id);
		ent->num_chunks = 0;
		ent->last_chunk_seq = 0;
		ent->size = 0;
		ent->reconstructed = NULL;
		dlist_init(&ent->chunks);

		if (chunk_seq != 0)
			elog(ERROR, "got sequence entry %d for toast chunk %u instead of seq 0",
				 chunk_seq, chunk_id);
	}
	else if (found && chunk_seq != ent->last_chunk_seq + 1)
		elog(ERROR, "got sequence entry %d for toast chunk %u instead of seq %d",
			 chunk_seq, chunk_id, ent->last_chunk_seq + 1);

	chunk = DatumGetPointer(fastgetattr(&change->newtuple->tuple, 3, desc, &isnull));
	Assert(!isnull);

	/* calculate size so we can allocate the right size at once later */
	if (!VARATT_IS_EXTENDED(chunk))
		chunksize = VARSIZE(chunk) - VARHDRSZ;
	else if (VARATT_IS_SHORT(chunk))
		/* could happen due to heap_form_tuple doing its thing */
		chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
	else
		elog(ERROR, "unexpected type of toast chunk");

	ent->size += chunksize;
	ent->last_chunk_seq = chunk_seq;
	ent->num_chunks++;
	dlist_push_tail(&ent->chunks, &change->node);
}

/*
 * Rejigger change->newtuple to point to in-memory toast tuples instead to
 * on-disk toast tuples that may not longer exist (think DROP TABLE or VACUUM).
 *
 * We cannot replace unchanged toast tuples though, so those will still point
 * to on-disk toast data.
 */
static void
ReorderBufferToastReplace(ReorderBuffer * buffer, ReorderBufferTXN * txn,
						  Relation relation, ReorderBufferChange * change)
{
	TupleDesc	desc;
	int			natt;
	Datum	   *attrs;
	bool	   *isnull;
	bool	   *free;
	HeapTuple	newtup;
	Relation	toast_rel;
	TupleDesc	toast_desc;

	/* no toast tuples changed */
	if (txn->toast_hash == NULL)
		return;

	/* we should only have toast tuples in an INSERT or UPDATE */
	Assert(change->newtuple);

	desc = RelationGetDescr(relation);

	toast_rel = RelationIdGetRelation(relation->rd_rel->reltoastrelid);
	toast_desc = RelationGetDescr(toast_rel);

	/* should we allocate from stack instead? */
	attrs = palloc0(sizeof(Datum) * desc->natts);
	isnull = palloc0(sizeof(bool) * desc->natts);
	free = palloc0(sizeof(bool) * desc->natts);

	heap_deform_tuple(&change->newtuple->tuple, desc,
					  attrs, isnull);

	for (natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute attr = desc->attrs[natt];
		ReorderBufferToastEnt *ent;
		struct varlena *varlena;

		/* va_rawsize is the size of the original datum -- including header */
		struct varatt_external toast_pointer;
		struct varatt_indirect redirect_pointer;
		struct varlena *new_datum = NULL;
		struct varlena *reconstructed;
		dlist_iter	it;
		Size		data_done = 0;

		/* system columns aren't toasted */
		if (attr->attnum < 0)
			continue;

		if (attr->attisdropped)
			continue;

		/* not a varlena datatype */
		if (attr->attlen != -1)
			continue;

		/* no data */
		if (isnull[natt])
			continue;

		/* ok, we know we have a toast datum */
		varlena = (struct varlena *) DatumGetPointer(attrs[natt]);

		/* no need to do anything if the tuple isn't external */
		if (!VARATT_IS_EXTERNAL(varlena))
			continue;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, varlena);

		/*
		 * check whether the toast tuple changed, replace if so.
		 */
		ent = (ReorderBufferToastEnt *)
			hash_search(txn->toast_hash,
						(void *) &toast_pointer.va_valueid,
						HASH_FIND,
						NULL);
		if (ent == NULL)
			continue;

		new_datum =
			(struct varlena *) palloc0(INDIRECT_POINTER_SIZE);

		free[natt] = true;

		reconstructed = palloc0(toast_pointer.va_rawsize);

		ent->reconstructed = reconstructed;

		/* stitch toast tuple back together from its parts */
		dlist_foreach(it, &ent->chunks)
		{
			bool		isnull;
			ReorderBufferTupleBuf *tup =
			dlist_container(ReorderBufferChange, node, it.cur)->newtuple;
			Pointer		chunk =
			DatumGetPointer(fastgetattr(&tup->tuple, 3, toast_desc, &isnull));

			Assert(!isnull);
			Assert(!VARATT_IS_EXTERNAL(chunk));
			Assert(!VARATT_IS_SHORT(chunk));

			memcpy(VARDATA(reconstructed) + data_done,
				   VARDATA(chunk),
				   VARSIZE(chunk) - VARHDRSZ);
			data_done += VARSIZE(chunk) - VARHDRSZ;
		}
		Assert(data_done == toast_pointer.va_extsize);

		/* make sure its marked as compressed or not */
		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			SET_VARSIZE_COMPRESSED(reconstructed, data_done + VARHDRSZ);
		else
			SET_VARSIZE(reconstructed, data_done + VARHDRSZ);

		memset(&redirect_pointer, 0, sizeof(redirect_pointer));
		redirect_pointer.pointer = reconstructed;

		SET_VARSIZE_EXTERNAL(new_datum, INDIRECT_POINTER_SIZE);
		memcpy(VARDATA_EXTERNAL(new_datum), &redirect_pointer,
			   sizeof(redirect_pointer));

		attrs[natt] = PointerGetDatum(new_datum);
	}

	/*
	 * Build tuple in separate memory & copy tuple back into the tuplebuf
	 * passed to the output plugin. We can't directly heap_fill_tuple() into
	 * the tuplebuf because attrs[] will point back into the current content.
	 */
	newtup = heap_form_tuple(desc, attrs, isnull);
	Assert(change->newtuple->tuple.t_len <= MaxHeapTupleSize);
	Assert(&change->newtuple->header == change->newtuple->tuple.t_data);

	memcpy(change->newtuple->tuple.t_data,
		   newtup->t_data,
		   newtup->t_len);
	change->newtuple->tuple.t_len = newtup->t_len;

	/*
	 * free resources we won't further need, more persistent stuff will be
	 * free'd in ReorderBufferToastReset().
	 */
	RelationClose(toast_rel);
	pfree(newtup);
	for (natt = 0; natt < desc->natts; natt++)
	{
		if (free[natt])
			pfree(DatumGetPointer(attrs[natt]));
	}
	pfree(attrs);
	pfree(free);
	pfree(isnull);

}

/*
 * Free all resources allocated for toast reconstruction.
 */
static void
ReorderBufferToastReset(ReorderBuffer * buffer, ReorderBufferTXN * txn)
{
	HASH_SEQ_STATUS hstat;
	ReorderBufferToastEnt *ent;

	if (txn->toast_hash == NULL)
		return;

	/* sequentially walk over the hash and free everything */
	hash_seq_init(&hstat, txn->toast_hash);
	while ((ent = (ReorderBufferToastEnt *) hash_seq_search(&hstat)) != NULL)
	{
		dlist_mutable_iter it;

		if (ent->reconstructed != NULL)
			pfree(ent->reconstructed);

		dlist_foreach_modify(it, &ent->chunks)
		{
			ReorderBufferChange *change =
			dlist_container(ReorderBufferChange, node, it.cur);

			dlist_delete(&change->node);
			ReorderBufferReturnChange(buffer, change);
		}
	}

	hash_destroy(txn->toast_hash);
}


/*
 * Visibility support routines
 */

/*-------------------------------------------------------------------------
 * Lookup actual cmin/cmax values during timetravel access. We can't always
 * rely on stored cmin/cmax values because of two scenarios:
 *
 * * A tuple got changed multiple times during a single transaction and thus
 *	 has got a combocid. Combocid's are only valid for the duration of a single
 *	 transaction.
 * * A tuple with a cmin but no cmax (and thus no combocid) got deleted/updated
 *	 in another transaction than the one which created it which we are looking
 *	 at right now. As only one of cmin, cmax or combocid is actually stored in
 *	 the heap we don't have access to the the value we need anymore.
 *
 * To resolve those problems we have a per-transaction hash of (cmin, cmax)
 * tuples keyed by (relfilenode, ctid) which contains the actual (cmin, cmax)
 * values. That also takes care of combocids by simply not caring about them at
 * all. As we have the real cmin/cmax values thats enough.
 *
 * As we only care about catalog tuples here the overhead of this hashtable
 * should be acceptable.
 * -------------------------------------------------------------------------
 */
extern bool
ResolveCminCmaxDuringDecoding(HTAB *tuplecid_data,
							  HeapTuple htup, Buffer buffer,
							  CommandId *cmin, CommandId *cmax)
{
	ReorderBufferTupleCidKey key;
	ReorderBufferTupleCidEnt *ent;
	ForkNumber	forkno;
	BlockNumber blockno;

	/* be careful about padding */
	memset(&key, 0, sizeof(key));

	Assert(!BufferIsLocal(buffer));

	/*
	 * get relfilenode from the buffer, no convenient way to access it other
	 * than that.
	 */
	BufferGetTag(buffer, &key.relnode, &forkno, &blockno);

	/* tuples can only be in the main fork */
	Assert(forkno == MAIN_FORKNUM);
	Assert(blockno == ItemPointerGetBlockNumber(&htup->t_self));

	ItemPointerCopy(&htup->t_self,
					&key.tid);

	ent = (ReorderBufferTupleCidEnt *)
		hash_search(tuplecid_data,
					(void *) &key,
					HASH_FIND,
					NULL);

	if (ent == NULL)
		return false;

	if (cmin)
		*cmin = ent->cmin;
	if (cmax)
		*cmax = ent->cmax;
	return true;
}
