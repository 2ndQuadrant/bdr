/*-------------------------------------------------------------------------
 *
 * seqam.h
 *	  Public header file for Sequence access method.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/seqam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQAM_H
#define SEQAM_H

#include "fmgr.h"

#include "access/htup.h"
#include "commands/sequence.h"
#include "utils/relcache.h"
#include "storage/buf.h"
#include "storage/bufpage.h"

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 */
typedef struct SeqTableData
{
	Oid			relid;			/* pg_class OID of this sequence (hash key) */
	Oid			filenode;		/* last seen relfilenode of this sequence */
	LocalTransactionId lxid;	/* xact in which we last did a seq op */
	bool		last_valid;		/* do we have a valid "last" value? */
	int64		last;			/* value last returned by nextval */
	int64		cached;			/* last value already cached for nextval */
	/* if last != cached, we have not used up all the cached values */
	int64		increment;		/* copy of sequence's increment field */
	/* note that increment is zero until we first do read_seq_tuple() */
	Datum		am_private;		/* private data of the SeqAm */
} SeqTableData;

typedef SeqTableData *SeqTable;

extern char *default_seqam;

Oid GetDefaultSeqAM(void);

extern void sequence_alloc(Relation seqRelation, SeqTable seq_elem, Buffer buf, HeapTuple tup);
extern void sequence_setval(Relation seqRelation, SeqTable seq_elem, Buffer buf, HeapTuple tup, int64 next, bool iscalled);


extern void sequence_local_alloc(PG_FUNCTION_ARGS);
extern void sequence_local_setval(PG_FUNCTION_ARGS);
extern Datum sequence_local_options(PG_FUNCTION_ARGS);

extern Oid get_seqam_oid(const char *sequencename, bool missing_ok);

extern void log_sequence_tuple(Relation seqrel, HeapTuple tup, Page page);
extern void init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel);
extern Form_pg_sequence read_seq_tuple(SeqTable elm, Relation rel,
			   Buffer *buf, HeapTuple seqtuple);
#endif   /* SEQAM_H */
