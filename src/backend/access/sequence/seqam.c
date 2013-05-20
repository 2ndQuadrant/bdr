/*-------------------------------------------------------------------------
 *
 * seqam.c
 *	  sequence access method routines
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/sequence/seqam.c
 *
 *
 * Sequence access method allows the SQL Standard Sequence objects to be
 * managed according to either the default access method or a pluggable
 * replacement. Each sequence can only use one access method at a time,
 * though different sequence access methods can be in use by different
 * sequences at the same time.
 *
 * The SQL Standard assumes that each Sequence object is completely controlled
 * from the current database node, preventing any form of clustering mechanisms
 * from controlling behaviour. Sequence access methods are general purpose
 * though designed specifically to address the needs of Sequences working as
 * part of a multi-node "cluster", though that is not defined here, nor are
 * there dependencies on anything outside of this module, nor any particular
 * form of clustering.
 *
 * The SQL Standard behaviour, also the historical PostgreSQL behaviour, is
 * referred to as the "Local" SeqAm. That is also the basic default.  Local
 * SeqAm assumes that allocations from the sequence will be contiguous, so if
 * user1 requests a range of values and is given 500-599 as values for their
 * backend then the next user to make a request will be given a range starting
 * with 600.
 *
 * The SeqAm mechanism allows us to override the Local behaviour, for use with
 * clustering systems. When multiple masters can request ranges of values it
 * would break the assumption of contiguous allocation. It seems likely that
 * the SeqAm would also wish to control node-level caches for sequences to
 * ensure good performance.  The CACHE option and other options may be
 * overridden by the _alloc API call, if needed, though in general having
 * cacheing per backend and per node seems desirable.
 *
 * SeqAm allows calls to allocate a new range of values, reset the sequence to
 * a new value and to define options for the AM module.  The on-disk format of
 * Sequences is the same for all AMs, except that each sequence has a SeqAm
 * defined private-data column, am_data.
 *
 * We currently assume that there is no persistent state held within the SeqAm,
 * so it is safe to ALTER the access method of an object without taking any
 * special actions, as long as we hold an AccessExclusiveLock while we do that.
 *
 * SeqAMs work similarly to IndexAMs in many ways. pg_class.relam stores the
 * Oid of the SeqAM, just as we do for IndexAm. The relcache stores AM
 * information in much the same way for indexes and sequences, and management
 * of options is similar also.
 *
 * Note that the SeqAM API calls are synchronous. It is up to the SeqAM to
 * decide how that is handled, for example, whether there is a higher level
 * cache at instance level to amortise network traffic in cluster.
 *
 * The SeqAM is identified by Oid of corresponding tuple in pg_seqam.  There is
 * no syscache for pg_seqam, though the SeqAm data is stored on the relcache
 * entry for the sequence.
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/seqam.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_seqam.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

char	*default_seqam = NULL;

#define GET_SEQAM_PROCEDURE(pname) \
do { \
	procedure = &seqRelation->rd_aminfo->pname; \
	if (!OidIsValid(procedure->fn_oid)) \
	{ \
		RegProcedure	procOid = seqRelation->rd_seqam->pname; \
		if (!RegProcedureIsValid(procOid)) \
			elog(ERROR, "invalid %s regproc", CppAsString(pname)); \
		fmgr_info_cxt(procOid, procedure, seqRelation->rd_indexcxt); \
	} \
} while(0)

/*-------------------------------------------------------------------------
 *
 *  Sequence Access Manager API
 *
 *  INTERFACE ROUTINES
 *		sequence_alloc		- allocate a new range of values for the sequence
 *		sequence_setval		- coordinate the reset of a sequence to new value
 *
 *		sequence_reloptions	- process options - located in reloptions.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * sequence_alloc - allocate sequence values in a sequence
 */
void
sequence_alloc(Relation seqRelation, SeqTable seq_elem, Buffer buf, HeapTuple tup)
{
	FmgrInfo   *procedure;

	Assert(RelationIsValid(seqRelation));
	Assert(PointerIsValid(seqRelation->rd_seqam));
	Assert(OidIsValid(seqRelation->rd_rel->relam));

	GET_SEQAM_PROCEDURE(seqamalloc);

	/*
	 * have the seqam's alloc proc do all the work.
	 */
	FunctionCall4(procedure,
				  PointerGetDatum(seqRelation),
				  PointerGetDatum(seq_elem),
				  Int32GetDatum(buf),
				  PointerGetDatum(tup));
}

/*
 * sequence_setval - set sequence values in a sequence
 */
void
sequence_setval(Relation seqRelation, SeqTable seq_elem, Buffer buf, HeapTuple tup, int64 next, bool iscalled)
{
	FmgrInfo   *procedure;

	Assert(RelationIsValid(seqRelation));
	Assert(PointerIsValid(seqRelation->rd_seqam));
	Assert(OidIsValid(seqRelation->rd_rel->relam));

	GET_SEQAM_PROCEDURE(seqamsetval);

	/*
	 * have the seqam's setval proc do all the work.
	 */
	FunctionCall6(procedure,
				  PointerGetDatum(seqRelation),
				  PointerGetDatum(seq_elem),
				  Int32GetDatum(buf),
				  PointerGetDatum(tup),
				  Int64GetDatum(next),
				  BoolGetDatum(iscalled));
}

/*------------------------------------------------------------
 *
 * Sequence Access Manager management functions
 *
 *------------------------------------------------------------
 */

/* check_hook: validate new default_sequenceam */
bool
check_default_seqam(char **newval, void **extra, GucSource source)
{
	if (**newval == '\0')
		return true;

	/*
	 * If we aren't inside a transaction, we cannot do database access so
	 * cannot verify the name.	Must accept the value on faith.
	 */
	if (IsTransactionState())
	{
		if (!OidIsValid(get_seqam_oid(*newval, true)))
		{
			/*
			 * When source == PGC_S_TEST, we are checking the argument of an
			 * ALTER DATABASE SET or ALTER USER SET command.  Value may
			 * be created later.  Because of that, issue a NOTICE if source ==
			 * PGC_S_TEST, but accept the value anyway.
			 */
			if (source == PGC_S_TEST)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("sequence manager \"%s\" does not exist",
								*newval)));
			}
			else
			{
				GUC_check_errdetail("sequence manager \"%s\" does not exist.",
									*newval);
				return false;
			}
		}
	}
	return true;
}

/*
 * GetDefaultSeqAM -- get the OID of the current default sequence AM
 *
 * This exists to hide (and possibly optimize the use of) the
 * default_seqam GUC variable.
 */
Oid
GetDefaultSeqAM(void)
{
	/* Fast path for default_tablespace == "" */
	if (default_seqam == NULL || default_seqam[0] == '\0')
		return LOCAL_SEQAM_OID;

	return get_seqam_oid(default_seqam, false);
}

/*
 * get_seqam_oid - given a sequence AM name, look up the OID
 *
 * If missing_ok is false, throw an error if SeqAM name not found.  If true,
 * just return InvalidOid.
 */
Oid
get_seqam_oid(const char *amname, bool missing_ok)
{
	Oid			result;
	HeapTuple	tuple;

	/* look up the access method */
	tuple = SearchSysCache1(SEQAMNAME, PointerGetDatum(amname));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("sequence access method \"%s\" does not exist",
						amname)));

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
	{
		result = HeapTupleGetOid(tuple);
		ReleaseSysCache(tuple);
	}
	else
		result = InvalidOid;

	if (!OidIsValid(result) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("squence am \"%s\" does not exist",
						amname)));
	return result;
}
