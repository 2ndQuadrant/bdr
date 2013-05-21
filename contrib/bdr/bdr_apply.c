/* -------------------------------------------------------------------------
 *
 * bdr_apply.c
 *		Replication!!!
 *
 * Replication???
 *
 * Copyright (C) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr_apply.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"

#include "pgstat.h"

#include "access/committs.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "executor/spi.h"
#include "executor/executor.h"

#include "parser/parse_relation.h"

#include "replication/logical.h"
#include "replication/replication_identifier.h"

#include "storage/bufmgr.h"

#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

static HeapTuple ExtractKeyTuple(Relation rel, Relation idx_rel, HeapTuple tp);
static void build_scan_key(ScanKey skey, Relation rel, Relation idx_rel, HeapTuple key);
static bool find_pkey_tuple(ScanKey skey, Relation rel, Relation idx_rel, ItemPointer tid, bool lock);
static void UserTableUpdateIndexes(Relation rel, HeapTuple tuple);
static char *read_tuple(char *data, size_t len, HeapTuple tuple, Oid *reloid);
static void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple);

static void check_sequencer_wakeup(Relation rel);

bool request_sequencer_wakeup = false;

void
process_remote_begin(char *data, size_t r)
{
	XLogRecPtr *origlsn;
	TimestampTz *committime;
	TimestampTz current;
	char	    statbuf[100];
	Assert(bdr_apply_con != NULL);

	origlsn = (XLogRecPtr *) data;
	data += sizeof(XLogRecPtr);

	committime = (TimestampTz *) data;
	data += sizeof(TimestampTz);

	/* setup state for commit and conflict detection */
	replication_origin_lsn = *origlsn;
	replication_origin_timestamp = *committime;

	snprintf(statbuf, sizeof(statbuf),
			"bdr_apply: BEGIN origin(source, orig_lsn, timestamp): %s, %X/%X, %s",
			 bdr_apply_con->name,
			(uint32) (*origlsn >> 32), (uint32) *origlsn,
			timestamptz_to_str(*committime));

	elog(LOG, "%s", statbuf);

	pgstat_report_activity(STATE_RUNNING, statbuf);

	/* don't want the overhead otherwise */
	if (bdr_apply_con->apply_delay > 0)
	{
		current = GetCurrentTimestamp();
#ifndef HAVE_INT64_TIMESTAMP
#error "we require integer timestamps"
#endif
		/* ensure no weirdness due to clock drift */
		if (current > replication_origin_timestamp)
		{
			long		sec;
			int			usec;

			current = TimestampTzPlusMilliseconds(current, -bdr_apply_con->apply_delay);

			TimestampDifference(current, replication_origin_timestamp,
								&sec, &usec);
			/* FIXME: deal with overflow? */
			pg_usleep(usec + (sec * USECS_PER_SEC));
		}
	}

	request_sequencer_wakeup = false;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
}

void
process_remote_commit(char *data, size_t r)
{
	XLogRecPtr *origlsn;
	TimestampTz *committime;

	origlsn = (XLogRecPtr *) data;
	data += sizeof(XLogRecPtr);

	committime = (TimestampTz *) data;
	data += sizeof(TimestampTz);

	elog(LOG, "COMMIT origin(lsn, timestamp): %X/%X, %s",
		 (uint32) (*origlsn >> 32), (uint32) *origlsn,
		 timestamptz_to_str(*committime));

	Assert(*origlsn == replication_origin_lsn);
	Assert(*committime == replication_origin_timestamp);

	PopActiveSnapshot();
	CommitTransactionCommand();

	pgstat_report_activity(STATE_IDLE, NULL);

	AdvanceCachedReplicationIdentifier(*origlsn, XactLastCommitEnd);

	CurrentResourceOwner = bdr_saved_resowner;

	bdr_count_commit();

	if (request_sequencer_wakeup)
	{
		request_sequencer_wakeup = false;
		bdr_sequencer_wakeup();
	}
}


void
process_remote_insert(char *data, size_t r)
{
#ifdef VERBOSE_INSERT
	StringInfoData s;
#endif
	char		action;
	HeapTupleData tup;
	Oid			reloid;
	Relation	rel;

	action = data[0];
	data++;

	if (action != 'N')
		elog(ERROR, "expected new tuple but got %c",
			 action);

	data = read_tuple(data, r, &tup, &reloid);

	rel = heap_open(reloid, RowExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	simple_heap_insert(rel, &tup);
	UserTableUpdateIndexes(rel, &tup);
	bdr_count_insert();

	check_sequencer_wakeup(rel);

	/* debug output */
#if VERBOSE_INSERT
	initStringInfo(&s);
	tuple_to_stringinfo(&s, RelationGetDescr(rel), &tup);
	elog(LOG, "INSERT: %s", s.data);
	resetStringInfo(&s);
#endif

	heap_close(rel, NoLock);
}

static void
fetch_sysid_via_node_id(RepNodeId node_id, uint64 *sysid, TimeLineID *tli)
{
	if (node_id == InvalidRepNodeId)
	{
		*sysid = GetSystemIdentifier();
		*tli = ThisTimeLineID;
	}
	else
	{
		HeapTuple	node;
		Form_pg_replication_identifier node_class;

		node = GetReplicationInfoByIdentifier(node_id);
		if (!HeapTupleIsValid(node))
		{
			Assert(false);
			elog(ERROR, "could not find replication identifier %u?", node_id);
		}

		node_class = (Form_pg_replication_identifier) GETSTRUCT(node);

		if (sscanf(NameStr(node_class->riremotesysid),
				   UINT64_FORMAT "-%u",
				   sysid, tli) != 2)
			elog(ERROR, "could not parse sysid: %s",
				 NameStr(node_class->riremotesysid));
		ReleaseSysCache(node);
	}
}

void
process_remote_update(char *data, size_t r)
{
	StringInfoData s_key;
	char		action;
	HeapTupleData old_key;
	HeapTupleData new_tuple;
	Oid			reloid;
	Oid			idxoid = InvalidOid;
	HeapTuple	generated_key = NULL;
	ItemPointerData oldtid;
	Relation	rel;
	Relation	idxrel;
	bool		found_old;
	ScanKeyData skey[INDEX_MAX_KEYS];
	bool		primary_key_changed = false;

	action = data[0];
	data++;

	/* old key present, identifying key changed */
	if (action == 'K')
	{
		data = read_tuple(data, r, &old_key, &idxoid);
		action = data[0];
		data++;
		primary_key_changed = idxoid != InvalidOid;
	}
	else if (action != 'N')
		elog(ERROR, "expected action 'N' or 'K', got %c",
			 action);

	/* check for new  tuple */
	if (action != 'N')
		elog(ERROR, "expected action 'N', got %c",
			 action);

	/* read new tuple */
	data = read_tuple(data, r, &new_tuple, &reloid);

	/* collected all data, lookup table definition */
	rel = heap_open(reloid, RowExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	/*
	 * if there's no separate primary key (i.e. pkey hasn't changed), extract
	 * pkey from the new tuple so we can find the old version of the row.
	 */
	if (!primary_key_changed)
	{
		if (rel->rd_indexvalid == 0)
			RelationGetIndexList(rel);
		idxoid = rel->rd_primary;

		if (!OidIsValid(idxoid))
		{
			elog(ERROR, "could not find primary key for table with oid %u",
				 RelationGetRelid(rel));
			return;
		}

		/* open index, so we can build scan key for row */
		idxrel = index_open(idxoid, RowExclusiveLock);

		generated_key = ExtractKeyTuple(rel, idxrel, &new_tuple);
		old_key.t_data = generated_key->t_data;
		old_key.t_len = generated_key->t_len;
	}
	else
	{
		/* open index, so we can build scan key for row */
		idxrel = index_open(idxoid, RowExclusiveLock);

#ifdef USE_ASSERT_CHECKING
		RelationGetIndexList(rel);
		Assert(idxoid == rel->rd_primary);
#endif
	}

	Assert(idxrel->rd_index->indisunique);

	build_scan_key(skey, rel, idxrel, &old_key);

	/* look for tuple identified by the (old) primary key */
	found_old = find_pkey_tuple(skey, rel, idxrel, &oldtid, true);

	if (found_old)
	{
		HeapTupleData oldtuple;
		Buffer		buf;
		bool		found;
		TransactionId xmin;
		TimestampTz ts;
		RepNodeId	local_node_id;
		bool		apply_update;
		bool		log_update;

		uint64		local_sysid,
					remote_sysid;
		TimeLineID	local_tli,
					remote_tli;
		CommitExtraData local_node_id_raw;

		ItemPointerCopy(&oldtid, &oldtuple.t_self);

		/* refetch tuple, check for old commit ts & origin */
		found = heap_fetch(rel, SnapshotAny, &oldtuple, &buf, false, NULL);
		if (!found)
			elog(ERROR, "could not refetch tuple %u/%u, relation %u",
				 ItemPointerGetBlockNumber(&oldtid),
				 ItemPointerGetOffsetNumber(&oldtid),
				 RelationGetRelid(rel));
		xmin = HeapTupleHeaderGetXmin(oldtuple.t_data);
		ReleaseBuffer(buf);

		/*
		 * We now need to determine whether to keep the original version of the
		 * row, or apply the update we received.  We use the last-update-wins
		 * strategy for this, except when the new update comes from the same
		 * node that originated the previous version of the tuple.
		 */
		TransactionIdGetCommitTsData(xmin, &ts, &local_node_id_raw);
		local_node_id = local_node_id_raw;

		if (local_node_id == bdr_apply_con->origin_id)
		{
			/*
			 * If the row got updated twice within a single node, just apply
			 * the update with no conflict.  Don't warn/log either, regardless
			 * of the timing; that's just too common and valid since normal row
			 * level locking guarantees are met.
			 */
			apply_update = true;
			log_update = false;
		}
		else
		{
			int		cmp;

			/*
			 * Decide what update wins based on transaction timestamp difference.
			 * The later transaction wins.  If the timestamps compare equal,
			 * use sysid + TLI to discern.
			 */

			cmp = timestamptz_cmp_internal(replication_origin_timestamp, ts);

			if (cmp > 0)
			{
				apply_update = true;
				log_update = false;
			}
			else if (cmp == 0)
			{
				log_update = true;

				fetch_sysid_via_node_id(local_node_id,
										&local_sysid, &local_tli);
				fetch_sysid_via_node_id(bdr_apply_con->origin_id,
										&remote_sysid, &remote_tli);

				if (local_sysid < remote_sysid)
					apply_update = true;
				else if (local_sysid > remote_sysid)
					apply_update = false;
				else if (local_tli < remote_tli)
					apply_update = true;
				else if (local_tli > remote_tli)
					apply_update = false;
				else
					/* shouldn't happen */
					elog(ERROR, "unsuccessful node comparison");
			}
			else
			{
				apply_update = false;
				log_update = true;
			}
		}

		if (log_update)
		{
			char		remote_ts[MAXDATELEN + 1];
			char		local_ts[MAXDATELEN + 1];

			fetch_sysid_via_node_id(local_node_id,
									&local_sysid, &local_tli);
			fetch_sysid_via_node_id(bdr_apply_con->origin_id,
									&remote_sysid, &remote_tli);
			Assert(remote_sysid == bdr_apply_con->sysid);
			Assert(remote_tli == bdr_apply_con->timeline);

			memcpy(remote_ts, timestamptz_to_str(replication_origin_timestamp),
				   MAXDATELEN);
			memcpy(local_ts, timestamptz_to_str(ts),
				   MAXDATELEN);

			initStringInfo(&s_key);
			tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), &old_key);

			ereport(LOG,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("CONFLICT: %s remote update originating at node " UINT64_FORMAT ":%u at ts %s; row was previously updated at %s node " UINT64_FORMAT ":%u at ts %s. PKEY:%s",
							apply_update ? "applying" : "skipping",
							remote_sysid, remote_tli, remote_ts,
					  local_node_id == InvalidRepNodeId ? "local" : "remote",
							local_sysid, local_tli, local_ts, s_key.data)));
			resetStringInfo(&s_key);
		}

		if (apply_update)
		{
			simple_heap_update(rel, &oldtid, &new_tuple);
			/* FIXME: HOT support */
			UserTableUpdateIndexes(rel, &new_tuple);
			bdr_count_update();
		}
		else
			bdr_count_update_conflict();
	}
	else
	{
		initStringInfo(&s_key);
		tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), &old_key);
		bdr_count_update_conflict();

		ereport(ERROR,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: could not find existing tuple for pkey %s", s_key.data)));
		/* XXX dead code */
		resetStringInfo(&s_key);
		goto err;
	}

err:
	if (!primary_key_changed && generated_key != NULL)
		heap_freetuple(generated_key);

	check_sequencer_wakeup(rel);

	/* release locks upon commit */
	index_close(idxrel, NoLock);
	heap_close(rel, NoLock);
}

void
process_remote_delete(char *data, size_t r)
{
#ifdef VERBOSE_DELETE
	StringInfoData s;
#endif
	char		action;

	Oid			idxoid;
	HeapTupleData old_key;
	Relation	rel;
	Relation	idxrel;
	ScanKeyData skey[INDEX_MAX_KEYS];
	bool		found_old;
	ItemPointerData oldtid;

	action = data[0];
	data++;

	if (action == 'E')
	{
		elog(WARNING, "got delete without pkey");
		return;
	}
	else if (action != 'K')
		elog(ERROR, "expected action K got %c", action);

	data = read_tuple(data, r, &old_key, &idxoid);

	/* FIXME: may not open relations in that order */
	idxrel = index_open(idxoid, RowExclusiveLock);

	rel = heap_open(idxrel->rd_index->indrelid, RowExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	build_scan_key(skey, rel, idxrel, &old_key);

	/* try to find tuple via a (candidate|primary) key */
	found_old = find_pkey_tuple(skey, rel, idxrel, &oldtid, true);

	if (found_old)
	{
		simple_heap_delete(rel, &oldtid);
		bdr_count_delete();

	}
	else
	{
		StringInfoData s_key;

		bdr_count_delete_conflict();

		initStringInfo(&s_key);
		tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), &old_key);

		ereport(ERROR,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: DELETE could not find existing tuple for pkey %s", s_key.data)));
		resetStringInfo(&s_key);
	}

#if VERBOSE_DELETE
	initStringInfo(&s);
	tuple_to_stringinfo(&s, RelationGetDescr(idxrel), &old_key);
	elog(LOG, "DELETE old-key: %s", s.data);
	resetStringInfo(&s);
#endif

	check_sequencer_wakeup(rel);

	index_close(idxrel, NoLock);
	heap_close(rel, NoLock);
}

static void
check_sequencer_wakeup(Relation rel)
{
	if (strcmp(RelationGetRelationName(rel), "bdr_sequence_values") == 0 ||
		strcmp(RelationGetRelationName(rel), "bdr_sequence_elections") == 0 ||
		strcmp(RelationGetRelationName(rel), "bdr_votes") == 0)
		request_sequencer_wakeup = true;
}

/*
 * Converts an int64 from network byte order to native format.
 *
 * FIXME: replace with pg_getmsgint64
 */
static int64
recvint64(char *buf)
{
	int64		result;
	uint32		h32;
	uint32		l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}

/*
 * Read a tuple specification from the given data of the given len, filling
 * the HeapTuple with it.  Also, reloid is set to the OID of the relation
 * that this tuple is related to.  (The passed data contains schema and
 * relation names; they are resolved to the corresponding local OID.)
 */
static char *
read_tuple(char *data, size_t len, HeapTuple tuple, Oid *reloid)
{
	int64		relnamelen;
	char	   *relname;
	Oid			relid;
	int64		nspnamelen;
	char	   *nspname;
	int64		tuplelen;
	Oid			nspoid;
	char		t;

	*reloid = InvalidOid;

	/* FIXME: unaligned data accesses */
	t = data[0];
	data += 1;
	if (t != 'T')
		elog(ERROR, "expected TUPLE, got %c", t);

	nspnamelen = recvint64(&data[0]);
	data += 8;
	nspname = data;
	data += nspnamelen;

	relnamelen = recvint64(&data[0]);
	data += 8;
	relname = data;
	data += relnamelen;

	tuplelen = recvint64(&data[0]);
	data += 8;

	tuple->t_data = (HeapTupleHeader) data;
	tuple->t_len = tuplelen;
	data += tuplelen;

	/* resolve the names into a relation OID */
	nspoid = get_namespace_oid(nspname, false);
	relid = get_relname_relid(relname, nspoid);
	if (relid == InvalidOid)
		elog(ERROR, "could not resolve relation name %s.%s", nspname, relname);

	*reloid = relid;

	return data;
}

/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
	int			natt;
	Oid			oid;

	/* print oid of tuple, it's not included in the TupleDesc */
	if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid)
	{
		appendStringInfo(s, " oid[oid]:%u", oid);
	}

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		HeapTuple	type_tuple; /* information about a type */
		Form_pg_type type_form;
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		Datum		val;		/* definitely detoasted Datum */
		char	   *outputstr = NULL;
		bool		isnull;		/* column is null? */

		attr = tupdesc->attrs[natt];

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* gather type name */
		type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);
		type_form = (Form_pg_type) GETSTRUCT(type_tuple);

		/* print attribute name */
		appendStringInfoChar(s, ' ');
		appendStringInfoString(s, NameStr(attr->attname));

		/* print attribute type */
		appendStringInfoChar(s, '[');
		appendStringInfoString(s, NameStr(type_form->typname));
		appendStringInfoChar(s, ']');

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		ReleaseSysCache(type_tuple);

		/* get Datum from tuple */
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull)
			outputstr = "(null)";
		else if (typisvarlena && VARATT_IS_EXTERNAL_TOAST(origval))
			outputstr = "(unchanged-toast-datum)";
		else if (typisvarlena)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;

		/* print data */
		if (outputstr == NULL)
			outputstr = OidOutputFunctionCall(typoutput, val);

		appendStringInfoChar(s, ':');
		appendStringInfoString(s, outputstr);
	}
}


/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
typedef struct ResultRelInfo *UserTableIndexState;

static void
UserTableUpdateIndexes(Relation rel, HeapTuple tuple)
{
	/* this is largely copied together from copy.c's CopyFrom */
	EState	   *estate = CreateExecutorState();
	ResultRelInfo *resultRelInfo;
	List	   *recheckIndexes = NIL;
	TupleDesc	tupleDesc = RelationGetDescr(rel);

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = rel;
	resultRelInfo->ri_TrigInstrument = NULL;

	ExecOpenIndices(resultRelInfo);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	if (resultRelInfo->ri_NumIndices > 0)
	{
		TupleTableSlot *slot = ExecInitExtraTupleSlot(estate);

		ExecSetSlotDescriptor(slot, tupleDesc);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		recheckIndexes = ExecInsertIndexTuples(slot, &tuple->t_self,
											   estate);
	}

	ExecResetTupleTable(estate->es_tupleTable, false);

	ExecCloseIndices(resultRelInfo);

	FreeExecutorState(estate);
	/* FIXME: recheck the indexes */
	list_free(recheckIndexes);
}

/*
 *
 */
static HeapTuple
ExtractKeyTuple(Relation relation, Relation idx_rel, HeapTuple tp)
{
	HeapTuple	idx_tuple = NULL;
	TupleDesc	desc = RelationGetDescr(relation);
	TupleDesc	idx_desc;
	Datum		idx_vals[INDEX_MAX_KEYS];
	bool		idx_isnull[INDEX_MAX_KEYS];
	int			natt;


	idx_rel = RelationIdGetRelation(relation->rd_primary);
	idx_desc = RelationGetDescr(idx_rel);

	for (natt = 0; natt < idx_desc->natts; natt++)
	{
		int			attno = idx_rel->rd_index->indkey.values[natt];

		if (attno == ObjectIdAttributeNumber)
		{
			idx_vals[natt] = HeapTupleGetOid(tp);
			idx_isnull[natt] = false;
		}
		else
		{
			/* FIXME: heap_deform_tuple */
			idx_vals[natt] =
				fastgetattr(tp, attno, desc, &idx_isnull[natt]);
		}
		Assert(!idx_isnull[natt]);
	}
	idx_tuple = heap_form_tuple(idx_desc, idx_vals, idx_isnull);
	RelationClose(idx_rel);

	return idx_tuple;
}

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'idxrel' (*NOT* rel).
 */
static void
build_scan_key(ScanKey skey, Relation rel, Relation idxrel, HeapTuple key)
{
	int			attoff;
	Datum		indclassDatum;
	Datum		indkeyDatum;
	bool		isnull;
	oidvector  *opclass;
	int2vector  *indkey;

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	indkeyDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indkey, &isnull);
	Assert(!isnull);
	indkey = (int2vector *) DatumGetPointer(indkeyDatum);


	for (attoff = 0; attoff < RelationGetNumberOfAttributes(idxrel); attoff++)
	{
		Oid			operator;
		Oid			opfamily;
		RegProcedure regop;
		int			pkattno = attoff + 1;
		int			mainattno = indkey->values[attoff];
		Oid			atttype = attnumTypeId(rel, mainattno);
		Oid			optype = get_opclass_input_type(opclass->values[attoff]);

		opfamily = get_opclass_family(opclass->values[attoff]);


		operator = get_opfamily_member(opfamily, optype,
									   optype,
									   BTEqualStrategyNumber);

		if (!OidIsValid(operator))
			elog(ERROR, "could not lookup equality operator for type %u, optype %u in opfamily %u",
				 atttype, optype, opfamily);

		regop = get_opcode(operator);

		/*
		 * FIXME: deform index tuple instead of fastgetattr'ing everything
		 */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					fastgetattr(key, pkattno,
								RelationGetDescr(idxrel), &isnull));
		Assert(!isnull);
	}
}

/*
 * Search the index 'idxrel' for a tuple identified by 'skey' in 'rel'.
 *
 * If a matching tuple is found setup 'tid' to point to it and return true,
 * false is returned otherwise.
 */
static bool
find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel,
				ItemPointer tid, bool lock)
{
	HeapTuple	tuple;
	bool		found = false;
	IndexScanDesc scan;
	Snapshot snap = GetActiveSnapshot();

	/*
	 * XXX: should we use a different snapshot here to be able to get more
	 * information about concurrent activity? For now we use a snapshot
	 * isolation snapshot...
	 */

	scan = index_beginscan(rel, idxrel,
						   snap,
						   RelationGetNumberOfAttributes(idxrel),
						   0);
	index_rescan(scan, skey, RelationGetNumberOfAttributes(idxrel), NULL, 0);

	while ((tuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		if (found)
			elog(ERROR, "WTF, more than one tuple found via pk???");
		found = true;
		ItemPointerCopy(&tuple->t_self, tid);
	}

	index_endscan(scan);

	if (lock && found)
	{
		Buffer buf;
		HeapUpdateFailureData hufd;
		HTSU_Result res;
		HeapTupleData locktup;
		ItemPointerCopy(tid, &locktup.t_self);

		res = heap_lock_tuple(rel, &locktup, snap->curcid, LockTupleExclusive,
							  false /* wait */,
							  false /* don't follow updates */,
							  &buf, &hufd);
		switch (res)
		{
			case HeapTupleMayBeUpdated:
				break;
			case HeapTupleUpdated:
				/* XXX: Improve handling here */
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("concurrent update, retrying")));
			default:
				elog(ERROR, "unexpected HTSU_Result after locking: %u", res);
				break;
		}
		ReleaseBuffer(buf);
	}
	return found;
}
