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

#include "libpq/pqformat.h"

#include "parser/parse_relation.h"

#include "replication/logical.h"
#include "replication/replication_identifier.h"

#include "storage/bufmgr.h"

#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/* Useful for development:
#define VERBOSE_INSERT
#define VERBOSE_DELETE
#define VERBOSE_UPDATE
*/

typedef struct BDRTupleData
{
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	bool		changed[MaxTupleAttributeNumber];
} BDRTupleData;

static void build_scan_key(ScanKey skey, Relation rel, Relation idx_rel, HeapTuple key);
static HeapTuple find_pkey_tuple(ScanKey skey, Relation rel, Relation idx_rel, ItemPointer tid, bool lock);
static void UserTableUpdateIndexes(Relation rel, HeapTuple tuple);
static Relation read_rel(StringInfo s, LOCKMODE mode);
extern void read_tuple_parts(StringInfo s, Relation rel, BDRTupleData *tup);
static HeapTuple read_tuple(StringInfo s, Relation rel);
static void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple);

static void check_sequencer_wakeup(Relation rel);

bool request_sequencer_wakeup = false;
Oid			QueuedDDLCommandsRelid = InvalidOid;

void
process_remote_begin(StringInfo s)
{
	XLogRecPtr		origlsn;
	TimestampTz		committime;
	TimestampTz		current;
	char	    	statbuf[100];

	Assert(bdr_apply_con != NULL);

	origlsn = pq_getmsgint64(s);
	committime = pq_getmsgint64(s);

	/* setup state for commit and conflict detection */
	replication_origin_lsn = origlsn;
	replication_origin_timestamp = committime;

	snprintf(statbuf, sizeof(statbuf),
			"bdr_apply: BEGIN origin(source, orig_lsn, timestamp): %s, %X/%X, %s",
			 bdr_apply_con->name,
			(uint32) (origlsn >> 32), (uint32) origlsn,
			timestamptz_to_str(committime));

	elog(LOG, "%s", statbuf);

	pgstat_report_activity(STATE_RUNNING, statbuf);

	/* don't want the overhead otherwise */
	if (bdr_apply_con->apply_delay > 0)
	{
		current = GetCurrentIntegerTimestamp();

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
process_remote_commit(StringInfo s)
{
	XLogRecPtr		origlsn;
	TimestampTz		committime;
	TimestampTz		end_lsn;

	Assert(bdr_apply_con != NULL);

	origlsn = pq_getmsgint64(s);
	end_lsn = pq_getmsgint64(s);
	committime = pq_getmsgint64(s);

	elog(LOG, "COMMIT origin(lsn, end, timestamp): %X/%X, %X/%X, %s",
		 (uint32) (origlsn >> 32), (uint32) origlsn,
		 (uint32) (end_lsn >> 32), (uint32) end_lsn,
		 timestamptz_to_str(committime));

	Assert(origlsn == replication_origin_lsn);
	Assert(committime == replication_origin_timestamp);

	PopActiveSnapshot();
	CommitTransactionCommand();

	pgstat_report_activity(STATE_IDLE, NULL);

	AdvanceCachedReplicationIdentifier(end_lsn, XactLastCommitEnd);

	CurrentResourceOwner = bdr_saved_resowner;

	bdr_count_commit();

	if (request_sequencer_wakeup)
	{
		request_sequencer_wakeup = false;
		bdr_sequencer_wakeup();
	}
}

static HeapTuple
process_queued_ddl_command(HeapTuple cmdtup)
{
	Relation	cmdsrel;
	HeapTuple	newtup;
	Datum		datum;
	char	   *type;
	char	   *identstr;
	char	   *cmdstr;
	bool		isnull;

	cmdsrel = heap_open(QueuedDDLCommandsRelid, AccessShareLock);

	/* fetch the object type */
	datum = heap_getattr(cmdtup, 1,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
	{
		elog(LOG, "null object type in command tuple in \"%s\"",
			 RelationGetRelationName(cmdsrel));
		return cmdtup;
	}
	type = TextDatumGetCString(datum);

	/* fetch the object identity */
	datum = heap_getattr(cmdtup, 2,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
	{
		elog(WARNING, "null identity in command tuple for object of type %s",
			 RelationGetRelationName(cmdsrel));
		return cmdtup;
	}
	identstr = TextDatumGetCString(datum);
	elog(LOG, "got queued command for %s: \"%s\"", type, identstr);

	/* finally fetch and execute the command */
	datum = heap_getattr(cmdtup, 3,
						 RelationGetDescr(cmdsrel),
						 &isnull);
	if (isnull)
	{
		elog(LOG, "null command in tuple for %s \"%s\"", type, identstr);
		return cmdtup;
	}
	cmdstr = TextDatumGetCString(datum);

	/* do the SPI dance */
	{
		int		ret;

		/*
		 * XXX it might be wise to establish a savepoint here, to avoid
		 * a larger problem in case the command fails; at the very least
		 * we still need to process the original insertion.
		 */
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		ret = SPI_execute(cmdstr, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(LOG, "SPI_execute failed");

		SPI_finish();
		PopActiveSnapshot();
	}

	/* set "executed" true */
	// newtup = heap_modify_tuple( .. );
	newtup = cmdtup;

	pfree(identstr);
	heap_close(cmdsrel, AccessShareLock);

	return newtup;
}

void
process_remote_insert(StringInfo s)
{
#ifdef VERBOSE_INSERT
	StringInfoData o;
#endif
	char		action;
	HeapTuple	tup;
	Relation	rel;

	rel = read_rel(s, RowExclusiveLock);

	action = pq_getmsgbyte(s);
	if (action != 'N')
		elog(ERROR, "expected new tuple but got %d",
			 action);

	tup = read_tuple(s, rel);

	if (RelationGetRelid(rel) == QueuedDDLCommandsRelid)
		tup = process_queued_ddl_command(tup);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	simple_heap_insert(rel, tup);
	UserTableUpdateIndexes(rel, tup);
	bdr_count_insert();

	check_sequencer_wakeup(rel);

	/* debug output */
#ifdef VERBOSE_INSERT
	initStringInfo(&o);
	tuple_to_stringinfo(&o, RelationGetDescr(rel), tup);
	elog(LOG, "INSERT: %s", o.data);
	resetStringInfo(&o);
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
		char *ident;

		uint64 remote_sysid;
		Oid remote_dboid;
		TimeLineID remote_tli;
		Oid local_dboid;
		NameData replication_name;

		node = GetReplicationInfoByIdentifier(node_id, false);

		node_class = (Form_pg_replication_identifier) GETSTRUCT(node);

		ident = text_to_cstring(&node_class->riname);

		if (sscanf(ident, "bdr: "UINT64_FORMAT"-%u-%u-%u:%s",
				   &remote_sysid, &remote_tli, &remote_dboid, &local_dboid, NameStr(replication_name)) != 4)
			elog(ERROR, "could not parse sysid: %s", ident);
		ReleaseSysCache(node);
		pfree(ident);
	}
}

void
process_remote_update(StringInfo s)
{
	StringInfoData s_key;
	char		action;
	HeapTuple	old_key;
	HeapTuple	old_tuple;
	BDRTupleData new_tuple;
	Oid			idxoid;
	HeapTuple	generated_key = NULL;
	ItemPointerData oldtid;
	Relation	rel;
	Relation	idxrel;
	ScanKeyData skey[INDEX_MAX_KEYS];
	bool		primary_key_changed = false;

	rel = read_rel(s, RowExclusiveLock);

	action = pq_getmsgbyte(s);

	/* old key present, identifying key changed */
	if (action != 'K' && action != 'N')
		elog(ERROR, "expected action 'N' or 'K', got %c",
			 action);

	if (action == 'K')
	{
		old_key = read_tuple(s, rel);
		action = pq_getmsgbyte(s);
		primary_key_changed = true;;
	}

	/* check for new  tuple */
	if (action != 'N')
		elog(ERROR, "expected action 'N', got %c",
			 action);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	/* read new tuple */
	read_tuple_parts(s, rel, &new_tuple);

	/*
	 * Check which tuple we want to use for the pkey lookup.
	 */
	if (!primary_key_changed)
	{
		/* key hasn't changed, just use columns from the new tuple */
		old_key = heap_form_tuple(RelationGetDescr(rel),
								  new_tuple.values, new_tuple.isnull);

	}

	/* lookup index to build scankey */
	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);
	idxoid = rel->rd_replidindex;
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(rel));
		return;
	}

	/* open index, so we can build scan key for row */
	idxrel = index_open(idxoid, RowExclusiveLock);

	Assert(idxrel->rd_index->indisunique);

	build_scan_key(skey, rel, idxrel, old_key);

	/* look for tuple identified by the (old) primary key */
	old_tuple = find_pkey_tuple(skey, rel, idxrel, &oldtid, true);

	if (old_key != NULL)
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
			tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), old_key);

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
			HeapTuple nt;
			Assert(old_tuple != NULL);
			nt = heap_modify_tuple(old_tuple, RelationGetDescr(rel),
								   new_tuple.values, new_tuple.isnull, new_tuple.changed);
			simple_heap_update(rel, &oldtid, nt);
			UserTableUpdateIndexes(rel, nt);
			bdr_count_update();
		}
		else
			bdr_count_update_conflict();
	}
	else
	{
		initStringInfo(&s_key);
		tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), old_key);
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
process_remote_delete(StringInfo s)
{
#ifdef VERBOSE_DELETE
	StringInfoData o;
#endif
	char		action;
	Oid			idxoid;
	HeapTuple	old_key;
	Relation	rel;
	Relation	idxrel;
	ScanKeyData skey[INDEX_MAX_KEYS];
	bool		found_old;
	ItemPointerData oldtid;

	rel = read_rel(s, RowExclusiveLock);

	action = pq_getmsgbyte(s);

	if (action != 'K' && action != 'E')
		elog(ERROR, "expected action K or E got %c", action);

	if (action == 'E')
	{
		elog(WARNING, "got delete without pkey");
		return;
	}

	old_key = read_tuple(s, rel);

	/* lookup index to build scankey */
	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);
	idxoid = rel->rd_replidindex;
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(rel));
		return;
	}

	/* Now open the primary key index */
	idxrel = index_open(idxoid, RowExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' rel \"%s\"",
			 rel->rd_rel->relkind, RelationGetRelationName(rel));

	build_scan_key(skey, rel, idxrel, old_key);

	/* try to find tuple via a (candidate|primary) key */
	found_old = find_pkey_tuple(skey, rel, idxrel, &oldtid, true) != NULL;

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
		tuple_to_stringinfo(&s_key, RelationGetDescr(idxrel), old_key);

		ereport(ERROR,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
				 errmsg("CONFLICT: DELETE could not find existing tuple for pkey %s", s_key.data)));
		resetStringInfo(&s_key);
	}

#ifdef VERBOSE_DELETE
	initStringInfo(&o);
	tuple_to_stringinfo(&o, RelationGetDescr(idxrel), old_key);
	elog(LOG, "DELETE old-key: %s", o.data);
	resetStringInfo(&o);
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

void
read_tuple_parts(StringInfo s, Relation rel, BDRTupleData *tup)
{
	TupleDesc	desc = RelationGetDescr(rel);
	int			i;
	int			rnatts;
	char		action;

	action = pq_getmsgbyte(s);

	if (action != 'T')
		elog(ERROR, "expected TUPLE, got %c", action);

	memset(tup->isnull, 1, sizeof(tup->isnull));
	memset(tup->changed, 1, sizeof(tup->changed));

	rnatts = pq_getmsgint(s, 4);

	if (desc->natts != rnatts)
		elog(ERROR, "tuple natts mismatch, %u vs %u", desc->natts, rnatts);

	/* FIXME: unaligned data accesses */

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = desc->attrs[i];
		char		kind = pq_getmsgbyte(s);
		const char *data;
		int	   		len;

		switch (kind)
		{
			case 'n': /* null */
				/* already marked as null */
				break;
			case 'u': /* unchanged column */
				tup->isnull[i] = false;
				tup->changed[i] = false;
				break;

			case 'b': /* binary format */
				tup->isnull[i] = false;
				len = pq_getmsgint(s, 4); /* read length */

				data = pq_getmsgbytes(s, len);

				/* and data */
				if (att->attbyval)
					tup->values[i] = fetch_att(data, true, len);
				else
					tup->values[i] = PointerGetDatum(data);
				break;
			case 's': /* send/recv format */
				{
					Oid typreceive;
					Oid typioparam;
					StringInfoData buf;

					tup->isnull[i] = false;
					len = pq_getmsgint(s, 4); /* read length */

					getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);

					/* create StringInfo pointing into the bigger buffer */
					initStringInfo(&buf);
					/* and data */
					buf.data = (char *) pq_getmsgbytes(s, len);
					buf.len = len;
					tup->values[i] = OidReceiveFunctionCall(
						typreceive, &buf, typioparam, att->atttypmod);

					if (buf.len != buf.cursor)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
								 errmsg("incorrect binary data format")));
					break;
				}
			case 't': /* text format */
				{
					Oid typinput;
					Oid typioparam;

					tup->isnull[i] = false;
					len = pq_getmsgint(s, 4); /* read length */

					getTypeInputInfo(att->atttypid, &typinput, &typioparam);
					/* and data */
					data = (char *) pq_getmsgbytes(s, len);
					tup->values[i] = OidInputFunctionCall(
						typinput, (char *) data, typioparam, att->atttypmod);
				}
				break;
			default:
				elog(ERROR, "unknown column type '%c'", kind);
		}

		if (att->attisdropped && !tup->isnull[i])
			elog(ERROR, "data for dropped column");
	}
}

static Relation
read_rel(StringInfo s, LOCKMODE mode)
{
	int			relnamelen;
	int			nspnamelen;
	RangeVar*	rv;
	Oid			relid;

	rv = makeNode(RangeVar);

	nspnamelen = pq_getmsgint(s, 2);
	rv->schemaname = (char *) pq_getmsgbytes(s, nspnamelen);

	relnamelen = pq_getmsgint(s, 2);
	rv->relname = (char *) pq_getmsgbytes(s, relnamelen);

	relid = RangeVarGetRelidExtended(rv, mode, false, false, NULL, NULL);

	return heap_open(relid, NoLock);
}

/*
 * Read a tuple from s, return it as a HeapTuple allocated in the current
 * memory context. Also, reloid is set to the OID of the relation that this
 * tuple is related to.(The passed data contains schema and relation names;
 * they are resolved to the corresponding local OID.)
 */
static HeapTuple
read_tuple(StringInfo s, Relation rel)
{
	BDRTupleData tupdata;
	HeapTuple	tup;

	read_tuple_parts(s, rel, &tupdata);
	tup = heap_form_tuple(RelationGetDescr(rel), tupdata.values, tupdata.isnull);
	return tup;
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
		else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
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
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	List	   *recheckIndexes = NIL;
	TupleDesc	tupleDesc = RelationGetDescr(rel);

	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(tuple))
		return;

	estate = CreateExecutorState();

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
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
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

		/* FIXME: convert type? */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					fastgetattr(key, mainattno,
								RelationGetDescr(rel), &isnull));
		if (isnull)
			elog(ERROR, "index tuple with a null column");
	}
}

/*
 * Search the index 'idxrel' for a tuple identified by 'skey' in 'rel'.
 *
 * If a matching tuple is found setup 'tid' to point to it and return true,
 * false is returned otherwise.
 */
static HeapTuple
find_pkey_tuple(ScanKey skey, Relation rel, Relation idxrel,
				ItemPointer tid, bool lock)
{
	HeapTuple	scantuple;
	HeapTuple	tuple = NULL;
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

	while ((scantuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		if (tuple != NULL)
			elog(ERROR, "WTF, more than one tuple found via pk???");
		tuple = heap_copytuple(scantuple);
		ItemPointerCopy(&scantuple->t_self, tid);
	}

	index_endscan(scan);

	if (lock && tuple != NULL)
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

	return tuple;
}
