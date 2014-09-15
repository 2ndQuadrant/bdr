#include "postgres.h"

#include "bdr.h"

#include "funcapi.h"

#include "access/xact.h"

#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#include "commands/sequence.h"

#include "tcop/tcopprot.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/* GUCs */
bool bdr_log_conflicts_to_table = false;
bool bdr_conflict_logging_include_tuples = false;

static Oid BdrConflictTypeOid = InvalidOid;
static Oid BdrConflictResolutionOid = InvalidOid;
static Oid BdrConflictHistorySeqId = InvalidOid;

/*
 * All this code runs only in the context of an apply worker, so
 * we can access the apply worker state global safely
 */
extern BdrApplyWorker *bdr_apply_worker;

#define BDR_CONFLICT_HISTORY_COLS 30
#define SYSID_DIGITS 33

/* We want our own memory ctx to clean up easily & reliably */
MemoryContext conflict_log_context;

/*
 * Perform syscache lookups etc for BDR conflict logging.
 *
 * Must be called during apply worker startup, after schema
 * maintenance.
 *
 * Runs even if !bdr_log_conflicts_to_table as that can be
 * toggled at runtime.
 */
void
bdr_conflict_logging_startup()
{
	Oid schema_oid;

	conflict_log_context = AllocSetContextCreate(CurrentMemoryContext,
		"bdr_log_conflict_ctx", ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

	StartTransactionCommand();

	schema_oid = get_namespace_oid("bdr", false);

	BdrConflictTypeOid = GetSysCacheOidError2(TYPENAMENSP,
		CStringGetDatum("bdr_conflict_type"), ObjectIdGetDatum(schema_oid));

	BdrConflictResolutionOid = GetSysCacheOidError2(TYPENAMENSP,
		CStringGetDatum("bdr_conflict_resolution"),
		ObjectIdGetDatum(schema_oid));

	BdrConflictHistorySeqId = GetSysCacheOidError2(RELNAMENSP,
		CStringGetDatum("bdr_conflict_history_id_seq"),
		ObjectIdGetDatum(schema_oid));

	CommitTransactionCommand();
}

/*
 * Cleanup our memory context.
 */
void
bdr_conflict_logging_cleanup(void)
{
	if (conflict_log_context)
		MemoryContextResetAndDeleteChildren(conflict_log_context);
}


/* Get the enum oid for a given BdrConflictType */
static Datum
bdr_conflict_type_get_datum(BdrConflictType conflict_type)
{
	Oid conflict_type_oid;
	char *enumname = NULL;

	switch(conflict_type)
	{
		case BdrConflictType_InsertInsert:
			enumname = "insert_insert";
			break;
		case BdrConflictType_InsertUpdate:
			enumname = "insert_update";
			break;
		case BdrConflictType_UpdateUpdate:
			enumname = "update_update";
			break;
		case BdrConflictType_UpdateDelete:
			enumname = "update_delete";
			break;
		case BdrConflictType_DeleteDelete:
			enumname = "delete_delete";
			break;
		case BdrConflictType_UnhandledTxAbort:
			enumname = "unhandled_tx_abort";
			break;
	}
	Assert(enumname != NULL);
	conflict_type_oid = GetSysCacheOid2(ENUMTYPOIDNAME,
		BdrConflictTypeOid, CStringGetDatum(enumname));
	if (conflict_type_oid == InvalidOid)
		elog(ERROR, "syscache lookup for enum %s of type "
			 "bdr.bdr_conflict_type failed", enumname);
	return conflict_type_oid;
}

/* Get the enum name for a given BdrConflictResolution */
static char *
bdr_conflict_resolution_get_name(BdrConflictResolution conflict_resolution)
{
	char *enumname = NULL;

	switch (conflict_resolution)
	{
		case BdrConflictResolution_ConflictTriggerSkipChange:
			enumname = "conflict_trigger_skip_change";
			break;
		case BdrConflictResolution_ConflictTriggerReturnedTuple:
			enumname = "conflict_trigger_returned_tuple";
			break;
		case BdrConflictResolution_LastUpdateWins_KeepLocal:
			enumname = "last_update_wins_keep_local";
			break;
		case BdrConflictResolution_LastUpdateWins_KeepRemote:
			enumname = "last_update_wins_keep_remote";
			break;
		case BdrConflictResolution_DefaultApplyChange:
			enumname = "apply_change";
			break;
		case BdrConflictResolution_DefaultSkipChange:
			enumname = "skip_change";
			break;
		case BdrConflictResolution_UnhandledTxAbort:
			enumname = "unhandled_tx_abort";
			break;
	}

	Assert(enumname != NULL);
	return enumname;
}

/* Get the enum oid for a given BdrConflictResolution */
static Datum
bdr_conflict_resolution_get_datum(BdrConflictResolution conflict_resolution)
{
	Oid conflict_resolution_oid;

	char *enumname = bdr_conflict_resolution_get_name(conflict_resolution);

	conflict_resolution_oid = GetSysCacheOid2(ENUMTYPOIDNAME,
		BdrConflictResolutionOid, CStringGetDatum(enumname));
	if (conflict_resolution_oid == InvalidOid)
		elog(ERROR, "syscache lookup for enum %s of type "
			 "bdr.bdr_conflict_resolution failed", enumname);
	return conflict_resolution_oid;
}

/*
 * Convert the target row to json form if it isn't null.
 */
static Datum
bdr_conflict_row_to_json(Datum row, bool row_isnull, bool *ret_isnull)
{
	Datum row_json;
	if (row_isnull)
	{
		row_json = (Datum) 0;
		*ret_isnull = 1;
	}
	else
	{
		/*
		 * We don't handle errors with a PG_TRY / PG_CATCH here, because that's
		 * not sufficient to make the transaction usable given that we might
		 * fail in user defined casts, etc. We'd need a full savepoint, which
		 * is too expensive. So if this fails we'll just propagate the exception
		 * and abort the apply transaction.
		 *
		 * It shouldn't fail unless something's pretty broken anyway.
		 */
		row_json = DirectFunctionCall1(row_to_json, row);
		*ret_isnull = 0;
	}
	return row_json;
}

/* print the tuple 'tuple' into the StringInfo s */
void
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
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

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

static void
row_to_stringinfo(StringInfo s, Datum composite)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tmptup,
			   *tuple;

	td = DatumGetHeapTupleHeader(composite);

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;

	tuple = &tmptup;

	/* Print the tuple to stringinfo */
	tuple_to_stringinfo(s, tupdesc, tuple);

	ReleaseTupleDesc(tupdesc);
}

static void
bdr_conflict_strtodatum(bool *nulls, Datum *values, int idx,
						const char *in_str)
{
	if (in_str == NULL)
	{
		nulls[idx] = true;
		values[idx] = (Datum) 0;
	}
	else
	{
		nulls[idx] = false;
		values[idx] = CStringGetTextDatum(in_str);
	}
}

/*
 * Log a BDR apply conflict to the bdr.bdr_conflict_history table.
 *
 * The change will then be replicated to other nodes.
 */
void
bdr_conflict_log_table(BdrApplyConflict *conflict)
{
	Datum		 	values[BDR_CONFLICT_HISTORY_COLS];
	bool			nulls[BDR_CONFLICT_HISTORY_COLS];
	int				attno;
	int				object_schema_attno, object_name_attno;
	char			sqlstate[12];
	Relation		log_rel;
	HeapTuple		log_tup;
	TupleTableSlot *log_slot;
	EState		   *log_estate;
	char			local_sysid[SYSID_DIGITS];
	char			remote_sysid[SYSID_DIGITS];
	char			origin_sysid[SYSID_DIGITS];

	if (IsAbortedTransactionBlockState())
		elog(ERROR, "bdr: attempt to log conflict in aborted transaction");

	if (!IsTransactionState())
		elog(ERROR, "bdr: attempt to log conflict without surrounding transaction");

	if (!bdr_log_conflicts_to_table)
		/* No logging enabled and we don't own any memory, just bail */
		return;

	/* Pg has no uint64 SQL type so we have to store all them as text */
	snprintf(local_sysid, sizeof(local_sysid), UINT64_FORMAT,
			 GetSystemIdentifier());

	snprintf(remote_sysid, sizeof(remote_sysid), UINT64_FORMAT,
			 conflict->remote_sysid);

	if (conflict->local_tuple_origin_sysid != 0)
		snprintf(origin_sysid, sizeof(origin_sysid), UINT64_FORMAT,
				 conflict->local_tuple_origin_sysid);
	else
		origin_sysid[0] = '\0';

	memset(nulls, 0, sizeof(bool) * BDR_CONFLICT_HISTORY_COLS);
	memset(values, 0, sizeof(Datum) * BDR_CONFLICT_HISTORY_COLS);

	/* Begin forming the tuple. See the extension SQL file for field info. */
	attno = 0;
	values[attno++] = DirectFunctionCall1(nextval_oid,
		BdrConflictHistorySeqId);
	values[attno++] = CStringGetTextDatum(local_sysid);
	values[attno++] = TransactionIdGetDatum(conflict->local_conflict_txid);
	values[attno++] = LSNGetDatum(conflict->local_conflict_lsn);
	values[attno++] = TimestampTzGetDatum(conflict->local_conflict_time);

	object_schema_attno = attno;
	bdr_conflict_strtodatum(nulls, values, attno++, conflict->object_schema);

	object_name_attno = attno;
	bdr_conflict_strtodatum(nulls, values, attno++, conflict->object_name);

	values[attno++] = CStringGetTextDatum(remote_sysid);
	if (conflict->remote_txid != InvalidTransactionId)
		values[attno] = TransactionIdGetDatum(conflict->remote_txid);
	else
		nulls[attno] = 1;
	attno++;

	values[attno++] = TimestampTzGetDatum(conflict->remote_commit_time);
	values[attno++] = LSNGetDatum(conflict->remote_commit_lsn);
	values[attno++] = bdr_conflict_type_get_datum(conflict->conflict_type);

	values[attno++] =
		bdr_conflict_resolution_get_datum(conflict->conflict_resolution);

	values[attno] = bdr_conflict_row_to_json(conflict->local_tuple,
		conflict->local_tuple_null, &nulls[attno]);
	attno++;

	values[attno] = bdr_conflict_row_to_json(conflict->remote_tuple,
		conflict->remote_tuple_null, &nulls[attno]);
	attno++;

	if (conflict->local_tuple_xmin != InvalidTransactionId)
		values[attno] = TransactionIdGetDatum(conflict->local_tuple_xmin);
	else
		nulls[attno] = 1;
	attno++;

	if (conflict->local_tuple_origin_sysid != 0)
		values[attno] = CStringGetTextDatum(origin_sysid);
	else
		nulls[attno] = 1;
	attno++;

	if (conflict->apply_error == NULL)
	{
		/* all the 13 remaining cols are error_ cols and are all null */
		memset(&nulls[attno], 1, sizeof(bool) * 13);
		attno += 13;
	}
	else
	{
		/*
		 * There's error data to log. We don't attempt to log it selectively,
		 * as bdr apply errors are not supposed to be routine anyway.
		 */
		ErrorData *edata = conflict->apply_error;

		bdr_conflict_strtodatum(nulls, values, attno++, edata->message);

		/*
		 * Always log the SQLSTATE. If it's ERRCODE_INTERNAL_ERROR - like after
		 * an elog(...) - we'll just be writing XX0000, but that's still better
		 * than nothing.
		 */
		strncpy(sqlstate, unpack_sql_state(edata->sqlerrcode), 12);
		sqlstate[sizeof(sqlstate)-1] = '\0';
		values[attno] = CStringGetTextDatum(sqlstate);

		/*
		 * We'd like to log the statement running at the time of the ERROR (for
		 * DDL apply errors) but have no reliable way to acquire it yet. So for
		 * now...
		 */
		nulls[attno] = 1;
		attno++;

		if (edata->cursorpos != 0)
			values[attno] = Int32GetDatum(edata->cursorpos);
		else
			nulls[attno] = 1;
		attno++;

		bdr_conflict_strtodatum(nulls, values, attno++, edata->detail);
		bdr_conflict_strtodatum(nulls, values, attno++, edata->hint);
		bdr_conflict_strtodatum(nulls, values, attno++, edata->context);
		bdr_conflict_strtodatum(nulls, values, attno++, edata->column_name);
		bdr_conflict_strtodatum(nulls, values, attno++, edata->datatype_name);
		bdr_conflict_strtodatum(nulls, values, attno++, edata->constraint_name);
		bdr_conflict_strtodatum(nulls, values, attno++, edata->filename);
		values[attno++] = Int32GetDatum(edata->lineno);
		bdr_conflict_strtodatum(nulls, values, attno++, edata->funcname);

		/* Set schema and table name based on the error, not arg values */
		bdr_conflict_strtodatum(nulls, values, object_schema_attno,
								edata->schema_name);
		bdr_conflict_strtodatum(nulls, values, object_name_attno,
								edata->table_name);

		/* note: do NOT free the errordata, it's the caller's responsibility */
	}

	/* Make sure assignments match allocated tuple size */
	Assert(attno == BDR_CONFLICT_HISTORY_COLS);

	/*
	 * Construct a bdr.bdr_conflict_history tuple from the conflict info we've
	 * been passed and insert it into bdr.bdr_conflict_history.
	 */
	log_rel = heap_open(BdrConflictHistoryRelId, RowExclusiveLock);

	/* Prepare executor state for index updates */
	log_estate = bdr_create_rel_estate(log_rel);
	log_slot = ExecInitExtraTupleSlot(log_estate);
	ExecSetSlotDescriptor(log_slot, RelationGetDescr(log_rel));
	/* Construct the tuple and insert it */
	log_tup = heap_form_tuple(RelationGetDescr(log_rel), values, nulls);
	ExecStoreTuple(log_tup, log_slot, InvalidBuffer, true);
	simple_heap_insert(log_rel, log_slot->tts_tuple);
	/* Then do any index maintanence required */
	UserTableUpdateIndexes(log_estate, log_slot);
	/* and finish up */
	heap_close(log_rel, RowExclusiveLock);
	ExecResetTupleTable(log_estate->es_tupleTable, true);
	FreeExecutorState(log_estate);
}

/*
 * Log a BDR apply conflict to the postgreql log.
 */
void
bdr_conflict_log_serverlog(BdrApplyConflict *conflict)
{
	StringInfoData	s_key;
	char		   *resolution_name;

#define CONFLICT_MSG_PREFIX "CONFLICT: remote %s on relation %s.%s originating at node " UINT64_FORMAT ":%u:%u at ts %s;"

	/* Create text representation of the PKEY tuple */
	initStringInfo(&s_key);
	if (!conflict->local_tuple_null)
		row_to_stringinfo(&s_key, conflict->local_tuple);

	resolution_name = bdr_conflict_resolution_get_name(conflict->conflict_resolution);

	switch(conflict->conflict_type)
	{
		case BdrConflictType_InsertInsert:
		case BdrConflictType_UpdateUpdate:
			ereport(LOG,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg(CONFLICT_MSG_PREFIX " row was previously updated at node " UINT64_FORMAT ":%u. Resolution: %s; PKEY:%s",
							BdrConflictType_InsertInsert ? "INSERT" : "UPDATE",
							conflict->object_schema, conflict->object_name,
							conflict->remote_sysid, conflict->remote_tli,
							conflict->remote_dboid,
							timestamptz_to_str(conflict->remote_commit_time),
							conflict->local_tuple_origin_sysid,
							conflict->local_tuple_origin_tli,
							resolution_name,
							s_key.data)));
			break;
		case BdrConflictType_UpdateDelete:
		case BdrConflictType_DeleteDelete:
			ereport(LOG,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg(CONFLICT_MSG_PREFIX " could not find existing row. Resolution: %s; PKEY:%s",
							BdrConflictType_UpdateDelete ? "UPDATE" : "DELETE",
							conflict->object_schema, conflict->object_name,
							conflict->remote_sysid, conflict->remote_tli,
							conflict->remote_dboid,
							timestamptz_to_str(conflict->remote_commit_time),
							resolution_name,
							s_key.data)));

			break;
		case BdrConflictType_InsertUpdate:
			/* XXX? */
			break;
		case BdrConflictType_UnhandledTxAbort:
			/* XXX? */
			break;
	}

	resetStringInfo(&s_key);
}


/*
 * Log a BDR apply conflict to the bdr.bdr_conflict_history table and/or
 * system log.
 *
 * If a transaction is in progress, the current transaction is used, in which
 * case the write is lost if that transaction subsequently aborts. If no
 * transaction is already open the caller must open one and commit it after
 * calling.
 *
 * Any open aborted transaction must be rolled back before calling.
 *
 * If apply_error is passed then this call is because of an unhandled error. In
 * this case the passed object_schema and object_name are ignored in favour of
 * those in the ErrorData struct and all the error_ fields are populated from
 * the ErrorData struct.
 *
 * Any palloc'd or copied values passed must be freed by the caller after
 * bdr_conflict_log returns. It won't retain references to any values and won't
 * free any of the values its self.
 *
 * The origin id - i.e. the id of the node the local tuple was first created
 * on, in case the local tuple was originally replicated from another node -
 * may be obtained with TransactionIdGetCommitTsData on the xmin of the
 * TupleTableSlot for the tuple. It isn't done here because the caller
 * frequently already has the node id to hand.
 */
BdrApplyConflict *
bdr_make_apply_conflict(BdrConflictType conflict_type,
						BdrConflictResolution resolution,
						TransactionId remote_txid,
						BDRRelation *conflict_relation,
						TupleTableSlot *local_tuple,
						RepNodeId local_tuple_origin_id,
						TupleTableSlot *remote_tuple,
						ErrorData *apply_error)
{
	MemoryContext	old_context;
	BdrApplyConflict *conflict;

	old_context = MemoryContextSwitchTo(conflict_log_context);

	conflict = palloc0(sizeof(BdrApplyConflict));

	/* Populate the conflict record we're going to log */
	conflict->conflict_type = conflict_type;
	conflict->conflict_resolution = resolution;

	conflict->local_conflict_txid = GetTopTransactionIdIfAny();
	conflict->local_conflict_lsn = GetXLogInsertRecPtr();
	conflict->local_conflict_time = GetCurrentTimestamp();
	conflict->remote_txid = remote_txid;

	/* set using bdr_conflict_setrel */
	if (conflict_relation == NULL)
	{
		conflict->object_schema = NULL;
		conflict->object_name = NULL;
	}
	else
	{
		conflict->object_name = RelationGetRelationName(conflict_relation->rel);
		conflict->object_schema =
			get_namespace_name(RelationGetNamespace(conflict_relation->rel));
	}

	/* TODO: May make sense to cache the remote sysid in a global too... */
	bdr_fetch_sysid_via_node_id(replication_origin_id,
								&conflict->remote_sysid,
								&conflict->remote_tli,
								&conflict->remote_dboid);
	conflict->remote_commit_time = replication_origin_timestamp;
	conflict->remote_txid = remote_txid;
	conflict->remote_commit_lsn = replication_origin_lsn;

	if (local_tuple != NULL)
	{
		/* Log local tuple xmin even if actual tuple value logging is off */
		conflict->local_tuple_xmin =
			HeapTupleHeaderGetXmin(local_tuple->tts_tuple->t_data);
		Assert(conflict->local_tuple_xmin >= FirstNormalTransactionId ||
			   conflict->local_tuple_xmin == FrozenTransactionId);
		if (bdr_conflict_logging_include_tuples)
		{
			conflict->local_tuple = ExecFetchSlotTupleDatum(local_tuple);
			conflict->local_tuple_null = false;
		}
	}
	else
	{
		conflict->local_tuple_null = true;
		conflict->local_tuple = (Datum) 0;
		conflict->local_tuple_xmin = InvalidTransactionId;
	}

	if (local_tuple_origin_id != InvalidRepNodeId)
	{
		bdr_fetch_sysid_via_node_id(local_tuple_origin_id,
									&conflict->local_tuple_origin_sysid,
									&conflict->local_tuple_origin_tli,
									&conflict->local_tuple_origin_dboid);
	}
	else
	{
		conflict->local_tuple_origin_sysid = 0;
	}

	if (remote_tuple != NULL && bdr_conflict_logging_include_tuples)
	{
		conflict->remote_tuple = ExecFetchSlotTupleDatum(remote_tuple);
		conflict->remote_tuple_null = false;
	}
	else
	{
		conflict->remote_tuple_null = true;
		conflict->remote_tuple = (Datum) 0;
	}

	conflict->apply_error = apply_error;

	MemoryContextSwitchTo(old_context);

	return conflict;
}
