/* -------------------------------------------------------------------------
 *
 * bdr_conflict_handlers.c
 *		Conflict handler handling
 *
 * User defined handlers for replication conflicts
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_conflict_handlers.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"

#include "bdr.h"

#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"

#include "executor/spi.h"

#include "fmgr.h"
#include "funcapi.h"

#include "miscadmin.h"

#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/rel.h"

PG_FUNCTION_INFO_V1(bdr_create_conflict_handler);
PG_FUNCTION_INFO_V1(bdr_drop_conflict_handler);

const char *create_handler_sql =
"INSERT INTO bdr.bdr_conflict_handlers " \
"   (ch_name, ch_type, ch_reloid, ch_fun, ch_timeframe)\n" \
"   VALUES ($1, $2, $3, $4, $5)";

const char *drop_handler_sql =
"DELETE FROM bdr.bdr_conflict_handlers WHERE ch_name = $1 AND ch_reloid = $2";

const char *drop_handler_get_tbl_oid_sql =
"SELECT oid FROM bdr.bdr_conflict_handlers WHERE ch_name = $1 AND ch_reloid = $2";

const char *handler_queued_table_sql =
"INSERT INTO bdr.bdr_queued_commands (lsn, queued_at, perpetrator, command_tag, command)\n" \
"   VALUES (pg_current_xlog_location(), NOW(), CURRENT_USER, 'SELECT', $1)";

const char *get_conflict_handlers_for_table_sql =
"SELECT ch_fun::regprocedure, ch_type::text ch_type, ch_timeframe FROM bdr.bdr_conflict_handlers" \
"   WHERE ch_reloid = $1 ORDER BY ch_type, ch_name";

static void bdr_conflict_handlers_check_handler_fun(Relation rel, Oid proc_oid);
static void bdr_conflict_handlers_check_access(Oid reloid);
static const char *bdr_conflict_handlers_event_type_name(BdrConflictType event_type);

static Oid	bdr_conflict_handler_table_oid = InvalidOid;
static Oid	bdr_conflict_handler_type_oid = InvalidOid;
static Oid	bdr_conflict_handler_action_oid = InvalidOid;
static Oid	bdr_conflict_handler_action_ignore_oid = InvalidOid;
static Oid	bdr_conflict_handler_action_row_oid = InvalidOid;
static Oid	bdr_conflict_handler_action_skip_oid = InvalidOid;

void
bdr_conflict_handlers_init(void)
{
	Oid			schema_oid = get_namespace_oid("bdr", false);

	bdr_conflict_handler_table_oid = get_relname_relid("bdr_conflict_handlers",
													   schema_oid);

	if (bdr_conflict_handler_table_oid == InvalidOid)
		elog(ERROR, "cache lookup failed for relation bdr.bdr_conflict_handlers");

	bdr_conflict_handler_type_oid =
		GetSysCacheOidError2(TYPENAMENSP, PointerGetDatum("bdr_conflict_type"),
							 ObjectIdGetDatum(schema_oid));

	bdr_conflict_handler_action_oid =
		GetSysCacheOidError2(TYPENAMENSP, PointerGetDatum("bdr_conflict_handler_action"),
							 ObjectIdGetDatum(schema_oid));

	bdr_conflict_handler_action_ignore_oid =
		GetSysCacheOidError2(ENUMTYPOIDNAME, bdr_conflict_handler_action_oid,
							 CStringGetDatum("IGNORE"));

	bdr_conflict_handler_action_row_oid =
		GetSysCacheOidError2(ENUMTYPOIDNAME, bdr_conflict_handler_action_oid,
							 CStringGetDatum("ROW"));

	bdr_conflict_handler_action_skip_oid =
		GetSysCacheOidError2(ENUMTYPOIDNAME, bdr_conflict_handler_action_oid,
							 CStringGetDatum("SKIP"));
}

/*
 * Verify privileges for the given relation; raise an error if current user is
 * not the owner of either the table or the schema it belongs it.
 *
 * Also raise an error if the relation is a system catalog.
 */
static void
bdr_conflict_handlers_check_access(Oid reloid)
{
	HeapTuple	tuple;
	Form_pg_class classform;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(reloid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", reloid);

	classform = (Form_pg_class) GETSTRUCT(tuple);

	if (!pg_class_ownercheck(reloid, GetUserId()) &&
		!pg_namespace_ownercheck(classform->relnamespace, GetUserId()))
		ereport(ERROR,
				(errmsg("permission denied to relation %s",
						NameStr(classform->relname))));

	if (IsSystemClass(reloid, classform))
		ereport(ERROR,
				(errmsg("permission denied: %s is a system catalog",
						NameStr(classform->relname))));

	ReleaseSysCache(tuple);
}

/*
 * Creates a new conflict handler. This replicates by inserting to
 * bdr.bdr_queued_commands.
 */
Datum
bdr_create_conflict_handler(PG_FUNCTION_ARGS)
{
	Oid			reloid,
				proc_oid;

	char	   *ch_name;
	Datum		ch_type_oid,
				type_label_datum;

	char	   *label = NULL;
	int			ret;

	Oid			argtypes[5];
	Datum		values[5];
	char		nulls[5];

	ObjectAddress myself,
				rel_object;

	Relation	rel;

	if (PG_NARGS() < 4 || PG_NARGS() > 5)
		elog(ERROR, "expecting four or five arguments, got %d", PG_NARGS());

	if (bdr_conflict_handler_table_oid == InvalidOid)
		bdr_conflict_handlers_init();

	reloid = PG_GETARG_OID(0);
	ch_name = NameStr(*PG_GETARG_NAME(1));
	proc_oid = PG_GETARG_OID(2);

	bdr_conflict_handlers_check_access(reloid);

	/*
	 * We lock the relation we're referring to avoid race conditions with
	 * DROP.
	 *
	 * XXX why SUE?  Wouldn't AccessShare be sufficient for that?
	 */
	rel = heap_open(reloid, ShareUpdateExclusiveLock);

	/* ensure that handler function is good */
	bdr_conflict_handlers_check_handler_fun(rel, proc_oid);

	/*
	 * build up arguments for the INSERT INTO bdr.bdr_conflict_handlers
	 */

	argtypes[0] = NAMEOID;
	nulls[0] = false;
	values[0] = PG_GETARG_DATUM(1);

	argtypes[1] = bdr_conflict_handler_type_oid;

	ch_type_oid = PG_GETARG_DATUM(3);
	type_label_datum = DirectFunctionCall1(enum_out, ch_type_oid);
	label = DatumGetCString(type_label_datum);

	nulls[1] = false;
	values[1] = ch_type_oid;

	argtypes[2] = REGCLASSOID;
	nulls[2] = false;
	values[2] = PG_GETARG_DATUM(0);

	argtypes[3] = TEXTOID;
	nulls[3] = false;
	values[3] =
		CStringGetTextDatum(format_procedure_qualified(PG_GETARG_OID(2)));

	argtypes[4] = INTERVALOID;
	if (PG_NARGS() == 4)
		nulls[4] = 'n';
	else
	{
		nulls[4] = false;
		values[4] = PG_GETARG_DATUM(4);
	}

	/*
	 * execute INSERT INTO bdr.bdr_conflict_handlers
	 */

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	ret = SPI_execute_with_args(create_handler_sql, 5, argtypes,
								values, nulls, false, 0);

	if (ret != SPI_OK_INSERT)
		elog(ERROR, "expected SPI state %u, got %u", SPI_OK_INSERT, ret);

	if (SPI_processed != 1)
		elog(ERROR, "expected one processed row, got "UINT64_FORMAT, (uint64)SPI_processed);

	/*
	 * set up the dependency relation with ourselves as "dependent"
	 */

	myself.classId = bdr_conflict_handler_table_oid;
	myself.objectId = SPI_lastoid;
	myself.objectSubId = 0;

	rel_object.classId = RelationRelationId;
	rel_object.objectId = reloid;
	rel_object.objectSubId = 0;

	recordDependencyOn(&myself, &rel_object, DEPENDENCY_INTERNAL);
	CommandCounterIncrement();

	CacheInvalidateRelcacheByRelid(reloid);

	/*
	 * last: INSERT to queued_commands for replication if not replaying
	 */
	if (replorigin_session_origin == InvalidRepOriginId)
	{
		StringInfoData query;
		char	   *proc_name = format_procedure_qualified(proc_oid);
		char	   *quoted_ch_name = quote_literal_cstr(ch_name),
				   *quoted_proc_name = quote_literal_cstr(proc_name),
				   *quoted_label,
				   *quoted_rel_name;

		initStringInfo(&query);

		quoted_rel_name =
			quote_literal_cstr(quote_qualified_identifier(
														  get_namespace_name(RelationGetNamespace(rel)),
														  RelationGetRelationName(rel)));

		if (label)
		{
			quoted_label = quote_literal_cstr(label);

			appendStringInfo(&query,
					"SELECT bdr.bdr_create_conflict_handler(%s, %s, %s, %s)",
							 quoted_rel_name,
							 quoted_ch_name,
							 quoted_proc_name,
							 quoted_label);
		}
		else
			appendStringInfo(&query,
						"SELECT bdr.bdr_create_conflict_handler(%s, %s, %s)",
							 quoted_rel_name,
							 quoted_ch_name,
							 quoted_proc_name);

		argtypes[0] = TEXTOID;
		nulls[0] = false;
		values[0] = CStringGetTextDatum(query.data);

		ret = SPI_execute_with_args(handler_queued_table_sql, 1, argtypes,
									values, nulls, false, 0);

		if (ret != SPI_OK_INSERT)
			elog(ERROR, "expected SPI state %u, got %u", SPI_OK_INSERT, ret);
		if (SPI_processed != 1)
			elog(ERROR, "expected one processed row, got "UINT64_FORMAT, (uint64)SPI_processed);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	heap_close(rel, NoLock);

	PG_RETURN_VOID();
}

/*
 * Drops a conflict handler by removing it from the table and removing the
 * correspondig dependency row. This replicates by inserting to
 * bdr.bdr_queued_commands.
 */
Datum
bdr_drop_conflict_handler(PG_FUNCTION_ARGS)
{
	Oid			rowoid,
				tg_reloid;
	char	   *ch_name;
	int			ret;
	bool		isnull;

	Oid			argtypes[2];
	Datum		values[2],
				dat;
	char		nulls[2];

	HeapTuple	spi_rslt;
	TupleDesc	spi_rslt_desc;

	int			col_oid;

	Relation	rel;

	if (PG_NARGS() != 2)
		elog(ERROR,
			 "expecting exactly two arguments");

	if (bdr_conflict_handler_table_oid == InvalidOid)
		bdr_conflict_handlers_init();

	tg_reloid = PG_GETARG_OID(0);

	ch_name = PG_GETARG_NAME(1)->data;

	argtypes[0] = NAMEOID;
	values[0] = PG_GETARG_DATUM(1);
	nulls[0] = 0;

	argtypes[1] = OIDOID;
	values[1] = PG_GETARG_DATUM(0);
	nulls[1] = 0;

	bdr_conflict_handlers_check_access(tg_reloid);

	rel = heap_open(tg_reloid, ShareUpdateExclusiveLock);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * get the row oid to remove the dependency
	 */
	ret = SPI_execute_with_args(drop_handler_get_tbl_oid_sql, 2, argtypes,
								values, nulls, false, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "expected SPI state %u, got %u", SPI_OK_SELECT, ret);

	if (SPI_processed != 1)
		elog(ERROR, "handler %s not found", ch_name);

	spi_rslt = SPI_tuptable->vals[0];
	spi_rslt_desc = SPI_tuptable->tupdesc;

	col_oid = SPI_fnumber(spi_rslt_desc, "oid");

	dat = SPI_getbinval(spi_rslt, spi_rslt_desc, col_oid, &isnull);
	rowoid = DatumGetObjectId(dat);

	/*
	 * delete the handler row from bdr_conflict_handlers
	 */
	ret = SPI_execute_with_args(drop_handler_sql, 2, argtypes,
								values, nulls, false, 0);

	if (ret != SPI_OK_DELETE)
		elog(ERROR, "expected SPI state %u, got %u", SPI_OK_DELETE, ret);

	/*
	 * remove the dependency
	 */
	deleteDependencyRecordsForClass(bdr_conflict_handler_table_oid, rowoid,
									RelationRelationId, DEPENDENCY_INTERNAL);
	CommandCounterIncrement();

	CacheInvalidateRelcacheByRelid(tg_reloid);

	/*
	 * last: INSERT to queued_commands for replication if not replaying
	 */
	if (replorigin_session_origin == InvalidRepOriginId)
	{
		StringInfoData query;
		char	   *quoted_ch_name = quote_literal_cstr(ch_name);

		initStringInfo(&query);

		appendStringInfo(&query,
						 "SELECT bdr.bdr_drop_conflict_handler(%d, %s)",
						 tg_reloid, quoted_ch_name);

		pfree(quoted_ch_name);

		argtypes[0] = TEXTOID;
		nulls[0] = false;
		values[0] = CStringGetTextDatum(query.data);

		ret = SPI_execute_with_args(handler_queued_table_sql, 1, argtypes,
									values, nulls, false, 0);

		if (ret != SPI_OK_INSERT)
			elog(ERROR, "expected SPI state %u, got %u", SPI_OK_INSERT, ret);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	heap_close(rel, NoLock);

	PG_RETURN_VOID();
}

/*
 * Check that the handler function signature is what we expect; error out if not
 */
static void
bdr_conflict_handlers_check_handler_fun(Relation rel, Oid proc_oid)
{
	HeapTuple	tuple;
	Form_pg_proc proc;
	char		typtype;
	int			numargs;
	TupleDesc	retdesc;
	Oid		   *argtypes;
	char	  **argnames;
	char	   *argmodes;
	const char *hint = NULL;

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc_oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", proc_oid);

	proc = (Form_pg_proc) GETSTRUCT(tuple);
	typtype = get_typtype(proc->prorettype);

	numargs = get_func_arg_info(tuple,
								&argtypes, &argnames, &argmodes);
	retdesc = build_function_result_tupdesc_t(tuple);

	do
	{
		if (typtype != TYPTYPE_PSEUDO || proc->prorettype != RECORDOID)
		{
			hint = "Return type is not RECORD.";
			break;
		}


		if (numargs != 7)
		{
			hint = "Function doesn't have 7 arguments.";
			break;
		}

		if (retdesc == NULL || retdesc->natts != 2)
		{
			hint = "Function doesn't have 2 OUT arguments";
			break;
		}

		if (retdesc->attrs[0]->atttypid != rel->rd_rel->reltype ||
			retdesc->attrs[1]->atttypid != bdr_conflict_handler_action_oid)
		{
			hint = "OUT argument are not of the expected types.";
			break;
		}

		if (argtypes[2] != TEXTOID ||
			argtypes[3] != REGCLASSOID ||
			argtypes[4] != bdr_conflict_handler_type_oid)
		{
			/* XXX ugh */
			hint = "Three last input arguments are not (text, regclass, bdr.bdr_conflict_type).";
			break;
		}

		if (argmodes[0] != PROARGMODE_IN ||
			argmodes[1] != PROARGMODE_IN ||
			argmodes[2] != PROARGMODE_IN ||
			argmodes[3] != PROARGMODE_IN ||
			argmodes[4] != PROARGMODE_IN ||
			argmodes[5] != PROARGMODE_OUT ||
			argmodes[6] != PROARGMODE_OUT)
		{
			hint = "There must be five IN arguments and two OUT arguments.";
			break;
		}

		typtype = get_typtype(argtypes[0]);
		if (typtype != TYPTYPE_COMPOSITE || argtypes[0] != rel->rd_rel->reltype)
		{
			hint = "First input argument must be of the same type as the table.";
			break;
		}

		typtype = get_typtype(argtypes[1]);
		if (typtype != TYPTYPE_COMPOSITE || argtypes[1] != rel->rd_rel->reltype)
		{
			hint = "Second input argument must be of the same type as the table.";
			break;
		}

		/* everything seems OK */
		ReleaseSysCache(tuple);
		return;
	} while (false);

	ereport(ERROR,
			(errmsg("conflict handler function signature must be %s",
					"(IN tablerow, IN tablerow, IN text, IN regclass, IN bdr.bdr_conflict_type, OUT tablerow, OUT bdr.bdr_conflict_handler_action)"),
			 hint ? errhint("%s", hint) : 0));
}

/*
 * get a list of user conflict handlers suitable for the specified relation
 * and handler type; ch_type may be NULL, in this case only handlers without
 * specified handler type are returned.
 */
static void
bdr_get_conflict_handlers(BDRRelation * rel)
{
	Oid			argtypes[1];
	Datum		values[1],
				dat;
	char		nulls[1];
	bool		isnull;

	HeapTuple	spi_row;

	int			ret;
	size_t		i;

	/*
	 * build up cache if not yet done
	 */
	if (rel->conflict_handlers == NULL)
	{
		int			fun_col_no,
					type_col_no,
					intrvl_col_no;
		char	   *htype;
		Interval   *intrvl;

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		argtypes[0] = OIDOID;
		nulls[0] = false;
		values[0] = ObjectIdGetDatum(RelationGetRelid(rel->rel));

		ret = SPI_execute_with_args(get_conflict_handlers_for_table_sql,
									1, argtypes, values, nulls, false, 0);

		if (ret != SPI_OK_SELECT)
			elog(ERROR, "expected SPI state %u, got %u", SPI_OK_SELECT, ret);

		rel->conflict_handlers_len = SPI_processed;
		rel->conflict_handlers =
			MemoryContextAlloc(CacheMemoryContext,
							   SPI_processed * sizeof(BDRConflictHandler));

		fun_col_no = SPI_fnumber(SPI_tuptable->tupdesc, "ch_fun");
		type_col_no = SPI_fnumber(SPI_tuptable->tupdesc, "ch_type");
		intrvl_col_no = SPI_fnumber(SPI_tuptable->tupdesc, "ch_timeframe");

		for (i = 0; i < SPI_processed; ++i)
		{
			spi_row = SPI_tuptable->vals[i];

			dat = SPI_getbinval(spi_row, SPI_tuptable->tupdesc, fun_col_no,
								&isnull);

			/*
			 * since we have a NOT NULL constraint this should never happen.
			 * But, y'know, defensive coding…
			 */
			if (isnull)
				elog(ERROR, "Handler OID is null");

			rel->conflict_handlers[i].handler_oid = DatumGetObjectId(dat);

			dat = SPI_getbinval(spi_row, SPI_tuptable->tupdesc, type_col_no,
								&isnull);

			/*
			 * since we have a NOT NULL constraint this should never happen.
			 * But, y'know, defensive coding…
			 */
			if (isnull)
				elog(ERROR, "Handler type is null");

			htype = TextDatumGetCString(dat);

			if (strcmp(htype, "update_update") == 0)
				rel->conflict_handlers[i].handler_type = BdrConflictType_UpdateUpdate;
			else if (strcmp(htype, "update_delete") == 0)
				rel->conflict_handlers[i].handler_type = BdrConflictType_UpdateDelete;
			else if (strcmp(htype, "delete_delete") == 0)
				rel->conflict_handlers[i].handler_type = BdrConflictType_DeleteDelete;
			else if (strcmp(htype, "insert_insert") == 0)
				rel->conflict_handlers[i].handler_type = BdrConflictType_InsertInsert;
			else if (strcmp(htype, "insert_update") == 0)
				rel->conflict_handlers[i].handler_type = BdrConflictType_InsertUpdate;
			else
				elog(ERROR, "unknown handler type: %s", htype);

			dat = SPI_getbinval(spi_row, SPI_tuptable->tupdesc, intrvl_col_no,
								&isnull);

			if (isnull)
				rel->conflict_handlers[i].timeframe = 0;
			else
			{
				intrvl = DatumGetIntervalP(dat);
				rel->conflict_handlers[i].timeframe =
					intrvl->month * DAYS_PER_MONTH * USECS_PER_DAY +
					intrvl->day * USECS_PER_DAY +
					intrvl->time;
			}

		}

		if (SPI_finish() != SPI_OK_FINISH)
			elog(ERROR, "SPI_finish failed");
	}
}

static const char *
bdr_conflict_handlers_event_type_name(BdrConflictType event_type)
{
	switch (event_type)
	{
		case BdrConflictType_InsertInsert:
			return "insert_insert";
		case BdrConflictType_InsertUpdate:
			return "insert_update";
		case BdrConflictType_UpdateUpdate:
			return "update_update";
		case BdrConflictType_UpdateDelete:
			return "update_delete";
		case BdrConflictType_DeleteDelete:
			return "delete_delete";
		case BdrConflictType_UnhandledTxAbort:
			return "unhandled_tx_abort";

		default:
			elog(ERROR,
				 "wrong value for event type, possibly corrupted memory: %d",
				event_type);
	}

	return "(unknown)";
}

/*
 * Call a list of handlers (identified by Oids) and return the first non-NULL
 * return value. Return NULL if no handler returns a non-NULL value.
 */
HeapTuple
bdr_conflict_handlers_resolve(BDRRelation * rel, const HeapTuple local,
							  const HeapTuple remote, const char *command_tag,
							  BdrConflictType event_type,
							  uint64 timeframe, bool *skip)
{
	size_t		i;
	Datum		retval;
	HeapTuple	copy_local = NULL,
				copy_remote = NULL;

	FunctionCallInfoData fcinfo;
	FmgrInfo	finfo;

	HeapTuple	fun_tup;
	HeapTupleData result_tup;
	HeapTupleHeader tup_header;
	TupleDesc	retdesc;
	Datum		val;
	bool		isnull;
	Oid			event_oid;
	const char *event = bdr_conflict_handlers_event_type_name(event_type);

	*skip = false;

	bdr_get_conflict_handlers(rel);

	event_oid = GetSysCacheOidError2(ENUMTYPOIDNAME,
									 bdr_conflict_handler_type_oid,
									 CStringGetDatum(event));

	for (i = 0; i < rel->conflict_handlers_len; ++i)
	{
		/*
		 * ignore all handlers which don't match the type or are not usable by
		 * timeframe
		 */
		if (rel->conflict_handlers[i].handler_type != event_type ||
			(rel->conflict_handlers[i].timeframe != 0 &&
			 rel->conflict_handlers[i].timeframe < timeframe))
			continue;

		fmgr_info(rel->conflict_handlers[i].handler_oid, &finfo);
		InitFunctionCallInfoData(fcinfo, &finfo, 5, InvalidOid, NULL, NULL);

		if (local != NULL)
		{
			/* FIXME: use facilities from 3f8c8e3c61ce once rebased again. */
			copy_local = heap_copytuple(local);

			HeapTupleHeaderSetDatumLength(copy_local->t_data,
										  copy_local->t_len);
			HeapTupleHeaderSetTypeId(copy_local->t_data,
									 RelationGetDescr(rel->rel)->tdtypeid);

			fcinfo.arg[0] = HeapTupleGetDatum(copy_local);
			fcinfo.argnull[0] = false;
		}
		else
			fcinfo.argnull[0] = true;

		if (remote != NULL)
		{
			/* FIXME: use facilities from 3f8c8e3c61ce once rebased again. */
			copy_remote = heap_copytuple(remote);

			HeapTupleHeaderSetDatumLength(copy_remote->t_data,
										  copy_remote->t_len);
			HeapTupleHeaderSetTypeId(copy_remote->t_data,
									 RelationGetDescr(rel->rel)->tdtypeid);

			fcinfo.arg[1] = HeapTupleGetDatum(copy_remote);
			fcinfo.argnull[1] = false;
		}
		else
			fcinfo.argnull[1] = true;

		fcinfo.arg[2] = CStringGetTextDatum(command_tag);
		fcinfo.arg[3] = ObjectIdGetDatum(RelationGetRelid(rel->rel));
		fcinfo.arg[4] = event_oid;

		retval = FunctionCallInvoke(&fcinfo);

		if (copy_local)
			heap_freetuple(copy_local);
		if (copy_remote)
			heap_freetuple(copy_remote);

		if (fcinfo.isnull)
			elog(ERROR, "handler return value is NULL");

		tup_header = DatumGetHeapTupleHeader(retval);

		fun_tup = SearchSysCache1(PROCOID,
					ObjectIdGetDatum(rel->conflict_handlers[i].handler_oid));
		if (!HeapTupleIsValid(fun_tup))
			elog(ERROR, "cache lookup failed for function %u",
				 rel->conflict_handlers[i].handler_oid);

		retdesc = build_function_result_tupdesc_t(fun_tup);

		ReleaseSysCache(fun_tup);

		result_tup.t_len = HeapTupleHeaderGetDatumLength(tup_header);
		ItemPointerSetInvalid(&(result_tup.t_self));
		result_tup.t_tableOid = InvalidOid;
		result_tup.t_data = tup_header;

		val = fastgetattr(&result_tup, 2, retdesc, &isnull);

		if (isnull)
			elog(ERROR, "handler action may not be NULL!");

		if (DatumGetObjectId(val) == bdr_conflict_handler_action_row_oid)
		{
			HeapTuple	tup = palloc(sizeof(*tup));

			val = fastgetattr(&result_tup, 1, retdesc, &isnull);

			if (isnull)
				elog(ERROR, "handler action is ROW but returned row is NULL");

			tup_header = DatumGetHeapTupleHeader(val);

			if(HeapTupleHeaderGetTypeId(tup_header) != rel->rel->rd_rel->reltype)
				elog(ERROR, "Handler %d returned unexpected tuple type %d",
					 rel->conflict_handlers[i].handler_oid,
					 retdesc->attrs[0]->atttypid);

			tup->t_len = HeapTupleHeaderGetDatumLength(tup_header);
			ItemPointerSetInvalid(&(tup->t_self));
			tup->t_tableOid = InvalidOid;
			tup->t_data = tup_header;

			return tup;
		}
		else if (DatumGetObjectId(val) == bdr_conflict_handler_action_skip_oid)
		{
			*skip = true;
			return NULL;
		}
		else if (DatumGetObjectId(val) == bdr_conflict_handler_action_ignore_oid)
			continue;
	}

	return NULL;
}
