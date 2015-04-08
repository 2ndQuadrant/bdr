/* -------------------------------------------------------------------------
 *
 * bdr_executor.c
 *      Relation and index access and maintenance routines required by bdr
 *
 * BDR does a lot of direct access to indexes and relations, some of which
 * isn't handled by simple calls into the backend. Most of it lives here.
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      bdr_executor.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "bdr.h"

#include "access/heapam.h"
#include "access/skey.h"
#include "access/xact.h"
#include "access/xlog_fn.h"

#include "catalog/namespace.h"
#include "catalog/pg_trigger.h"

#include "commands/event_trigger.h"
#include "commands/trigger.h"

#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"

#include "parser/parse_relation.h"
#include "parser/parsetree.h"

#include "storage/bufmgr.h"
#include "storage/lmgr.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


static void BdrExecutorStart(QueryDesc *queryDesc, int eflags);

static ExecutorStart_hook_type PrevExecutorStart_hook = NULL;

static bool bdr_always_allow_writes = false;
bool in_bdr_replicate_ddl_command = false;

#ifdef BUILDING_BDR
PGDLLEXPORT Datum bdr_queue_ddl_commands(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bdr_queue_ddl_commands);
PGDLLEXPORT Datum bdr_queue_dropped_objects(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bdr_queue_dropped_objects);
#endif
PGDLLEXPORT Datum bdr_replicate_ddl_command(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bdr_replicate_ddl_command);
PGDLLEXPORT Datum bdr_(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(bdr_truncate_trigger_add);

EState *
bdr_create_rel_estate(Relation rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;

	estate = CreateExecutorState();

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = rel;
	resultRelInfo->ri_TrigInstrument = NULL;

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	return estate;
}

void
UserTableUpdateIndexes(EState *estate, TupleTableSlot *slot)
{
	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(slot->tts_tuple))
		return;

	ExecOpenIndices(estate->es_result_relation_info);
	UserTableUpdateOpenIndexes(estate, slot);
	ExecCloseIndices(estate->es_result_relation_info);
}

void
UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot)
{
	List	   *recheckIndexes = NIL;

	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(slot->tts_tuple))
		return;

	if (estate->es_result_relation_info->ri_NumIndices > 0)
	{
		recheckIndexes = ExecInsertIndexTuples(slot,
											   &slot->tts_tuple->t_self,
											   estate);

		if (recheckIndexes != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("bdr doesn't support index rechecks")));
	}

	/* FIXME: recheck the indexes */
	list_free(recheckIndexes);
}

void
build_index_scan_keys(EState *estate, ScanKey *scan_keys, BDRTupleData *tup)
{
	ResultRelInfo *relinfo;
	int i;

	relinfo = estate->es_result_relation_info;

	/* build scankeys for each index */
	for (i = 0; i < relinfo->ri_NumIndices; i++)
	{
		IndexInfo  *ii = relinfo->ri_IndexRelationInfo[i];

		/*
		 * Only unique indexes are of interest here, and we can't deal with
		 * expression indexes so far. FIXME: predicates should be handled
		 * better.
		 */
		if (!ii->ii_Unique || ii->ii_Expressions != NIL)
		{
			scan_keys[i] = NULL;
			continue;
		}

		scan_keys[i] = palloc(ii->ii_NumIndexAttrs * sizeof(ScanKeyData));

		/*
		 * Only return index if we could build a key without NULLs.
		 */
		if (build_index_scan_key(scan_keys[i],
								  relinfo->ri_RelationDesc,
								  relinfo->ri_IndexRelationDescs[i],
								  tup))
		{
			pfree(scan_keys[i]);
			scan_keys[i] = NULL;
			continue;
		}
	}
}

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 *
 * Returns whether any column contains NULLs.
 */
bool
build_index_scan_key(ScanKey skey, Relation rel, Relation idxrel, BDRTupleData *tup)
{
	int			attoff;
	Datum		indclassDatum;
	Datum		indkeyDatum;
	bool		isnull;
	oidvector  *opclass;
	int2vector  *indkey;
	bool		hasnulls = false;

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
			elog(ERROR,
				 "could not lookup equality operator for type %u, optype %u in opfamily %u",
				 atttype, optype, opfamily);

		regop = get_opcode(operator);

		/* FIXME: convert type? */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					tup->values[mainattno - 1]);

		if (tup->isnull[mainattno - 1])
		{
			hasnulls = true;
			skey[attoff].sk_flags |= SK_ISNULL;
		}
	}
	return hasnulls;
}

/*
 * Search the index 'idxrel' for a tuple identified by 'skey' in 'rel'.
 *
 * If a matching tuple is found setup 'tid' to point to it and return true,
 * false is returned otherwise.
 */
bool
find_pkey_tuple(ScanKey skey, BDRRelation *rel, Relation idxrel,
				TupleTableSlot *slot, bool lock, LockTupleMode mode)
{
	HeapTuple	scantuple;
	bool		found;
	IndexScanDesc scan;
	SnapshotData snap;
	TransactionId xwait;

	InitDirtySnapshot(snap);
	scan = index_beginscan(rel->rel, idxrel,
						   &snap,
						   RelationGetNumberOfAttributes(idxrel),
						   0);

retry:
	found = false;

	index_rescan(scan, skey, RelationGetNumberOfAttributes(idxrel), NULL, 0);

	if ((scantuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		found = true;
		/* FIXME: Improve TupleSlot to not require copying the whole tuple */
		ExecStoreTuple(scantuple, slot, InvalidBuffer, false);
		ExecMaterializeSlot(slot);

		xwait = TransactionIdIsValid(snap.xmin) ?
			snap.xmin : snap.xmax;

		if (TransactionIdIsValid(xwait))
		{
			XactLockTableWait(xwait, NULL, NULL, XLTW_None);
			goto retry;
		}
	}

	if (lock && found)
	{
		Buffer buf;
		HeapUpdateFailureData hufd;
		HTSU_Result res;
		HeapTupleData locktup;

		ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

		PushActiveSnapshot(GetLatestSnapshot());

		res = heap_lock_tuple(rel->rel, &locktup, GetCurrentCommandId(false), mode,
							  false /* wait */,
							  false /* don't follow updates */,
							  &buf, &hufd);
		/* the tuple slot already has the buffer pinned */
		ReleaseBuffer(buf);

		PopActiveSnapshot();

		switch (res)
		{
			case HeapTupleMayBeUpdated:
				break;
			case HeapTupleUpdated:
				/* XXX: Improve handling here */
				ereport(LOG,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("concurrent update, retrying")));
				goto retry;
			default:
				elog(ERROR, "unexpected HTSU_Result after locking: %u", res);
				break;
		}
	}

	index_endscan(scan);

	return found;
}

/*
 * bdr_queue_ddl_command
 *
 * Insert DDL command into the bdr.bdr_queued_commands table.
 */
void
bdr_queue_ddl_command(char *command_tag, char *command)
{
	EState		   *estate;
	TupleTableSlot *slot;
	RangeVar	   *rv;
	Relation		queuedcmds;
	HeapTuple		newtup = NULL;
	Datum			values[5];
	bool			nulls[5];

	elog(DEBUG2, "node " BDR_LOCALID_FORMAT " enqueuing DDL command \"%s\"",
		 BDR_LOCALID_FORMAT_ARGS, command);

	/* prepare bdr.bdr_queued_commands for insert */
	rv = makeRangeVar("bdr", "bdr_queued_commands", -1);
	queuedcmds = heap_openrv(rv, RowExclusiveLock);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(queuedcmds));
	estate = bdr_create_rel_estate(queuedcmds);
	ExecOpenIndices(estate->es_result_relation_info);

	/* lsn, queued_at, perpetrator, command_tag, command */
	values[0] = pg_current_xlog_location(NULL);
	values[1] = now(NULL);
	values[2] = PointerGetDatum(cstring_to_text(GetUserNameFromId(GetUserId())));
	values[3] = CStringGetTextDatum(command_tag);
	values[4] = CStringGetTextDatum(command);
	MemSet(nulls, 0, sizeof(nulls));

	newtup = heap_form_tuple(RelationGetDescr(queuedcmds), values, nulls);
	simple_heap_insert(queuedcmds, newtup);
	ExecStoreTuple(newtup, slot, InvalidBuffer, false);
	UserTableUpdateOpenIndexes(estate, slot);

	ExecCloseIndices(estate->es_result_relation_info);
	ExecDropSingleTupleTableSlot(slot);
	heap_close(queuedcmds, RowExclusiveLock);
}


/*
 * bdr_truncate_trigger_add
 *
 * This function adds TRUNCATE trigger to newly created tables.
 *
 * Note: it's important that this function be named so that it comes
 * after bdr_queue_ddl_commands when triggers are alphabetically sorted.
 */
Datum
bdr_truncate_trigger_add(PG_FUNCTION_ARGS)
{
	EventTriggerData   *trigdata;
	char			   *skip_ddl;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))  /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	/*
	 * If we're currently replaying something from a remote node, don't queue
	 * the commands; that would cause recursion.
	 */
	if (replication_origin_id != InvalidRepNodeId)
		PG_RETURN_VOID();	/* XXX return type? */

	/*
	 * Similarly, if configured to skip queueing DDL, don't queue.  This is
	 * mostly used when pg_restore brings a remote node state, so all objects
	 * will be copied over in the dump anyway.
	 */
	skip_ddl = GetConfigOptionByName("bdr.skip_ddl_replication", NULL);
	if (strcmp(skip_ddl, "on") == 0)
		PG_RETURN_VOID();

	trigdata = (EventTriggerData *) fcinfo->context;

	if (strcmp(trigdata->tag, "CREATE TABLE") == 0 &&
		IsA(trigdata->parsetree, CreateStmt))
	{
		CreateStmt *stmt = (CreateStmt *)trigdata->parsetree;
		char	   *nspname;
		char	   *query;
		int			res;

		/* Skip temporary and unlogged tables */
		if (stmt->relation->relpersistence != RELPERSISTENCE_PERMANENT)
			PG_RETURN_VOID();

		nspname = get_namespace_name(RangeVarGetCreationNamespace(stmt->relation));

		SPI_connect();

		query = psprintf("CREATE TRIGGER truncate_trigger AFTER TRUNCATE "
						 "ON %s.%s FOR EACH STATEMENT EXECUTE PROCEDURE "
						 "bdr.queue_truncate()",
						 quote_identifier(nspname),
						 quote_identifier(stmt->relation->relname));
		res = SPI_execute(query, false, 0);
		if (res != SPI_OK_UTILITY)
			elog(ERROR, "SPI failure: %d", res);

		/*
		 * If this is inside manually replicated DDL, the
		 * bdr_queue_ddl_commands will skip queueing the CREATE TRIGGER
		 * command, so we have to do it ourselves.
		 *
		 * XXX: The whole in_bdr_replicate_ddl_command concept is not very nice
		 */
		if (in_bdr_replicate_ddl_command)
			bdr_queue_ddl_command("CREATE TRIGGER", query);

		SPI_finish();
	}

	PG_RETURN_VOID();
}

#ifdef BUILDING_BDR
/*
 * bdr_queue_ddl_commands
 * 		ddl_command_end event triggger handler for BDR
 *
 * This function queues all commands reported in a replicated table, so that
 * they can be replayed by remote BDR nodes.
 */
Datum
bdr_queue_ddl_commands(PG_FUNCTION_ARGS)
{
	char   *skip_ddl;
	int		res;
	int		i;
	MemoryContext	tupcxt;
	uint32	nprocessed;
	SPITupleTable *tuptable;

	/*
	 * If the trigger comes from DDL executed by bdr_replicate_ddl_command,
	 * don't queue it as it would insert duplicate commands into the queue.
	 */
	if (in_bdr_replicate_ddl_command)
		PG_RETURN_VOID();	/* XXX return type? */

	/*
	 * If we're currently replaying something from a remote node, don't queue
	 * the commands; that would cause recursion.
	 */
	if (replication_origin_id != InvalidRepNodeId)
		PG_RETURN_VOID();	/* XXX return type? */

	/*
	 * Similarly, if configured to skip queueing DDL, don't queue.  This is
	 * mostly used when pg_restore brings a remote node state, so all objects
	 * will be copied over in the dump anyway.
	 */
	skip_ddl = GetConfigOptionByName("bdr.skip_ddl_replication", NULL);
	if (strcmp(skip_ddl, "on") == 0)
		PG_RETURN_VOID();

	/*
	 * Connect to SPI early, so that all memory allocated in this routine is
	 * released when we disconnect.  Also create a memory context that's reset
	 * for each iteration, to avoid per-tuple leakage.  Normally there would be
	 * very few tuples, but it's possible to create larger commands and it's
	 * pretty easy to fix the issue anyway.
	 */
	SPI_connect();
	tupcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "per-tuple DDL queue cxt",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);

	res = SPI_execute("SELECT "
					  "   command_tag, object_type, schema, identity, "
					  "   in_extension, "
					  "   pg_event_trigger_expand_command(command) AS command "
					  "FROM "
					  "   pg_catalog.pg_event_trigger_get_creation_commands()",
					  false, 0);
	if (res != SPI_OK_SELECT)
		elog(ERROR, "SPI query failed: %d", res);

	/*
	 * For each command row reported by the event trigger facility, insert zero
	 * or one row in the BDR queued commands table specifying how to replicate
	 * it.
	 */
	MemoryContextSwitchTo(tupcxt);
	nprocessed = SPI_processed;
	tuptable = SPI_tuptable;
	for (i = 0; i < nprocessed; i++)
	{
		Datum		cmdvalues[6];	/* # cols returned by above query */
		bool		cmdnulls[6];

		MemoryContextReset(tupcxt);

		/* this is the tuple reported by event triggers */
		heap_deform_tuple(tuptable->vals[i], tuptable->tupdesc,
						  cmdvalues, cmdnulls);

		/* if a temp object, ignore it */
		if (!cmdnulls[2] &&
			(strcmp(TextDatumGetCString(cmdvalues[2]), "pg_temp") == 0))
			continue;

		/* if in_extension, ignore the command */
		if (DatumGetBool(cmdvalues[4]))
			continue;

		bdr_queue_ddl_command(TextDatumGetCString(cmdvalues[0]),
							  TextDatumGetCString(cmdvalues[5]));
	}

	SPI_finish();

	PG_RETURN_VOID();
}

/*
 * bdr_queue_dropped_objects
 * 		sql_drop event triggger handler for BDR
 *
 * This function queues DROPs for replay by other BDR nodes.
 */
Datum
bdr_queue_dropped_objects(PG_FUNCTION_ARGS)
{
	char	   *skip_ddl;
	int			res;
	int			i;
	Oid			schema_oid;
	Oid			elmtype;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	int			droppedcnt = 0;
	Datum	   *droppedobjs;
	ArrayType  *droppedarr;
	TupleDesc	tupdesc;
	uint32		nprocessed;
	SPITupleTable *tuptable;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))  /* internal error */
		elog(ERROR, "%s: not fired by event trigger manager",
			 "bdr_queue_dropped_objects");

	/*
	 * If the trigger comes from DDL executed by bdr_replicate_ddl_command,
	 * don't queue it as it would insert duplicate commands into the queue.
	 */
	if (in_bdr_replicate_ddl_command)
		PG_RETURN_VOID();	/* XXX return type? */

	/*
	 * If we're currently replaying something from a remote node, don't queue
	 * the commands; that would cause recursion.
	 */
	if (replication_origin_id != InvalidRepNodeId)
		PG_RETURN_VOID();	/* XXX return type? */

	/*
	 * Similarly, if configured to skip queueing DDL, don't queue.  This is
	 * mostly used when pg_restore brings a remote node state, so all objects
	 * will be copied over in the dump anyway.
	 */
	skip_ddl = GetConfigOptionByName("bdr.skip_ddl_replication", NULL);
	if (strcmp(skip_ddl, "on") == 0)
		PG_RETURN_VOID();

	/*
	 * Connect to SPI early, so that all memory allocated in this routine is
	 * released when we disconnect.
	 */
	SPI_connect();

	res = SPI_execute("SELECT "
					  "   original, normal, object_type, "
					  "   address_names, address_args "
					  "FROM pg_event_trigger_dropped_objects()",
					  false, 0);
	if (res != SPI_OK_SELECT)
		elog(ERROR, "SPI query failed: %d", res);

	/*
	 * Build array of dropped objects based on the results of the query.
	 */
	nprocessed = SPI_processed;
	tuptable = SPI_tuptable;

	droppedobjs = (Datum *) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
											   sizeof(Datum) * nprocessed);

	schema_oid = get_namespace_oid("bdr", false);
	elmtype = bdr_lookup_relid("dropped_object", schema_oid);
	elmtype = get_rel_type_id(elmtype);

	get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
	tupdesc = TypeGetTupleDesc(elmtype, NIL);

	for (i = 0; i < nprocessed; i++)
	{
		Datum		cmdvalues[5];	/* # cols returned by above query */
		bool		cmdnulls[5];
		Datum		values[3];
		bool		nulls[3];
		HeapTuple	tuple;
		MemoryContext oldcontext;

		/* this is the tuple reported by event triggers */
		heap_deform_tuple(tuptable->vals[i], tuptable->tupdesc,
						  cmdvalues, cmdnulls);

		/* if not original or normal skip */
		if ((cmdnulls[0] || !DatumGetBool(cmdvalues[0])) &&
			(cmdnulls[1] || !DatumGetBool(cmdvalues[1])))
			continue;

		nulls[0] = cmdnulls[2];
		nulls[1] = cmdnulls[3];
		nulls[2] = cmdnulls[4];
		values[0] = cmdvalues[2];
		values[1] = cmdvalues[3];
		values[2] = cmdvalues[4];

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		tuple = heap_form_tuple(tupdesc, values, nulls);
		droppedobjs[droppedcnt] = HeapTupleGetDatum(tuple);
		droppedcnt++;
		MemoryContextSwitchTo(oldcontext);
	}

	SPI_finish();

	/* No objects dropped? */
	if (droppedcnt == 0)
		PG_RETURN_VOID();

	droppedarr = construct_array(droppedobjs, droppedcnt,
								 elmtype, elmlen, elmbyval, elmalign);

	/*
	 * Insert the dropped object(s) info into the bdr_queued_drops table
	 */
	{
		EState		   *estate;
		TupleTableSlot *slot;
		RangeVar	   *rv;
		Relation		queuedcmds;
		HeapTuple		newtup = NULL;
		Datum			values[5];
		bool			nulls[5];

		/*
		 * Prepare bdr.bdr_queued_drops for insert.
		 * Can't use preloaded table oid since this method is executed under
		 * normal backends and not inside BDR worker.
		 * The tuple slot here is only needed for updating indexes.
		 */
		rv = makeRangeVar("bdr", "bdr_queued_drops", -1);
		queuedcmds = heap_openrv(rv, RowExclusiveLock);
		slot = MakeSingleTupleTableSlot(RelationGetDescr(queuedcmds));
		estate = bdr_create_rel_estate(queuedcmds);
		ExecOpenIndices(estate->es_result_relation_info);

		/* lsn, queued_at, dropped_objects */
		values[0] = pg_current_xlog_location(NULL);
		values[1] = now(NULL);
		values[2] = PointerGetDatum(droppedarr);
		MemSet(nulls, 0, sizeof(nulls));

		newtup = heap_form_tuple(RelationGetDescr(queuedcmds), values, nulls);
		simple_heap_insert(queuedcmds, newtup);
		ExecStoreTuple(newtup, slot, InvalidBuffer, false);
		UserTableUpdateOpenIndexes(estate, slot);

		ExecCloseIndices(estate->es_result_relation_info);
		ExecDropSingleTupleTableSlot(slot);
		heap_close(queuedcmds, RowExclusiveLock);
	}

	PG_RETURN_VOID();
}
#endif /*BUILDING_BDR*/

/*
 * bdr_replicate_ddl_command
 *
 * Queues the input SQL for replication.
 *
 * Note that we don't allow CONCURRENTLY commands here, this is mainly because
 * we queue command before we actually execute it, which we currently need
 * to make the bdr_truncate_trigger_add work correctly. As written there
 * the in_bdr_replicate_ddl_command concept is ugly.
 */
Datum
bdr_replicate_ddl_command(PG_FUNCTION_ARGS)
{
	text	*command = PG_GETARG_TEXT_PP(0);
	char	*query = text_to_cstring(command);

	/* Force everything in the query to be fully qualified. */
	(void) set_config_option("search_path", "",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0);

	/* Execute the query locally. */
	in_bdr_replicate_ddl_command = true;

	PG_TRY();
		/* Queue the query for replication. */
		bdr_queue_ddl_command("SQL", query);

		/* Execute the query locally. */
		bdr_execute_ddl_command(query, GetUserNameFromId(GetUserId()), false);
	PG_CATCH();
		in_bdr_replicate_ddl_command = false;
		PG_RE_THROW();
	PG_END_TRY();

	in_bdr_replicate_ddl_command = false;

	PG_RETURN_VOID();
}

void
bdr_executor_always_allow_writes(bool always_allow)
{
	Assert(IsUnderPostmaster);
	bdr_always_allow_writes = always_allow;
}

/*
 * The BDR ExecutorStart_hook that does DDL lock checks and forbids
 * writing into tables without replica identity index.
 *
 * Runs in all backends and workers.
 */
static void
BdrExecutorStart(QueryDesc *queryDesc, int eflags)
{
	bool		performs_writes = false;
	ListCell   *l;
	List	   *rangeTable = queryDesc->plannedstmt->rtable;

	if (bdr_always_allow_writes)
		goto done;

	/* identify whether this is a modifying statement */
	if (queryDesc->plannedstmt != NULL &&
		queryDesc->plannedstmt->hasModifyingCTE)
		performs_writes = true;
	else if (queryDesc->operation != CMD_SELECT)
		performs_writes = true;

	if (!performs_writes)
		goto done;

	if (!bdr_is_bdr_activated_db(MyDatabaseId))
		goto done;

	/* check for concurrent global DDL locks */
	bdr_locks_check_dml();

	/* plain INSERTs are always ok beyond this point */
	if (queryDesc->operation == CMD_INSERT &&
		!queryDesc->plannedstmt->hasModifyingCTE)
		goto done;

	/* Fail if query tries to UPDATE or DELETE any of tables without PK */
	foreach(l, queryDesc->plannedstmt->resultRelations)
	{
		Index			rtei = lfirst_int(l);
		RangeTblEntry  *rte = rt_fetch(rtei, rangeTable);
		Relation		rel;

		rel = RelationIdGetRelation(rte->relid);

		/* Skip UNLOGGED and TEMP tables */
		if (!RelationNeedsWAL(rel))
		{
			RelationClose(rel);
			continue;
		}

		if (rel->rd_indexvalid == 0)
			RelationGetIndexList(rel);
		if (OidIsValid(rel->rd_replidindex))
		{
			RelationClose(rel);
			continue;
		}

		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Cannot run UPDATE or DELETE on table %s because it does not have primary key.",
						RelationGetRelationName(rel)),
				 errhint("Add primary key to the table")));

		RelationClose(rel);
	}

done:
	if (PrevExecutorStart_hook)
		(*PrevExecutorStart_hook) (queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}


void
bdr_executor_init(void)
{
	PrevExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = BdrExecutorStart;
}
