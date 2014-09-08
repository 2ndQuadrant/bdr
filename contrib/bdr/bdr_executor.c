/* -------------------------------------------------------------------------
 *
 * bdr_executor.c
 *      Relation and index access and maintenance routines required by bdr
 *
 * BDR does a lot of direct access to indexes and relations, some of which
 * isn't handled by simple calls into the backend. Most of it lives here.
 *
 * Copyright (C) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      contrib/bdr/bdr_executor.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "bdr.h"

#include "access/heapam.h"
#include "access/skey.h"
#include "access/xact.h"
#include "access/xlog_fn.h"

#include "catalog/pg_trigger.h"

#include "commands/trigger.h"

#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"

#include "miscadmin.h"

#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"

#include "parser/parse_relation.h"

#include "storage/bufmgr.h"
#include "storage/lmgr.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


PG_FUNCTION_INFO_V1(bdr_queue_ddl_commands);

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
build_index_scan_keys(EState *estate, ScanKey *scan_keys, TupleTableSlot *slot)
{
	ResultRelInfo *relinfo;
	int i;

	relinfo = estate->es_result_relation_info;

	/* build scankeys for each index */
	for (i = 0; i < relinfo->ri_NumIndices; i++)
	{
		IndexInfo  *ii = relinfo->ri_IndexRelationInfo[i];

		if (!ii->ii_Unique)
			continue;

		scan_keys[i] = palloc(ii->ii_NumIndexAttrs * sizeof(ScanKeyData));
		build_index_scan_key(scan_keys[i],
					   relinfo->ri_RelationDesc,
					   relinfo->ri_IndexRelationDescs[i],
					   slot);
	}
}

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 */
void
build_index_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleTableSlot *slot)
{
	int			attoff;
	Datum		indclassDatum;
	Datum		indkeyDatum;
	bool		isnull;
	oidvector  *opclass;
	int2vector  *indkey;
	HeapTuple	key = slot->tts_tuple;

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
 * bdr_queue_ddl_commands
 * 		ddl_command_end event triggger handler for BDR
 *
 * This function queues all commands reported in a replicated table, so that
 * they can be replayed by remote BDR nodes.
 */
Datum
bdr_queue_ddl_commands(PG_FUNCTION_ARGS)
{
	EState  *estate;
	TupleTableSlot	*slot;
	RangeVar *rv;
	Relation queuedcmds;
	char   *skip_ddl;
	int		res;
	int		i;
	MemoryContext	tupcxt;

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

	/* prepare bdr.bdr_queued_commands for insert */
	rv = makeRangeVar("bdr", "bdr_queued_commands", -1);
	queuedcmds = heap_openrv(rv, RowExclusiveLock);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(queuedcmds));
	estate = bdr_create_rel_estate(queuedcmds);
	ExecOpenIndices(estate->es_result_relation_info);

	/*
	 * For each command row reported by the event trigger facility, insert zero
	 * or one row in the BDR queued commands table specifying how to replicate
	 * it.
	 */
	MemoryContextSwitchTo(tupcxt);
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	newtup = NULL;
		Datum		cmdvalues[6];	/* # cols returned by above query */
		bool		cmdnulls[6];
		Datum		values[5];		/* # cols in bdr_queued_commands */
		bool		nulls[5];

		MemoryContextReset(tupcxt);

		/* this is the tuple reported by event triggers */
		heap_deform_tuple(SPI_tuptable->vals[i], SPI_tuptable->tupdesc,
						  cmdvalues, cmdnulls);

		/* if a temp object, ignore it */
		if (!cmdnulls[2] &&
			(strcmp(TextDatumGetCString(cmdvalues[1]), "pg_temp") == 0))
			continue;

		/* if in_extension, ignore the command */
		if (DatumGetBool(cmdvalues[4]))
			continue;

		/* lsn, queued_at, perpetrator, command_tag, command */
		values[0] = pg_current_xlog_location(NULL);
		values[1] = now(NULL);
		values[2] = PointerGetDatum(cstring_to_text(GetUserNameFromId(GetUserId())));
		values[3] = cmdvalues[0];
		values[4] = cmdvalues[5];
		MemSet(nulls, 0, sizeof(nulls));

		newtup = heap_form_tuple(RelationGetDescr(queuedcmds), values, nulls);
		simple_heap_insert(queuedcmds, newtup);
		ExecStoreTuple(newtup, slot, InvalidBuffer, false);
		UserTableUpdateOpenIndexes(estate, slot);

		/*
		 * If we're creating a table, attach a per-stmt trigger to it too, so
		 * that whenever a TRUNCATE is executed in a node, it's replicated to
		 * all other nodes.
		 */
		if ((strcmp(TextDatumGetCString(cmdvalues[0]), "CREATE TABLE") == 0) &&
			(strcmp(TextDatumGetCString(cmdvalues[1]), "table") == 0))
		{
			char	*stmt;

			/* The table identity is already quoted */
			stmt = psprintf("CREATE TRIGGER truncate_trigger AFTER TRUNCATE "
							"ON %s FOR EACH STATEMENT EXECUTE PROCEDURE "
							"bdr.queue_truncate()",
							TextDatumGetCString(cmdvalues[3]));
			res = SPI_execute(stmt, false, 0);
			if (res != SPI_OK_UTILITY)
				elog(ERROR, "SPI failure: %d", res);
		}
	}

	ExecCloseIndices(estate->es_result_relation_info);
	ExecDropSingleTupleTableSlot(slot);
	heap_close(queuedcmds, RowExclusiveLock);

	SPI_finish();

	PG_RETURN_VOID();
}
