/*-------------------------------------------------------------------------
 *
 * logicalfuncs.c
 *
 *     Support functions for using xlog decoding
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logicalfuncs.c
 *
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"

#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/snapbuild.h"


/*
 * Return one row for each logical replication slot currently in use.
 */

Datum
pg_stat_get_logical_replication_slots(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_LOGICAL_REPLICATION_SLOTS_COLS 6
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int			i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < max_logical_slots; i++)
	{
		LogicalDecodingSlot *slot = &LogicalDecodingCtl->logical_slots[i];
		Datum values[PG_STAT_GET_LOGICAL_REPLICATION_SLOTS_COLS];
		bool nulls[PG_STAT_GET_LOGICAL_REPLICATION_SLOTS_COLS];
		char location[MAXFNAMELEN];
		const char *slot_name;
		const char *plugin;
		TransactionId xmin;
		XLogRecPtr last_req;
		bool active;
		Oid database;

		SpinLockAcquire(&slot->mutex);
		if (!slot->in_use)
		{
			SpinLockRelease(&slot->mutex);
			continue;
		}
		else
		{
			xmin = slot->xmin;
			active = slot->active;
			database = slot->database;
			last_req = slot->last_required_checkpoint;
			slot_name = pstrdup(NameStr(slot->name));
			plugin = pstrdup(NameStr(slot->plugin));
		}
		SpinLockRelease(&slot->mutex);

		memset(nulls, 0, sizeof(nulls));

		snprintf(location, sizeof(location), "%X/%X",
				 (uint32) (last_req >> 32), (uint32) last_req);

		values[0] = CStringGetTextDatum(slot_name);
		values[1] = CStringGetTextDatum(plugin);
		values[2] = database;
		values[3] = BoolGetDatum(active);
		values[4] = TransactionIdGetDatum(xmin);
		values[5] = CStringGetTextDatum(location);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
