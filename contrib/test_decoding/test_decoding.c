/*-------------------------------------------------------------------------
 *
 * test_decoding.c
 *		  example output plugin for the logical replication functionality
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/test_decoding/test_decoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"

#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


PG_MODULE_MAGIC;

void _PG_init(void);

typedef struct
{
	MemoryContext context;
	bool include_xids;
} TestDecodingData;
extern void pg_decode_init(struct LogicalDecodingContext *ctx, bool is_init);

extern bool pg_decode_begin_txn(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn);
extern bool pg_decode_commit_txn(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn);
extern bool pg_decode_change(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn, Relation rel, ReorderBufferChange *change);

void
_PG_init(void)
{
}

/* initialize this plugin */
void
pg_decode_init(struct LogicalDecodingContext *ctx, bool is_init)
{
	ListCell *option;
	TestDecodingData *data;

	AssertVariableIsOfType(&pg_decode_init, LogicalDecodeInitCB);

	data = palloc(sizeof(TestDecodingData));
	data->context = AllocSetContextCreate(TopMemoryContext,
								 "text conversion context",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);
	data->include_xids = true;

	ctx->output_plugin_private = data;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem *elem = lfirst(option);
		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "hide-xids") == 0)
		{
			/* FIXME: parse argument */
			data->include_xids = false;
		}
		else
		{
			elog(WARNING, "option %s = %s is unknown",
				 elem->defname, elem->arg ? strVal(elem->arg) : "(null)");
		}
	}
}

/* BEGIN callback */
bool
pg_decode_begin_txn(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn)
{
	TestDecodingData *data = ctx->output_plugin_private;
	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	ctx->prepare_write(ctx, txn->lsn, txn->xid);
	if (data->include_xids)
		appendStringInfo(ctx->out, "BEGIN %u", txn->xid);
	else
		appendStringInfoString(ctx->out, "BEGIN");
	ctx->write(ctx, txn->lsn, txn->xid);
	return true;
}

/* COMMIT callback */
bool
pg_decode_commit_txn(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	TestDecodingData *data = ctx->output_plugin_private;
	AssertVariableIsOfType(&pg_decode_commit_txn, LogicalDecodeCommitCB);

	ctx->prepare_write(ctx, txn->lsn, txn->xid);
	if (data->include_xids)
		appendStringInfo(ctx->out, "COMMIT %u", txn->xid);
	else
		appendStringInfoString(ctx->out, "COMMIT");
	ctx->write(ctx, txn->lsn, txn->xid);
	return true;
}

/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
	int	natt;
	Oid oid;

	/* print oid of tuple, it's not included in the TupleDesc */
	if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid)
	{
		appendStringInfo(s, " oid[oid]:%u", oid);
	}

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid; /* type of current attribute */
		HeapTuple	type_tuple; /* information about a type */
		Form_pg_type type_form;
		Oid			typoutput; /* output function */
		bool		typisvarlena;
		Datum		origval; /* possibly toasted Datum */
		Datum		val; /* definitely detoasted Datum */
		char        *outputstr = NULL;
		bool        isnull; /* column is null? */

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
 * callback for individual changed tuples
 */
bool
pg_decode_change(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn,
				 Relation relation, ReorderBufferChange *change)
{
	TestDecodingData *data = ctx->output_plugin_private;

	Form_pg_class class_form = RelationGetForm(relation);
	TupleDesc	tupdesc = RelationGetDescr(relation);
	MemoryContext context = data->context;
	/*
	 * switch to our own context we can reset after the tuple is printed,
	 * otherwise we will leak memory in via many of the output routines.
	 */
	MemoryContext old = MemoryContextSwitchTo(context);

	AssertVariableIsOfType(&pg_decode_change, LogicalDecodeChangeCB);

	ctx->prepare_write(ctx, change->lsn, txn->xid);

	appendStringInfoString(ctx->out, "table \"");
	appendStringInfoString(ctx->out, NameStr(class_form->relname));
	appendStringInfoString(ctx->out, "\":");

	switch (change->action)
	{
	case REORDER_BUFFER_CHANGE_INSERT:
		appendStringInfoString(ctx->out, " INSERT:");
		if (change->newtuple == NULL)
			appendStringInfoString(ctx->out, " (no-tuple-data)");
		else
			tuple_to_stringinfo(ctx->out, tupdesc, &change->newtuple->tuple);
		break;
	case REORDER_BUFFER_CHANGE_UPDATE:
		appendStringInfoString(ctx->out, " UPDATE:");
		if (change->oldtuple != NULL)
		{
			Relation indexrel;
			TupleDesc	indexdesc;

			appendStringInfoString(ctx->out, " old-pkey:");
			RelationGetIndexList(relation);

			if (!OidIsValid(relation->rd_primary))
			{
				elog(LOG, "tuple in table with oid: %u without primary key",
				     RelationGetRelid(relation));
				break;
			}

			indexrel = RelationIdGetRelation(relation->rd_primary);

			indexdesc = RelationGetDescr(indexrel);

			tuple_to_stringinfo(ctx->out, indexdesc, &change->oldtuple->tuple);

			RelationClose(indexrel);
			appendStringInfoString(ctx->out, " new-tuple:");
		}

		if (change->newtuple == NULL)
			appendStringInfoString(ctx->out, " (no-tuple-data)");
		else
			tuple_to_stringinfo(ctx->out, tupdesc, &change->newtuple->tuple);

		break;
	case REORDER_BUFFER_CHANGE_DELETE:
		{
			Relation indexrel;
			TupleDesc	indexdesc;

			appendStringInfoString(ctx->out, " DELETE:");

			if (change->oldtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			else
			{
				/*
				 * deletions only store the primary key part of the tuple,
				 * display that index.
				 */

				/* make sure rd_primary is set */
				RelationGetIndexList(relation);

				if (!OidIsValid(relation->rd_primary))
				{
					elog(LOG, "tuple in table with oid: %u without primary key",
					     RelationGetRelid(relation));
					break;
				}

				indexrel = RelationIdGetRelation(relation->rd_primary);

				indexdesc = RelationGetDescr(indexrel);

				tuple_to_stringinfo(ctx->out, indexdesc, &change->oldtuple->tuple);

				RelationClose(indexrel);
			}
			break;
		}
	}

	MemoryContextSwitchTo(old);
	MemoryContextReset(context);

	ctx->write(ctx, change->lsn, txn->xid);
	return true;
}
