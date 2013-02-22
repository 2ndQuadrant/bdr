/*-------------------------------------------------------------------------
 *
 * bdr_output.c
 *		  BDR output plugin
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/bdr/bdr_output.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"

#include "libpq/pqformat.h"

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


static void write_tuple(StringInfo out, Relation rel, HeapTuple tuple);

void
_PG_init(void)
{
}

/* initialize this plugin */
void
pg_decode_init(struct LogicalDecodingContext *ctx, bool is_init)
{
	TestDecodingData *data;

	AssertVariableIsOfType(&pg_decode_init, LogicalDecodeInitCB);

	data = palloc(sizeof(TestDecodingData));
	data->context = AllocSetContextCreate(TopMemoryContext,
								 "text conversion context",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);

	ctx->output_plugin_private = data;
}

/* BEGIN callback */
bool
pg_decode_begin_txn(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn)
{
	TestDecodingData *data = ctx->output_plugin_private;
	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	if (txn->origin_id != InvalidRepNodeId)
		return false;

	ctx->prepare_write(ctx, txn->lsn, txn->xid);
	appendStringInfoChar(ctx->out, 'B');/*  BEGIN */
	appendStringInfo(ctx->out, "BEGIN %u", txn->xid);
	ctx->write(ctx, txn->lsn, txn->xid);
	return true;
}

/* COMMIT callback */
bool
pg_decode_commit_txn(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	TestDecodingData *data = ctx->output_plugin_private;
	AssertVariableIsOfType(&pg_decode_commit_txn, LogicalDecodeCommitCB);

	if (txn->origin_id != InvalidRepNodeId)
		return false;

	ctx->prepare_write(ctx, txn->lsn, txn->xid);
	appendStringInfoChar(ctx->out, 'C');/* sending COMMIT */
	appendBinaryStringInfo(ctx->out, (char *)&commit_lsn, sizeof(XLogRecPtr));
	appendStringInfo(ctx->out, "COMMIT %u", txn->xid);
	ctx->write(ctx, txn->lsn, txn->xid);
	return true;
}

bool
pg_decode_change(struct LogicalDecodingContext *ctx, ReorderBufferTXN* txn,
				 Relation relation, ReorderBufferChange *change)
{
	TestDecodingData *data = ctx->output_plugin_private;
	MemoryContext context = data->context;
	/*
	 * switch to our own context we can reset after the tuple is printed,
	 * otherwise we will leak memory in via many of the output routines.
	 */
	MemoryContext old = MemoryContextSwitchTo(context);
	Relation index_rel = NULL;

	Assert(change->origin_id == txn->origin_id);
	AssertVariableIsOfType(&pg_decode_change, LogicalDecodeChangeCB);

	/* only log changes originating locally */
	if (txn->origin_id != InvalidRepNodeId)
		return false;

	ctx->prepare_write(ctx, change->lsn, txn->xid);

	switch (change->action)
	{
	case REORDER_BUFFER_CHANGE_INSERT:
		appendStringInfoChar(ctx->out, 'I');/* action INSERT */
		appendStringInfoChar(ctx->out, 'N');/* new tuple follows */
		write_tuple(ctx->out, relation, &change->newtuple->tuple);
		break;
	case REORDER_BUFFER_CHANGE_UPDATE:
		appendStringInfoChar(ctx->out, 'U');/* action UPDATE */

		if (change->oldtuple != NULL)
		{
			if (relation->rd_indexvalid == 0)
				RelationGetIndexList(relation);

			Assert(relation->rd_primary);
			index_rel = RelationIdGetRelation(relation->rd_primary);
			appendStringInfoChar(ctx->out, 'K');/* old key follows */
			write_tuple(ctx->out, index_rel, &change->oldtuple->tuple);
			RelationClose(index_rel);
		}
		appendStringInfoChar(ctx->out, 'N');/* new tuple follows */
		write_tuple(ctx->out, relation, &change->newtuple->tuple);
		break;
	case REORDER_BUFFER_CHANGE_DELETE:
		appendStringInfoChar(ctx->out, 'D');/* action DELETE */
		RelationGetIndexList(relation);
		if (relation->rd_primary != InvalidOid)
		{
			index_rel = RelationIdGetRelation(relation->rd_primary);
			appendStringInfoChar(ctx->out, 'K');/* old key follows */
			write_tuple(ctx->out, index_rel, &change->oldtuple->tuple);
			RelationClose(index_rel);
		}
		else
			appendStringInfoChar(ctx->out, 'E');/* empty */

		break;
	}
	ctx->write(ctx, change->lsn, txn->xid);

	MemoryContextSwitchTo(old);
	MemoryContextReset(context);
	return true;
}

static void
write_tuple(StringInfo out, Relation rel, HeapTuple tuple)
{
	HeapTuple cache;
	Form_pg_namespace classNsp;
	const char *nspname;
	int64 nspnamelen;
	const char *relname;
	int64 relnamelen;

	cache = SearchSysCache1(NAMESPACEOID,
							ObjectIdGetDatum(rel->rd_rel->relnamespace));
	if (!HeapTupleIsValid(cache))
		elog(ERROR, "cache lookup failed for namespace %u",
			 rel->rd_rel->relnamespace);
	classNsp = (Form_pg_namespace) GETSTRUCT(cache);
	nspname = pstrdup(NameStr(classNsp->nspname));
	nspnamelen = strlen(nspname) + 1;
	ReleaseSysCache(cache);

	relname = NameStr(rel->rd_rel->relname);
	relnamelen = strlen(relname) + 1;

	appendStringInfoChar(out, 'T');/* tuple follows */

	pq_sendint64(out, nspnamelen);	/* schema name length */
	appendBinaryStringInfo(out, nspname, nspnamelen);

	pq_sendint64(out, relnamelen);	/* table name length */
	appendBinaryStringInfo(out, relname, relnamelen);

	pq_sendint64(out, tuple->t_len);	/* tuple length */
	appendBinaryStringInfo(out, (char *)tuple->t_data, tuple->t_len);

}
