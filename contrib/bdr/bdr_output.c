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
#include "utils/timestamp.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

extern void		_PG_init(void);
extern void		_PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef struct
{
	MemoryContext context;
	bool		include_xids;
} TestDecodingData;

/* These must be available to pg_dlsym() */
static void pg_decode_startup(LogicalDecodingContext * ctx, OutputPluginOptions *opt,
							  bool is_init);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);

/* private prototypes */
static void write_tuple(StringInfo out, Relation rel, HeapTuple tuple);

void
_PG_init(void)
{
}

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->commit_cb = pg_decode_commit_txn;
	cb->shutdown_cb = NULL;
}


/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext * ctx, OutputPluginOptions *opt, bool is_init)
{
	TestDecodingData *data;

	data = palloc(sizeof(TestDecodingData));
	data->context = AllocSetContextCreate(TopMemoryContext,
										  "bdr conversion context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
}

/* BEGIN callback */
void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
#ifdef NOT_YET
	TestDecodingData *data = ctx->output_plugin_private;
#endif
	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	if (txn->origin_id != InvalidRepNodeId)
		return;

	OutputPluginPrepareWrite(ctx, true);
	appendStringInfoChar(ctx->out, 'B');		/* BEGIN */
	appendBinaryStringInfo(ctx->out, (char *) &txn->final_lsn, sizeof(XLogRecPtr));
	appendBinaryStringInfo(ctx->out, (char *) &txn->commit_time, sizeof(TimestampTz));
	OutputPluginWrite(ctx, true);
	return;
}

/* COMMIT callback */
void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
#ifdef NOT_YET
	TestDecodingData *data = ctx->output_plugin_private;
#endif

	if (txn->origin_id != InvalidRepNodeId)
		return;

	OutputPluginPrepareWrite(ctx, true);
	appendStringInfoChar(ctx->out, 'C');		/* sending COMMIT */
	appendBinaryStringInfo(ctx->out, (char *) &commit_lsn, sizeof(XLogRecPtr));
	appendBinaryStringInfo(ctx->out, (char *) &txn->end_lsn, sizeof(XLogRecPtr));
	appendBinaryStringInfo(ctx->out, (char *) &txn->commit_time, sizeof(TimestampTz));
	OutputPluginWrite(ctx, true);
}

void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	TestDecodingData *data;
	MemoryContext old;

	data = ctx->output_plugin_private;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/* only log changes originating locally */
	if (txn->origin_id != InvalidRepNodeId)
		return;

	OutputPluginPrepareWrite(ctx, true);

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			appendStringInfoChar(ctx->out, 'I');		/* action INSERT */
			appendStringInfoChar(ctx->out, 'N');		/* new tuple follows */
			write_tuple(ctx->out, relation, &change->data.tp.newtuple->tuple);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			appendStringInfoChar(ctx->out, 'U');		/* action UPDATE */
			if (change->data.tp.oldtuple != NULL)
			{
				appendStringInfoChar(ctx->out, 'K');	/* old key follows */
				write_tuple(ctx->out, relation,
							&change->data.tp.oldtuple->tuple);
			}
			appendStringInfoChar(ctx->out, 'N');		/* new tuple follows */
			write_tuple(ctx->out, relation,
						&change->data.tp.newtuple->tuple);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			appendStringInfoChar(ctx->out, 'D');		/* action DELETE */
			if (change->data.tp.oldtuple != NULL)
			{
				appendStringInfoChar(ctx->out, 'K');	/* old key follows */
				write_tuple(ctx->out, relation,
							&change->data.tp.oldtuple->tuple);
			}
			else
				appendStringInfoChar(ctx->out, 'E');	/* empty */
			break;
		default:
			Assert(false);
	}
	OutputPluginWrite(ctx, true);

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}

static void
write_tuple(StringInfo out, Relation rel, HeapTuple tuple)
{
	HeapTuple	cache;
	Form_pg_namespace classNsp;
	const char *nspname;
	int64		nspnamelen;
	const char *relname;
	int64		relnamelen;

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

	appendStringInfoChar(out, 'T');		/* tuple follows */

	pq_sendint64(out, nspnamelen);		/* schema name length */
	appendBinaryStringInfo(out, nspname, nspnamelen);

	pq_sendint64(out, relnamelen);		/* table name length */
	appendBinaryStringInfo(out, relname, relnamelen);

	pq_sendint64(out, tuple->t_len);	/* tuple length */
	appendBinaryStringInfo(out, (char *) tuple->t_data, tuple->t_len);
}
