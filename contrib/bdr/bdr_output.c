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
#include "access/tuptoaster.h"

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

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void
write_tuple(StringInfo out, Relation rel, HeapTuple tuple)
{
	TupleDesc	desc;

	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];

	const char *nspname;
	int64		nspnamelen;
	const char *relname;
	int64		relnamelen;
	int			i;

	nspname = get_namespace_name(rel->rd_rel->relnamespace);
	if (nspname == NULL)
		elog(ERROR, "cache lookup failed for namespace %u",
			 rel->rd_rel->relnamespace);
	nspnamelen = strlen(nspname) + 1;

	relname = NameStr(rel->rd_rel->relname);
	relnamelen = strlen(relname) + 1;

	appendStringInfoChar(out, 'T');		/* tuple follows */

	pq_sendint(out, nspnamelen, 2);		/* schema name length */
	appendBinaryStringInfo(out, nspname, nspnamelen);

	pq_sendint(out, relnamelen, 2);		/* table name length */
	appendBinaryStringInfo(out, relname, relnamelen);

	desc = RelationGetDescr(rel);

	pq_sendint(out, desc->natts, 4);		/* number of attributes */

	/* try to allocate enough memory from the get go */
	enlargeStringInfo(out, tuple->t_len +
					  desc->natts * ( 1 + 4));

	/*
	 * XXX: should this prove to be a relevant bottleneck, it might be
	 * interesting to inline heap_deform_tuple() here, we don't actually need
	 * the information in the form we get from it.
	 */
	heap_deform_tuple(tuple, desc, values, isnull);

	for (i = 0; i < desc->natts; i++)
	{
		HeapTuple	typtup;
		Form_pg_type typclass;

		Form_pg_attribute att = desc->attrs[i];

		bool use_binary = false;
		bool use_sendrecv = false;

		if (isnull[i] || att->attisdropped)
		{
			appendStringInfoChar(out, 'n');	/* null column */
			continue;
		}
		else if (att->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(values[i]))
		{
			appendStringInfoChar(out, 'u');	/* unchanged toast column */
			continue;
		}

		typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(att->atttypid));
		if (!HeapTupleIsValid(typtup))
			elog(ERROR, "cache lookup failed for type %u", att->atttypid);
		typclass = (Form_pg_type) GETSTRUCT(typtup);

		/* builtin type */
		if (typclass->typtype == 'b' &&
			att->atttypid < FirstNormalObjectId &&
			typclass->typelem == InvalidOid)
			use_binary = true;
		else if (OidIsValid(typclass->typreceive))
			use_sendrecv = true;

		if (use_binary)
		{
			appendStringInfoChar(out, 'b');	/* binary data follows */

			/* pass by value */
			if (att->attbyval)
			{
				pq_sendint(out, att->attlen, 4); /* length */

				enlargeStringInfo(out, att->attlen);
				store_att_byval(out->data + out->len, values[i], att->attlen);
				out->len += att->attlen;
				out->data[out->len] = '\0';
			}
			/* fixed length non-varlena pass-by-reference type */
			else if (att->attlen > 0)
			{
				pq_sendint(out, att->attlen, 4); /* length */

				appendBinaryStringInfo(out, DatumGetPointer(values[i]),
									   att->attlen);
			}
			/* varlena type */
			else if (att->attlen == -1)
			{
				char *data = DatumGetPointer(values[i]);

				/* send indirect datums inline */
				if (VARATT_IS_EXTERNAL_INDIRECT(values[i]))
				{
					struct varatt_indirect redirect;
					VARATT_EXTERNAL_GET_POINTER(redirect, data);
					data = (char *) redirect.pointer;
				}

				Assert(!VARATT_IS_EXTERNAL(data));

				pq_sendint(out, VARSIZE_ANY(data), 4); /* length */

				appendBinaryStringInfo(out, data,
									   VARSIZE_ANY(data));

			}
			else
			{
				elog(ERROR, "unsupported tuple type");
			}
		}
		else if (use_sendrecv)
		{
			bytea	   *outputbytes;
			int			len;

			appendStringInfoChar(out, 's');	/* 'send' data follows */

			outputbytes =
				OidSendFunctionCall(typclass->typsend, values[i]);

			len = VARSIZE(outputbytes) - VARHDRSZ;
			pq_sendint(out, len, 4); /* length */
			pq_sendbytes(out, VARDATA(outputbytes), len); /* data */
			pfree(outputbytes);
		}
		else
		{
			char   	   *outputstr;
			int			len;

			appendStringInfoChar(out, 's');	/* 'text' data follows */

			outputstr =
				OidOutputFunctionCall(typclass->typoutput, values[i]);
			len = strlen(outputstr) + 1;
			pq_sendint(out, len, 4); /* length */
			appendBinaryStringInfo(out, outputstr, len); /* data */
			pfree(outputstr);
		}

		ReleaseSysCache(typtup);
	}
}
