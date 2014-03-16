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

#include "bdr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"

#include "utils/builtins.h"
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

	bool allow_binary_protocol;
	bool allow_sendrecv_protocol;
	bool int_datetime_mismatch;

	uint32 client_pg_version;
	uint32 client_pg_catversion;
	uint32 client_bdr_version;
	size_t client_sizeof_int;
	size_t client_sizeof_long;
	size_t client_sizeof_datum;
	size_t client_maxalign;
	bool client_bigendian;
	bool client_float4_byval;
	bool client_float8_byval;
	bool client_int_datetime;
	char *client_db_encoding;
} BdrOutputData;

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
static void write_rel(StringInfo out, Relation rel);
static void write_tuple(BdrOutputData *data, StringInfo out, Relation rel,
						HeapTuple tuple);

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


static void
bdr_parse_uint32(DefElem *elem, uint32 *res)
{
	errno = 0;
	*res = strtoul(strVal(elem->arg), NULL, 0);

	if (errno != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse uint32 value \"%s\" for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));
}

static void
bdr_parse_size_t(DefElem *elem, size_t *res)
{
	errno = 0;
	*res = strtoull(strVal(elem->arg), NULL, 0);

	if (errno != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse size_t value \"%s\" for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));
}

static void
bdr_parse_bool(DefElem *elem, bool *res)
{
	if (!parse_bool(strVal(elem->arg), res))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse boolean value \"%s\" for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));
}

static void
bdr_req_param(const char *param)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("missing value for for parameter \"%s\"",
					param)));
}


/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext * ctx, OutputPluginOptions *opt, bool is_init)
{
	ListCell   *option;
	BdrOutputData *data;

	data = palloc0(sizeof(BdrOutputData));
	data->context = AllocSetContextCreate(TopMemoryContext,
										  "bdr conversion context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	/* parse options passed in by the client */

	foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "pg_version") == 0)
			bdr_parse_uint32(elem, &data->client_pg_version);
		else if (strcmp(elem->defname, "pg_catversion") == 0)
			bdr_parse_uint32(elem, &data->client_pg_catversion);
		else if (strcmp(elem->defname, "bdr_version") == 0)
			bdr_parse_uint32(elem, &data->client_bdr_version);
		else if (strcmp(elem->defname, "sizeof_int") == 0)
			bdr_parse_size_t(elem, &data->client_sizeof_int);
		else if (strcmp(elem->defname, "sizeof_long") == 0)
			bdr_parse_size_t(elem, &data->client_sizeof_long);
		else if (strcmp(elem->defname, "sizeof_datum") == 0)
			bdr_parse_size_t(elem, &data->client_sizeof_datum);
		else if (strcmp(elem->defname, "maxalign") == 0)
			bdr_parse_size_t(elem, &data->client_maxalign);
		else if (strcmp(elem->defname, "bigendian") == 0)
			bdr_parse_bool(elem, &data->client_bigendian);
		else if (strcmp(elem->defname, "float4_byval") == 0)
			bdr_parse_bool(elem, &data->client_float4_byval);
		else if (strcmp(elem->defname, "float8_byval") == 0)
			bdr_parse_bool(elem, &data->client_float8_byval);
		else if (strcmp(elem->defname, "integer_datetimes") == 0)
			bdr_parse_bool(elem, &data->client_int_datetime);
		else if (strcmp(elem->defname, "db_encoding") == 0)
			data->client_db_encoding = pstrdup(strVal(elem->arg));
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}

	/* no options are passed in during initialization, so don't complain there */
	if (!is_init)
	{
		if (data->client_pg_version == 0)
			bdr_req_param("pg_version");
		if (data->client_pg_catversion == 0)
			bdr_req_param("pg_catversion");
		if (data->client_bdr_version == 0)
			bdr_req_param("bdr_version");
		if (data->client_sizeof_int == 0)
			bdr_req_param("sizeof_int");
		if (data->client_sizeof_long == 0)
			bdr_req_param("sizeof_long");
		if (data->client_sizeof_datum == 0)
			bdr_req_param("sizeof_datum");
		if (data->client_maxalign == 0)
			bdr_req_param("maxalign");
		/* XXX: can't check for boolean values this way */
		if (data->client_db_encoding == NULL)
			bdr_req_param("db_encoding");

		/* check incompatibilities we cannot work around */
		if (strcmp(data->client_db_encoding, GetDatabaseEncodingName()) != 0)
			elog(ERROR, "mismatching encodings are not yet supported");

		if (data->client_bdr_version != BDR_VERSION_NUM)
			elog(ERROR, "bdr versions currently have to match on both sides");

		data->allow_binary_protocol = true;
		data->allow_sendrecv_protocol = true;

		/*
		 * Now use the passed in information to determine how to encode the
		 * data sent by the output plugin. We don't make datatype specific
		 * decisions here, just generic decisions about using binary and/or
		 * send/recv protocols.
		 */

		/*
		 * Don't use the binary protocol if there are fundamental arch
		 * differences.
		 */
		if (data->client_sizeof_int != sizeof(int) ||
			data->client_sizeof_long != sizeof(long) ||
			data->client_sizeof_datum != sizeof(Datum))
		{
			data->allow_binary_protocol = false;
			elog(LOG, "disabling binary protocol because of sizeof differences");
		}
		else if (data->client_bigendian != bdr_get_bigendian())
		{
			data->allow_binary_protocol = false;
			elog(LOG, "disabling binary protocol because of endianess difference");
		}

		/*
		 * We also can't use the binary protocol if there are critical
		 * differences in compile time settings.
		 */
		if (data->client_float4_byval != bdr_get_float4byval() ||
			data->client_float8_byval != bdr_get_float8byval())
			data->allow_binary_protocol = false;

		if (data->client_int_datetime != bdr_get_integer_timestamps())
			data->int_datetime_mismatch = true;
		else
			data->int_datetime_mismatch = false;


		/*
		 * Don't use the send/recv protocol if there are version
		 * differences. There currently isn't any guarantee for cross version
		 * compatibility of the send/recv representations. But there actually
		 * *is* a compat. guarantee for architecture differences...
		 *
		 * XXX: We could easily do better by doing per datatype considerations
		 * if there are known incompatibilities.
		 */
		if (data->client_pg_version / 100 == PG_VERSION_NUM / 100)
			data->allow_sendrecv_protocol = false;
	}
}

/* BEGIN callback */
void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
#ifdef NOT_YET
	BdrOutputData *data = ctx->output_plugin_private;
#endif
	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	if (txn->origin_id != InvalidRepNodeId)
		return;

	OutputPluginPrepareWrite(ctx, true);
	pq_sendbyte(ctx->out, 'B');		/* BEGIN */
	pq_sendint64(ctx->out, txn->final_lsn);
	pq_sendint64(ctx->out, txn->commit_time);
	OutputPluginWrite(ctx, true);
	return;
}

/* COMMIT callback */
void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
#ifdef NOT_YET
	BdrOutputData *data = ctx->output_plugin_private;
#endif

	if (txn->origin_id != InvalidRepNodeId)
		return;

	OutputPluginPrepareWrite(ctx, true);
	pq_sendbyte(ctx->out, 'C');		/* sending COMMIT */
	pq_sendint64(ctx->out, commit_lsn);
	pq_sendint64(ctx->out, txn->end_lsn);
	pq_sendint64(ctx->out, txn->commit_time);
	OutputPluginWrite(ctx, true);
}

void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	BdrOutputData *data;
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
			pq_sendbyte(ctx->out, 'I');		/* action INSERT */
			write_rel(ctx->out, relation);
			pq_sendbyte(ctx->out, 'N');		/* new tuple follows */
			write_tuple(data, ctx->out, relation, &change->data.tp.newtuple->tuple);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			pq_sendbyte(ctx->out, 'U');		/* action UPDATE */
			write_rel(ctx->out, relation);
			if (change->data.tp.oldtuple != NULL)
			{
				pq_sendbyte(ctx->out, 'K');	/* old key follows */
				write_tuple(data, ctx->out, relation,
							&change->data.tp.oldtuple->tuple);
			}
			pq_sendbyte(ctx->out, 'N');		/* new tuple follows */
			write_tuple(data, ctx->out, relation,
						&change->data.tp.newtuple->tuple);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			pq_sendbyte(ctx->out, 'D');		/* action DELETE */
			write_rel(ctx->out, relation);
			if (change->data.tp.oldtuple != NULL)
			{
				pq_sendbyte(ctx->out, 'K');	/* old key follows */
				write_tuple(data, ctx->out, relation,
							&change->data.tp.oldtuple->tuple);
			}
			else
				pq_sendbyte(ctx->out, 'E');	/* empty */
			break;
		default:
			Assert(false);
	}
	OutputPluginWrite(ctx, true);

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}

/*
 * Write schema.relation to the output stream.
 */
static void
write_rel(StringInfo out, Relation rel)
{
	const char *nspname;
	int64		nspnamelen;
	const char *relname;
	int64		relnamelen;

	nspname = get_namespace_name(rel->rd_rel->relnamespace);
	if (nspname == NULL)
		elog(ERROR, "cache lookup failed for namespace %u",
			 rel->rd_rel->relnamespace);
	nspnamelen = strlen(nspname) + 1;

	relname = NameStr(rel->rd_rel->relname);
	relnamelen = strlen(relname) + 1;

	pq_sendint(out, nspnamelen, 2);		/* schema name length */
	appendBinaryStringInfo(out, nspname, nspnamelen);

	pq_sendint(out, relnamelen, 2);		/* table name length */
	appendBinaryStringInfo(out, relname, relnamelen);
}

/*
 * Make the executive decision about which protocol to use.
 */
static void
decide_datum_transfer(BdrOutputData *data,
					  Form_pg_attribute att, Form_pg_type typclass,
					  bool *use_binary, bool *use_sendrecv)
{
	/* builtin type */
	if (data->int_datetime_mismatch &&
		(att->atttypid == TIMESTAMPOID || att->atttypid == TIMESTAMPTZOID ||
		 att->atttypid == TIMEOID))
	{
		*use_binary = false;
		*use_sendrecv = false;
	}
	else if (data->allow_binary_protocol &&
		typclass->typtype == 'b' &&
		att->atttypid < FirstNormalObjectId &&
		typclass->typelem == InvalidOid)
	{
		*use_binary = true;
	}
	else if (data->allow_sendrecv_protocol &&
			 OidIsValid(typclass->typreceive))
	{
		*use_sendrecv = true;
	}
}

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void
write_tuple(BdrOutputData *data, StringInfo out, Relation rel,
			HeapTuple tuple)
{
	TupleDesc	desc;
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];
	int			i;

	desc = RelationGetDescr(rel);

	pq_sendbyte(out, 'T');			/* tuple follows */

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
			pq_sendbyte(out, 'n');	/* null column */
			continue;
		}
		else if (att->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(values[i]))
		{
			pq_sendbyte(out, 'u');	/* unchanged toast column */
			continue;
		}

		typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(att->atttypid));
		if (!HeapTupleIsValid(typtup))
			elog(ERROR, "cache lookup failed for type %u", att->atttypid);
		typclass = (Form_pg_type) GETSTRUCT(typtup);

		decide_datum_transfer(data, att, typclass, &use_binary, &use_sendrecv);

		if (use_binary)
		{
			pq_sendbyte(out, 'b');	/* binary data follows */

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

			pq_sendbyte(out, 's');	/* 'send' data follows */

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

			pq_sendbyte(out, 's');	/* 'text' data follows */

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
