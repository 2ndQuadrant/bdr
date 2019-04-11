/*-------------------------------------------------------------------------
 *
 * bdr_output.c
 *		  BDR output plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_output.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "bdr.h"
#include "bdr_internal.h"
#include "miscadmin.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "catalog/catversion.h"
#include "catalog/index.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "executor/spi.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "nodes/parsenodes.h"

#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/replication_identifier.h"
#include "replication/slot.h"
#include "replication/walsender_private.h"

#include "storage/fd.h"
#include "storage/proc.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

extern void		_PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef struct
{
	MemoryContext context;

	uint64		remote_sysid;
	TimeLineID	remote_timeline;
	Oid			remote_dboid;

	bool allow_binary_protocol;
	bool allow_sendrecv_protocol;
	bool int_datetime_mismatch;
	bool forward_changesets;

	uint32 client_pg_version;
	uint32 client_pg_catversion;
	uint32 client_bdr_version;
	char *client_bdr_variant;
	uint32 client_min_bdr_version;
	size_t client_sizeof_int;
	size_t client_sizeof_long;
	size_t client_sizeof_datum;
	size_t client_maxalign;
	bool client_bigendian;
	bool client_float4_byval;
	bool client_float8_byval;
	bool client_int_datetime;
	char *client_db_encoding;
	Oid bdr_schema_oid;
	Oid bdr_conflict_handlers_reloid;
	Oid bdr_locks_reloid;
	Oid bdr_conflict_history_reloid;

	int num_replication_sets;
	char **replication_sets;
} BdrOutputData;

/* These must be available to pg_dlsym() */
static void pg_decode_startup(LogicalDecodingContext * ctx, OutputPluginOptions *opt,
							  bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext * ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);
static void pg_decode_message(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, XLogRecPtr message_lsn,
							  bool transactional, Size sz,
							  const char *message);

/* private prototypes */
static void write_rel(StringInfo out, Relation rel);
static void write_tuple(BdrOutputData *data, StringInfo out, Relation rel,
						HeapTuple tuple);

static void pglReorderBufferCleanSerializedTXNs(const char *slotname);

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->commit_cb = pg_decode_commit_txn;
	cb->message_cb = pg_decode_message;
	cb->shutdown_cb = pg_decode_shutdown;
}

/* Ensure a bdr_parse_... arg is non-null */
static void
bdr_parse_notnull(DefElem *elem, const char *paramtype)
{
	if (elem->arg == NULL || strVal(elem->arg) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s parameter \"%s\" had no value",
				 paramtype, elem->defname)));
}


static void
bdr_parse_uint32(DefElem *elem, uint32 *res)
{
	bdr_parse_notnull(elem, "uint32");
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
	bdr_parse_notnull(elem, "size_t");
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
	bdr_parse_notnull(elem, "bool");
	if (!parse_bool(strVal(elem->arg), res))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse boolean value \"%s\" for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));
}

static void
bdr_parse_identifier_list_arr(DefElem *elem, char ***list, int *len)
{
	List	   *namelist;
	ListCell   *c;

	bdr_parse_notnull(elem, "list");

	if (!SplitIdentifierString(pstrdup(strVal(elem->arg)),
							  ',', &namelist))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not identifier list value \"%s\" for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));
	}

	*len = 0;
	*list = palloc(list_length(namelist) * sizeof(char *));

	foreach(c, namelist)
	{
		(*list)[(*len)++] = pstrdup(lfirst(c));
	}
	list_free(namelist);
}

static void
bdr_parse_str(DefElem *elem, char **res)
{
	bdr_parse_notnull(elem, "string");
	*res = pstrdup(strVal(elem->arg));
}

static void
bdr_req_param(const char *param)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("missing value for for parameter \"%s\"",
					param)));
}

/*
 * Check bdr.bdr_nodes entry in local DB and if status != r
 * and we're trying to begin logical replay, raise an error.
 *
 * Also prevents slot creation if the BDR extension isn't installed in the
 * local node.
 *
 * If this function returns it's safe to begin replay.
 */
static void
bdr_ensure_node_ready(BdrOutputData *data)
{
	int spi_ret;
	const uint64 sysid = GetSystemIdentifier();
	char our_status;
	char remote_status;
	BDRNodeInfo *our_node;
	BDRNodeInfo *remote_node;
	NameData dbname;
	char *tmp_dbname;

	/* We need dbname valid outside this transaction, so copy it */
	tmp_dbname = get_database_name(MyDatabaseId);
	strncpy(NameStr(dbname), tmp_dbname, NAMEDATALEN);
	NameStr(dbname)[NAMEDATALEN-1] = '\0';
	pfree(tmp_dbname);

	/*
	 * Refuse to begin replication if the local node isn't yet ready to
	 * send data. Check the status in bdr.bdr_nodes.
	 */
	spi_ret = SPI_connect();
	if (spi_ret != SPI_OK_CONNECT)
		elog(ERROR, "Local SPI connect failed; shouldn't happen");
	our_node = bdr_nodes_get_local_info(sysid, ThisTimeLineID, MyDatabaseId,
										CurrentMemoryContext);
	remote_node = bdr_nodes_get_local_info(data->remote_sysid,
										   data->remote_timeline,
										   data->remote_dboid,
										   CurrentMemoryContext);

	our_status = our_node == NULL ? '\0' : our_node->status;
	remote_status = remote_node == NULL ? '\0' : remote_node->status;
	bdr_bdr_node_free(our_node);
	bdr_bdr_node_free(remote_node);

	SPI_finish();

	if (remote_status == 'k')
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bdr output plugin: slot usage rejected, node killed")));
	}

	/*
	 * Complain if node isn't ready,
	 * i.e. state is fully 'r'eady, or waiting for inbound sl'o't creation.
	 */
	/* TODO: Allow soft error so caller can sleep and recheck? */
	if (our_status != 'r' && our_status != 'o')
	{
		const char * const base_msg =
			"bdr output plugin: slot creation rejected, bdr.bdr_nodes entry for local node (sysid=" UINT64_FORMAT
			", timelineid=%u, dboid=%u): %s";
		switch (our_status)
		{
			case 'r':
			case 'o':
				break; /* unreachable */
			case '\0':
			case 'b':
				/*
				 * Can't allow replay when BDR hasn't started yet, as
				 * replica init might still need to run, causing a dump to
				 * be applied, catchup from a remote node, etc.
				 *
				 * If there's no init_replica set, the bdr extension will
				 * create a bdr.bdr_nodes entry with 'r' state shortly
				 * after it starts, so we won't hit this.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg(base_msg, sysid, ThisTimeLineID, MyDatabaseId,
								"does not exist"),
						 errdetail("BDR is not active on this database or is still initializing."),
						 errhint("Add bdr to shared_preload_libraries and check logs for bdr startup errors.")));
				break;
			case 'c':
				/*
				 * Can't allow replay while still catching up. We get the
				 * real origin node ID and LSN over the protocol in catchup
				 * mode, but changes are written to WAL with the ID and LSN
				 * of the immediate origin node. So if we cascade them to
				 * another node now, they'll incorrectly see the immediate
				 * origin node ID and LSN, not the true original ones.
				 *
				 * It should be possible to lift this restriction later,
				 * if we write the original node id and lsn in WAL.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg(base_msg, sysid, ThisTimeLineID, MyDatabaseId,
								"status='c', bdr still starting up: catching up from remote node"),
						 errhint("Monitor pg_stat_replication on the remote node, watch the logs and wait until the node has caught up")));
				break;
			case 'i':
				/*
				 * Can't allow replay while still applying a dump because the
				 * origin_id and origin_lsn are not preserved on the dump, so
				 * we'd replay all the changes. If the connections are from
				 * nodes that already have that data (or the origin node),
				 * that'll create a right mess.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg(base_msg, sysid, ThisTimeLineID, MyDatabaseId,
								"status='i', bdr still starting up: applying initial dump of remote node"),
						 errhint("Monitor pg_stat_activity and the logs, wait until the node has caught up")));
				break;
			case 'k':
				elog(ERROR, "node is exiting");
				break;

			default:
				elog(ERROR, "Unhandled case status=%c", our_status);
				break;
		}
	}
}


/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext * ctx, OutputPluginOptions *opt, bool is_init)
{
	ListCell	   *option;
	BdrOutputData  *data;
	Oid				schema_oid;
	bool			tx_started = false;
	Oid				local_dboid;

	data = palloc0(sizeof(BdrOutputData));
	data->context = AllocSetContextCreate(TopMemoryContext,
										  "bdr conversion context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	data->bdr_conflict_history_reloid = InvalidOid;
	data->bdr_conflict_handlers_reloid = InvalidOid;
	data->bdr_locks_reloid = InvalidOid;
	data->bdr_schema_oid = InvalidOid;
	data->num_replication_sets = -1;

	/* parse where the connection has to be from */
	bdr_parse_slot_name(NameStr(MyReplicationSlot->data.name),
						&data->remote_sysid,
						&data->remote_timeline,
						&data->remote_dboid,
						&local_dboid);

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
		else if (strcmp(elem->defname, "bdr_variant") == 0)
			bdr_parse_str(elem, &data->client_bdr_variant);
		else if (strcmp(elem->defname, "min_bdr_version") == 0)
			bdr_parse_uint32(elem, &data->client_min_bdr_version);
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
		else if (strcmp(elem->defname, "forward_changesets") == 0)
			bdr_parse_bool(elem, &data->forward_changesets);
		else if (strcmp(elem->defname, "unidirectional") == 0)
		{
			bool is_unidirectional;
			bdr_parse_bool(elem, &is_unidirectional);
			if (is_unidirectional)
				elog(ERROR, "support for unidirectional connections has been removed");
		}
		else if (strcmp(elem->defname, "replication_sets") == 0)
		{
			int i;

			/* parse list */
			bdr_parse_identifier_list_arr(elem,
										  &data->replication_sets,
										  &data->num_replication_sets);

			Assert(data->num_replication_sets >= 0);

			/* validate elements */
			for (i = 0; i < data->num_replication_sets; i++)
				bdr_validate_replication_set_name(data->replication_sets[i],
												  true);

			/* make it bsearch()able */
			qsort(data->replication_sets, data->num_replication_sets,
				  sizeof(char *), pg_qsort_strcmp);
		}
		else if (strcmp(elem->defname, "interactive") == 0)
		{
			/*
			 * Set defaults for interactive mode
			 *
			 * This is used for examining the replication queue from SQL.
			 */
			data->client_pg_version = PG_VERSION_NUM;
			data->client_pg_catversion = CATALOG_VERSION_NO;
			data->client_bdr_version = BDR_VERSION_NUM;
			data->client_bdr_variant = BDR_VARIANT;
			data->client_min_bdr_version = BDR_VERSION_NUM;
			data->client_sizeof_int = sizeof(int);
			data->client_sizeof_long = sizeof(long);
			data->client_sizeof_datum = sizeof(Datum);
			data->client_maxalign = MAXIMUM_ALIGNOF;
			data->client_bigendian = bdr_get_bigendian();
			data->client_float4_byval = bdr_get_float4byval();
			data->client_float8_byval = bdr_get_float8byval();
			data->client_int_datetime = bdr_get_integer_timestamps();
			data->client_db_encoding = pstrdup(GetDatabaseEncodingName());
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}

	if (!is_init)
	{
		/*
		 * There's a potential corruption bug in PostgreSQL 10.1, 9.6.6, 9.5.10
		 * and 9.4.15 that can cause reorder buffers to accumulate duplicated
		 * transactions. See
		 *   https://www.postgresql.org/message-id/CAMsr+YHdX=XECbZshDZ2CZNWGTyw-taYBnzqVfx4JzM4ExP5xg@mail.gmail.com
		 *
		 * We can defend against this by doing our own cleanup of any serialized
		 * txns in the reorder buffer on startup.
		 */
		pglReorderBufferCleanSerializedTXNs(NameStr(MyReplicationSlot->data.name));
	}

	/*
	 * Ensure that the BDR extension is installed on this database.
	 *
	 * We must prevent slot creation before the BDR extension is created,
	 * otherwise the event trigger for DDL replication will record the
	 * extension's creation in bdr.bdr_queued_commands and the slot position
	 * will be before then, causing CREATE EXTENSION to be replayed. Since
	 * the other end already has the BDR extension (obviously) this will
	 * cause replay to fail.
	 *
	 * TODO: Should really test for the extension its self, but this is faster
	 * and easier...
	 */
	if (!IsTransactionState())
	{
		tx_started = true;
		StartTransactionCommand();
	}

	/* BDR extension must be installed. */
	if (get_namespace_oid("bdr", true) == InvalidOid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bdr extension does not exist on " BDR_LOCALID_FORMAT,
						BDR_LOCALID_FORMAT_ARGS),
				 errdetail("Cannot create a BDR slot without the BDR extension installed")));
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
		if (data->client_min_bdr_version == 0)
			bdr_req_param("min_bdr_version");
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

		if (data->client_min_bdr_version > BDR_VERSION_NUM)
			elog(ERROR, "incompatible bdr client and server versions, server too old");
		if (data->client_bdr_version < BDR_MIN_REMOTE_VERSION_NUM)
			elog(ERROR, "incompatible bdr client and server versions, client too old");

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
		if (data->client_pg_version / 100 != PG_VERSION_NUM / 100)
			data->allow_sendrecv_protocol = false;

		bdr_maintain_schema(false);

		data->bdr_schema_oid = get_namespace_oid("bdr", true);
		schema_oid = data->bdr_schema_oid;

		if (schema_oid != InvalidOid)
		{
			data->bdr_conflict_handlers_reloid =
				get_relname_relid("bdr_conflict_handlers", schema_oid);

			if (data->bdr_conflict_handlers_reloid == InvalidOid)
				elog(ERROR, "cache lookup for relation bdr.bdr_conflict_handlers failed");
			else
				elog(DEBUG1, "bdr.bdr_conflict_handlers OID set to %u",
					 data->bdr_conflict_handlers_reloid);

			data->bdr_conflict_history_reloid =
				get_relname_relid("bdr_conflict_history", schema_oid);

			if (data->bdr_conflict_history_reloid == InvalidOid)
				elog(ERROR, "cache lookup for relation bdr.bdr_conflict_history failed");

			data->bdr_locks_reloid =
				get_relname_relid("bdr_global_locks", schema_oid);

			if (data->bdr_locks_reloid == InvalidOid)
				elog(ERROR, "cache lookup for relation bdr.bdr_locks failed");
		}
		else
			elog(WARNING, "cache lookup for schema bdr failed");

		/*
		 * Make sure it's safe to begin playing changes to the remote end.
		 * This'll ERROR out if we're not ready. Note that this does NOT
		 * prevent slot creation, only START_REPLICATION from the slot.
		 */
		bdr_ensure_node_ready(data);
	}

	if (tx_started)
		CommitTransactionCommand();

	/*
	 * Everything looks ok. Acquire a shmem slot to represent us running.
	 */
	{
		uint32 worker_idx;
		LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

		if (BdrWorkerCtl->worker_management_paused)
		{
			elog(ERROR, "BDR worker management is currently paused, walsender exiting. Retry later.");
			LWLockRelease(BdrWorkerCtl->lock);
		}

		bdr_worker_shmem_alloc(BDR_WORKER_WALSENDER, &worker_idx);
		bdr_worker_shmem_acquire(BDR_WORKER_WALSENDER, worker_idx, true);
		bdr_worker_slot->worker_pid = MyProcPid;
		bdr_worker_slot->worker_proc = MyProc;
		/* can be null if sql interface is used */
		bdr_worker_slot->data.walsnd.walsender = MyWalSnd;
		bdr_worker_slot->data.walsnd.slot = MyReplicationSlot;
		bdr_worker_slot->data.walsnd.remote_sysid = data->remote_sysid;
		bdr_worker_slot->data.walsnd.remote_timeline = data->remote_timeline;
		bdr_worker_slot->data.walsnd.remote_dboid = data->remote_dboid;

		LWLockRelease(BdrWorkerCtl->lock);
	}
}

static void
pg_decode_shutdown(LogicalDecodingContext * ctx)
{
	/* release and free slot */
	bdr_worker_shmem_release();
}

/*
 * Only changesets generated on the local node should be replicated
 * to the client unless we're in changeset forwarding mode.
 */
static inline bool
should_forward_changeset(LogicalDecodingContext *ctx, BdrOutputData *data,
						 ReorderBufferTXN *txn)
{
	return txn->origin_id == InvalidRepNodeId || data->forward_changesets;
}

static inline bool
should_forward_change(LogicalDecodingContext *ctx, BdrOutputData *data,
					  BDRRelation *r, enum ReorderBufferChangeType change)
{
	/* internal bdr relations that may not be replicated */
	if (RelationGetRelid(r->rel) == data->bdr_conflict_handlers_reloid ||
		RelationGetRelid(r->rel) == data->bdr_locks_reloid ||
		RelationGetRelid(r->rel) == data->bdr_conflict_history_reloid)
		return false;

	/*
	 * Quite ugly, but there's no neat way right now: Flush replication set
	 * configuration from bdr's relcache.
	 */
	if (RelationGetRelid(r->rel) == BdrReplicationSetConfigRelid)
		BDRRelcacheHashInvalidateCallback(0, InvalidOid);

	/* always replicate other stuff in the bdr schema */
	if (r->rel->rd_rel->relnamespace == data->bdr_schema_oid)
		return true;

	if (!r->computed_repl_valid)
		bdr_heap_compute_replication_settings(r,
											  data->num_replication_sets,
											  data->replication_sets);

	/* Check whether the current action is configured to be replicated */
	switch (change)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			return r->computed_repl_insert;
		case REORDER_BUFFER_CHANGE_UPDATE:
			return r->computed_repl_update;
		case REORDER_BUFFER_CHANGE_DELETE:
			return r->computed_repl_delete;
		default:
			elog(ERROR, "should be unreachable");
	}
}

/*
 * BEGIN callback
 *
 * If you change this you must also change the corresponding code in
 * bdr_apply.c . Make sure that any flags are in sync.
 */
void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	BdrOutputData *data = ctx->output_plugin_private;
	int flags = 0;

	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	if (!should_forward_changeset(ctx, data, txn))
		return;

	OutputPluginPrepareWrite(ctx, true);
	pq_sendbyte(ctx->out, 'B');		/* BEGIN */


	/*
	 * Are we forwarding changesets from other nodes? If so, we must include
	 * the origin node ID and LSN in BEGIN records.
	 */
	if (data->forward_changesets)
		flags |= BDR_OUTPUT_TRANSACTION_HAS_ORIGIN;

	/* send the flags field its self */
	pq_sendint(ctx->out, flags, 4);

	/* fixed fields */
	pq_sendint64(ctx->out, txn->final_lsn);
	pq_sendint64(ctx->out, txn->commit_time);
	pq_sendint(ctx->out, txn->xid, 4);

	/* and optional data selected above */
	if (flags & BDR_OUTPUT_TRANSACTION_HAS_ORIGIN)
	{
		/*
		 * The RepNodeId in txn->origin_id is our local identifier for the
		 * origin node, but it's not valid outside our node. It must be
		 * converted into the (sysid, tlid, dboid) that uniquely identifies the
		 * node globally so that can be sent.
		 */
		uint64		origin_sysid;
		TimeLineID	origin_tlid;
		Oid			origin_dboid = InvalidOid;

		bdr_fetch_sysid_via_node_id(txn->origin_id, &origin_sysid,
									&origin_tlid, &origin_dboid);

		pq_sendint64(ctx->out, origin_sysid);
		pq_sendint(ctx->out, origin_tlid, 4);
		pq_sendint(ctx->out, origin_dboid, 4);
		pq_sendint64(ctx->out, txn->origin_lsn);
	}

	OutputPluginWrite(ctx, true);
	return;
}

/*
 * COMMIT callback
 *
 * Send the LSN at the time of the commit, the commit time, and the end LSN.
 *
 * The presence of additional records is controlled by a flag field, with
 * records that're present appearing strictly in the order they're listed
 * here. There is no sub-record header or other structure beyond the flags
 * field.
 *
 * If you change this, you'll need to change process_remote_commit(...)
 * too. Make sure to keep any flags in sync.
 */
void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	BdrOutputData *data = ctx->output_plugin_private;

	int flags = 0;

	if (!should_forward_changeset(ctx, data, txn))
		return;

	OutputPluginPrepareWrite(ctx, true);
	pq_sendbyte(ctx->out, 'C');		/* sending COMMIT */

	/* send the flags field its self */
	pq_sendint(ctx->out, flags, 4);

	/* Send fixed fields */
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
	BDRRelation *bdr_relation;

	bdr_relation = bdr_heap_open(RelationGetRelid(relation), NoLock);

	data = ctx->output_plugin_private;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	if (!should_forward_changeset(ctx, data, txn))
		goto skip;

	if (!should_forward_change(ctx, data, bdr_relation, change->action))
		goto skip;

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

skip:
	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);

	bdr_heap_close(bdr_relation, NoLock);
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
	/* always disallow fancyness if there's type representation mismatches */
	if (data->int_datetime_mismatch &&
		(att->atttypid == TIMESTAMPOID || att->atttypid == TIMESTAMPTZOID ||
		 att->atttypid == TIMEOID))
	{
		*use_binary = false;
		*use_sendrecv = false;
	}
	/*
	 * Use the binary protocol, if allowed, for builtin & plain datatypes.
	 */
	else if (data->allow_binary_protocol &&
		typclass->typtype == 'b' &&
		att->atttypid < FirstNormalObjectId &&
		typclass->typelem == InvalidOid)
	{
		*use_binary = true;
	}
	/*
	 * Use send/recv, if allowed, if the type is plain or builtin.
	 *
	 * XXX: we can't use send/recv for array or composite types for now due to
	 * the embedded oids.
	 */
	else if (data->allow_sendrecv_protocol &&
			 OidIsValid(typclass->typreceive) &&
			 (att->atttypid < FirstNormalObjectId || typclass->typtype != 'c') &&
			 (att->atttypid < FirstNormalObjectId || typclass->typelem == InvalidOid))
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
				elog(ERROR, "unsupported tuple type");
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

			pq_sendbyte(out, 't');	/* 'text' data follows */

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

static void
pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr lsn,
				  bool transactional, Size sz,
				  const char *message)
{
	/*
	 * TODO: at some point we'll need several channels and filtering here..
	 */
	OutputPluginPrepareWrite(ctx, true);
	pq_sendbyte(ctx->out, 'M');	/* message follows */
	pq_sendbyte(ctx->out, transactional);
	pq_sendint64(ctx->out, lsn);
	pq_sendint(ctx->out, sz, 4);
	pq_sendbytes(ctx->out, message, sz);
	OutputPluginWrite(ctx, true);
}

/*
 * Clone of ReorderBufferCleanSerializedTXNs; see
 * https://www.postgresql.org/message-id/CAMsr+YHdX=XECbZshDZ2CZNWGTyw-taYBnzqVfx4JzM4ExP5xg@mail.gmail.com
 */
static void
pglReorderBufferCleanSerializedTXNs(const char *slotname)
{
	DIR		   *spill_dir = NULL;
	struct dirent *spill_de;
	struct stat statbuf;
	char		path[MAXPGPATH * 2 + 12];

	sprintf(path, "pg_replslot/%s", slotname);

	/* we're only handling directories here, skip if it's not our's */
	if (lstat(path, &statbuf) == 0 && !S_ISDIR(statbuf.st_mode))
		return;

	spill_dir = AllocateDir(path);
	while ((spill_de = ReadDir(spill_dir, path)) != NULL)
	{
		if (strcmp(spill_de->d_name, ".") == 0 ||
			strcmp(spill_de->d_name, "..") == 0)
			continue;

		/* only look at names that can be ours */
		if (strncmp(spill_de->d_name, "xid", 3) == 0)
		{
			sprintf(path, "pg_replslot/%s/%s", slotname,
					spill_de->d_name);

			if (unlink(path) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m",
								path)));
		}
	}
	FreeDir(spill_dir);
}
