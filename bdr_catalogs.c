/* -------------------------------------------------------------------------
 *
 * bdr_catalogs.c
 *		Access to bdr catalog information like bdr.bdr_nodes
 *
 * Functions usable by both the output plugin and the extension/workers for
 * accessing and manipulating BDR's catalogs, like bdr.bdr_nodes.
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "bdr.h"
#include "miscadmin.h"

#include "access/xact.h"

#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "executor/spi.h"

#include "nodes/makefuncs.h"

#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

static int getattno(const char *colname);
static char* bdr_textarr_to_identliststr(ArrayType *textarray);


/* GetSysCacheOid equivalent that errors out if nothing is found */
Oid
GetSysCacheOidError(int cacheId,
					Datum key1,
					Datum key2,
					Datum key3,
					Datum key4)
{
	HeapTuple	tuple;
	Oid			result;

	tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failure in cache %d", cacheId);
	result = HeapTupleGetOid(tuple);
	ReleaseSysCache(tuple);
	return result;
}

#define GetSysCacheOidError2(cacheId, key1, key2) \
	GetSysCacheOidError(cacheId, key1, key2, 0, 0)

/*
 * Get the bdr.bdr_nodes status value for the specififed node from the local
 * bdr.bdr_nodes table via SPI.
 *
 * Returns the status value, or '\0' if no such row exists.
 *
 * SPI must be initialized, and you must be in a running transaction.
 */
char
bdr_nodes_get_local_status(uint64 sysid, TimeLineID tli, Oid dboid)
{
	int			spi_ret;
	Oid			argtypes[] = { TEXTOID, OIDOID, OIDOID };
	Datum		values[3];
	bool		isnull;
	char		status;
	char		sysid_str[33];
	Oid			schema_oid;

	Assert(IsTransactionState());

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, sysid);
	sysid_str[sizeof(sysid_str)-1] = '\0';

	/*
	 * Determine if BDR is present on this DB. The output plugin can
	 * be started on a db that doesn't actually have BDR active, but
	 * we don't want to allow that.
	 *
	 * Check for a bdr schema.
	 */
	schema_oid = GetSysCacheOid1(NAMESPACENAME, CStringGetDatum("bdr"));
	if (schema_oid == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("No bdr schema is present in database %s, cannot create a bdr slot",
					   get_database_name(MyDatabaseId)),
				errhint("There is no bdr.connections entry for this database on the target node or bdr is not in shared_preload_libraries")));

	values[0] = CStringGetTextDatum(sysid_str);
	values[1] = ObjectIdGetDatum(tli);
	values[2] = ObjectIdGetDatum(dboid);

	spi_ret = SPI_execute_with_args(
			"SELECT node_status FROM bdr.bdr_nodes "
			"WHERE node_sysid = $1 AND node_timeline = $2 AND node_dboid = $3",
			3, argtypes, values, NULL, false, 1);

	if (spi_ret != SPI_OK_SELECT)
		elog(ERROR, "Unable to query bdr.bdr_nodes, SPI error %d", spi_ret);

	if (SPI_processed == 0)
		return '\0';

	status = DatumGetChar(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
						  &isnull));

	if (isnull)
		elog(ERROR, "bdr.bdr_nodes.status NULL; shouldn't happen");

	return status;
}

/*
 * Get the bdr.bdr_nodes record for the specififed node from the local
 * bdr.bdr_nodes table via SPI.
 *
 * Returns the status value, or NULL if no such row exists.
 *
 * SPI must be initialized, and you must be in a running transaction.
 */
BDRNodeInfo *
bdr_nodes_get_local_info(uint64 sysid, TimeLineID tli, Oid dboid)
{
	BDRNodeInfo *node = NULL;
	char		sysid_str[33];
	HeapTuple	tuple = NULL;
	Relation	rel;
	RangeVar   *rv;
	SysScanDesc scan;
	ScanKeyData	key[3];

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, sysid);
	sysid_str[sizeof(sysid_str)-1] = '\0';

	rv = makeRangeVar("bdr", "bdr_nodes", -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(sysid_str));
	ScanKeyInit(&key[1],
				2,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(tli));
	ScanKeyInit(&key[2],
				3,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dboid));

	scan = systable_beginscan(rel, 0, true, NULL, 3, key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		TupleDesc	desc = RelationGetDescr(rel);
		Datum		tmp;

		node = palloc0(sizeof(BDRNodeInfo));
		node->id.sysid = sysid;
		node->id.timeline = tli;
		node->id.dboid = dboid;
		node->status = DatumGetChar(fastgetattr(tuple, 4, desc, &isnull));
		if (isnull)
			elog(ERROR, "bdr.bdr_nodes.status NULL; shouldn't happen");

		tmp = fastgetattr(tuple, 5, desc, &isnull);
		if (isnull)
			node->name = NULL;
		else
			node->name = pstrdup(TextDatumGetCString(tmp));

		tmp = fastgetattr(tuple, 6, desc, &isnull);
		if (!isnull)
			node->local_dsn = pstrdup(TextDatumGetCString(tmp));

		tmp = fastgetattr(tuple, 7, desc, &isnull);
		if (!isnull)
			node->init_from_dsn = pstrdup(TextDatumGetCString(tmp));

		node->read_only = DatumGetBool(fastgetattr(tuple, 8, desc, &isnull));
		/* Readonly will be null on upgrade from an older BDR */
		if (isnull)
			node->read_only = false;

		node->seq_id = DatumGetInt16(fastgetattr(tuple, 9, desc, &isnull));
		/* seq_id will be null if seq2 not in use or on upgrade */
		if (isnull)
			node->seq_id = -1;

		node->valid = true;
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return node;
}

/*
 * Quick lookup in bdr nodes to map a node name to an identity tuple. Returns
 * true if found, false if not.
 */
bool
bdr_get_node_identity_by_name(const char *node_name, uint64 *sysid, TimeLineID *timeline, Oid *dboid)
{
	HeapTuple	tuple = NULL;
	Relation	rel;
	RangeVar   *rv;
	SysScanDesc scan;
	ScanKeyData	key[1];
	bool		found = false;

	rv = makeRangeVar("bdr", "bdr_nodes", -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				5, /* node_name attno */
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(node_name));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		TupleDesc	desc = RelationGetDescr(rel);
		Datum		d;

		const char *sysid_str;

		d = fastgetattr(tuple, 1, desc, &isnull);
		if (isnull)
			elog(ERROR, "bdr.bdr_nodes.sysid is NULL; shouldn't happen");
		sysid_str = TextDatumGetCString(d);

		if (sscanf(sysid_str, UINT64_FORMAT, sysid) != 1)
			elog(ERROR, "bdr.bdr_nodes.sysid didn't parse to integer; shouldn't happen");

		*timeline = DatumGetObjectId(fastgetattr(tuple, 2, desc, &isnull));
		if (isnull)
			elog(ERROR, "bdr.bdr_nodes.timeline is NULL; shouldn't happen");
			
		*dboid = DatumGetObjectId(fastgetattr(tuple, 3, desc, &isnull));
		if (isnull)
			elog(ERROR, "bdr.bdr_nodes.dboid is NULL; shouldn't happen");

		found = true;
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return found;
}

/* Free the BDRNodeInfo pointer including its properties. */
void
bdr_bdr_node_free(BDRNodeInfo *node)
{
	if (node == NULL)
		return;

	if (node->local_dsn)
		pfree(node->local_dsn);
	if (node->init_from_dsn)
		pfree(node->init_from_dsn);
	pfree(node);
}

/*
 * Update the status field on the local node (as identified by current
 * sysid,tlid,dboid) of bdr.bdr_nodes. The node record must already exist.
 *
 * Unlike bdr_nodes_get_local_status, this inteface does not accept
 * sysid, tlid and dboid input but can only set the status of the local node.
 */
void
bdr_nodes_set_local_status(char status)
{
	int			spi_ret;
	Oid			argtypes[] = { CHAROID, TEXTOID, OIDOID, OIDOID };
	Datum		values[4];
	char		sysid_str[33];
	bool		tx_started = false;
	bool		spi_pushed;

	Assert(status != '\0'); /* Cannot pass \0 */
	/* Cannot have replication apply state set in this tx */
	Assert(replorigin_session_origin == InvalidRepOriginId);

	if (!IsTransactionState())
	{
		tx_started = true;
		StartTransactionCommand();
	}
	spi_pushed = SPI_push_conditional();
	SPI_connect();

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT,
			 GetSystemIdentifier());
	sysid_str[sizeof(sysid_str)-1] = '\0';

	values[0] = CharGetDatum(status);
	values[1] = CStringGetTextDatum(sysid_str);
	values[2] = ObjectIdGetDatum(ThisTimeLineID);
	values[3] = ObjectIdGetDatum(MyDatabaseId);

	spi_ret = SPI_execute_with_args(
							   "UPDATE bdr.bdr_nodes"
							   "   SET node_status = $1"
							   " WHERE node_sysid = $2"
							   "   AND node_timeline = $3"
							   "   AND node_dboid = $4;",
							   4, argtypes, values, NULL, false, 0);

	if (spi_ret != SPI_OK_UPDATE)
		elog(ERROR, "Unable to set status=%c of row (node_sysid="
					UINT64_FORMAT ", node_timeline=%u, node_dboid=%u) "
					"in bdr.bdr_nodes: SPI error %d",
					status, GetSystemIdentifier(), ThisTimeLineID,
					MyDatabaseId, spi_ret);

	SPI_finish();
	SPI_pop_conditional(spi_pushed);
	if (tx_started)
		CommitTransactionCommand();
}

/*
 * Given a node's local RepOriginId, get its globally unique identifier (sysid,
 * timeline id, database oid). Ignore identifiers local to databases other than
 * the active DB.
 */
void
bdr_fetch_sysid_via_node_id(RepOriginId node_id, uint64 *sysid, TimeLineID *tli,
							Oid *dboid)
{
	if (node_id == InvalidRepOriginId || node_id == DoNotReplicateId)
	{
		/* It's the local node */
		*sysid = GetSystemIdentifier();
		*tli = ThisTimeLineID;
		*dboid = MyDatabaseId;
	}
	else
	{
		char *riname;

		uint64 remote_sysid;
		Oid remote_dboid;
		TimeLineID remote_tli;
		Oid local_dboid;
		NameData replication_name;

		replorigin_by_oid(node_id, false, &riname);

		if (sscanf(riname, BDR_NODE_ID_FORMAT,
				   &remote_sysid, &remote_tli, &remote_dboid, &local_dboid,
				   NameStr(replication_name)) != 4)
			elog(ERROR, "could not parse sysid: %s", riname);
		pfree(riname);

		if (local_dboid != MyDatabaseId)
		{
			ereport(ERROR,
					(errmsg("lookup failed for replication identifier %u", node_id),
					 errmsg("Replication identifier %u exists but is owned by another BDR node in the same PostgreSQL instance, with dboid %u. Current node oid is %u.",
					 		node_id, local_dboid, MyDatabaseId)));
		}

		*sysid = remote_sysid;
		*tli = remote_tli;
		*dboid = remote_dboid;
	}
}

/*
 * Get node identifiers from a replication identifier (replident, riident) name
 * 
 * This isn't in bdr_common.c because it uses elog().
 */
void
bdr_parse_replident_name(const char *sname, uint64 *remote_sysid, TimeLineID *remote_tli,
					Oid *remote_dboid, Oid *local_dboid)
{
	NameData	replication_name;

	if (sscanf(sname, BDR_NODE_ID_FORMAT,
			   remote_sysid, remote_tli, remote_dboid, local_dboid,
			   NameStr(replication_name)) != 4)
	{
		elog(ERROR, "could not parse slot name: %s", sname);
	}
}

/*
 * Get node identifiers from a slot name
 *
 * This isn't in bdr_common.c because it uses elog().
 */
void
bdr_parse_slot_name(const char *sname, uint64 *remote_sysid, TimeLineID *remote_tli,
					Oid *remote_dboid, Oid *local_dboid)
{
	NameData	replication_name;

	if (sscanf(sname, BDR_SLOT_NAME_FORMAT,
			   local_dboid, remote_sysid, remote_tli, remote_dboid,
			   NameStr(replication_name)) != 4)
	{
		elog(ERROR, "could not parse slot name: %s", sname);
	}
}

/*
 * Format a replication origin / replication identifier (riident, replident)
 * name from a (sysid,timeline,dboid tuple).
 *
 * This isn't in bdr_common.c because it uses StringInfo.
 */
char*
bdr_replident_name(uint64 remote_sysid, TimeLineID remote_timeline, Oid remote_dboid, Oid local_dboid)
{
	StringInfoData si;

	initStringInfo(&si);

	appendStringInfo(&si, BDR_NODE_ID_FORMAT,
			 remote_sysid, remote_timeline, remote_dboid, local_dboid,
			 EMPTY_REPLICATION_NAME);

	/* stringinfo's data is palloc'd, can be returned directly */
	return si.data;
}

RepOriginId
bdr_fetch_node_id_via_sysid(uint64 sysid, TimeLineID tli, Oid dboid)
{
	char		ident[256];

	snprintf(ident, sizeof(ident),
			 BDR_NODE_ID_FORMAT,
			 sysid, tli, dboid, MyDatabaseId,
			 "");
	return replorigin_by_name(ident, false);
}

/*
 * Read connection configuration data from the DB and return zero or more
 * matching palloc'd BdrConnectionConfig results in a list.
 *
 * A transaction must be open.
 *
 * The list and values are allocated in the calling memory context. By default
 * this is the transaction memory context, but you can switch to contexts
 * before calling.
 *
 * Each BdrConnectionConfig's char* fields are palloc'd values.
 *
 * Uses the SPI, so push/pop caller's SPI state if needed.
 *
 * May raise exceptions from queries, SPI errors, etc.
 *
 * If both an entry with conn_origin for this node and one with null
 * conn_origin are found, only the one specific to this node is returned,
 * as it takes precedence over any generic configuration entry.
 *
 * Connections for nodes with state 'k'illed are not returned.
 * Connections in other states are, since we should fail (and retry)
 * until they're ready to accept slot creation. Connections with
 * no corresponding bdr.bdr_nodes row also get ignored.
 */
List*
bdr_read_connection_configs()
{
	HeapTuple tuple;
	StringInfoData query;
	int			i;
	int			ret;
	List	   *configs = NIL;
	MemoryContext caller_ctx, saved_ctx;
	char		sysid_str[33];
	Datum		values[3];
	Oid			types[3] = { TEXTOID, OIDOID, OIDOID };

	Assert(IsTransactionState());

	/* Save the calling memory context, which we'll allocate results in */
	caller_ctx = MemoryContextSwitchTo(CurTransactionContext);

	initStringInfo(&query);

	/*
	 * Find a connections row specific to this origin node or if none
	 * exists, the default connection data for that node.
	 *
	 * Configurations for all nodes, including the local node, are read.
	 */
	appendStringInfo(&query, "SELECT DISTINCT ON (conn_sysid, conn_timeline, conn_dboid) "
							 "  conn_sysid, conn_timeline, conn_dboid, "
							 "  conn_dsn, conn_apply_delay, "
							 "  conn_replication_sets, "
							 "  conn_origin_dboid <> 0 AS origin_is_my_id "
							 "FROM bdr.bdr_connections "
							 "INNER JOIN bdr.bdr_nodes "
							 "  ON (conn_sysid = node_sysid AND "
							 "      conn_timeline = node_timeline AND "
							 "      conn_dboid = node_dboid) "
							 "WHERE (conn_origin_sysid = '0' "
							 "  AND  conn_origin_timeline = 0 "
							 "  AND  conn_origin_dboid = 0) "
							 "   OR (conn_origin_sysid = $1 "
							 "  AND  conn_origin_timeline = $2 "
							 "  AND  conn_origin_dboid = $3) "
							 "  AND node_status <> 'k' "
							 "  AND NOT conn_is_unidirectional "
							 "ORDER BY conn_sysid, conn_timeline, conn_dboid, "
							 "         conn_origin_sysid ASC NULLS LAST, "
							 "         conn_timeline ASC NULLS LAST, "
							 "         conn_dboid ASC NULLS LAST "
					 );

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, GetSystemIdentifier());
	sysid_str[sizeof(sysid_str)-1] = '\0';

	values[0] = CStringGetTextDatum(&sysid_str[0]);
	values[1] = ObjectIdGetDatum(ThisTimeLineID);
	values[2] = ObjectIdGetDatum(MyDatabaseId);

	SPI_connect();

	ret = SPI_execute_with_args(query.data, 3, types, values, NULL, false, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI error while querying bdr.bdr_connections");

	/* Switch to calling memory context to copy results */
	saved_ctx = MemoryContextSwitchTo(caller_ctx);

	for (i = 0; i < SPI_processed; i++)
	{
		Datum			tmp_datum;
		bool			isnull;
		ArrayType	   *conn_replication_sets;
		char		   *tmp_sysid;

		BdrConnectionConfig *cfg = palloc(sizeof(BdrConnectionConfig));

		tuple = SPI_tuptable->vals[i];

		/*
		 * Fetch tuple attributes
		 *
		 * Note: SPI_getvalue calls the output function for the type, so the
		 * string is allocated in our memory context and doesn't need copying.
		 */
		tmp_sysid = SPI_getvalue(tuple, SPI_tuptable->tupdesc,
								 getattno("conn_sysid"));

		if (sscanf(tmp_sysid, UINT64_FORMAT, &cfg->sysid) != 1)
			elog(ERROR, "Parsing sysid uint64 from %s failed", tmp_sysid);

		tmp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								  getattno("conn_timeline"),
								  &isnull);
		Assert(!isnull);
		cfg->timeline = DatumGetObjectId(tmp_datum);

		tmp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								  getattno("conn_dboid"),
								  &isnull);
		Assert(!isnull);
		cfg->dboid = DatumGetObjectId(tmp_datum);

		tmp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								  getattno("origin_is_my_id"),
								  &isnull);
		Assert(!isnull);
		cfg->origin_is_my_id = DatumGetBool(tmp_datum);


		cfg->dsn = SPI_getvalue(tuple,
											 SPI_tuptable->tupdesc,
											 getattno("conn_dsn"));

		tmp_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
								  getattno("conn_apply_delay"), &isnull);
		if (isnull)
			cfg->apply_delay = -1;
		else
			cfg->apply_delay = DatumGetInt32(tmp_datum);

		/*
		 * Replication sets are stored in the catalogs as a text[]
		 * of identifiers, so we'll want to unpack that.
		 */

		conn_replication_sets = (ArrayType*)
			SPI_getbinval(tuple, SPI_tuptable->tupdesc,
						  getattno("conn_replication_sets"), &isnull);

		if (isnull)
			cfg->replication_sets = NULL;
		else
		{
			cfg->replication_sets =
				bdr_textarr_to_identliststr(DatumGetArrayTypeP(conn_replication_sets));
		}

		configs = lcons(cfg, configs);

	}

	MemoryContextSwitchTo(saved_ctx);

	SPI_finish();

	MemoryContextSwitchTo(caller_ctx);

	return configs;
}

void
bdr_free_connection_config(BdrConnectionConfig *cfg)
{
	if (cfg->dsn != NULL)
		pfree(cfg->dsn);
	if (cfg->replication_sets != NULL)
		pfree(cfg->replication_sets);
}

/*
 * Fetch the connection configuration for the local node, i.e. the entry
 * with our (conn_sysid, conn_tlid, conn_dboid).
 */
BdrConnectionConfig*
bdr_get_connection_config(uint64 sysid, TimeLineID timeline, Oid dboid,
						  bool missing_ok)
{
	List *configs;
	ListCell *lc;
	MemoryContext saved_ctx;
	BdrConnectionConfig *found_config = NULL;
	bool tx_started = false;

	Assert(MyDatabaseId != InvalidOid);

	if (!IsTransactionState())
	{
		tx_started = true;
		StartTransactionCommand();
	}

	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
	configs = bdr_read_connection_configs();
	MemoryContextSwitchTo(saved_ctx);

	/*
	 * TODO DYNCONF Instead of reading all configs and then discarding all but
	 * the interesting one, we should really be doing a different query that
	 * returns only the configuration of interest. As this runs only during apply
	 * worker startup the impact is negligible.
	 */
	foreach(lc, configs)
	{
		BdrConnectionConfig *cfg = (BdrConnectionConfig*) lfirst(lc);

		if (cfg->sysid == sysid
			&& cfg->timeline == timeline
			&& cfg->dboid == dboid)
		{
			found_config = cfg;
			break;
		}
		else
		{
			bdr_free_connection_config(cfg);
		}
	}

	if (found_config == NULL && !missing_ok)
		elog(ERROR, "Failed to find expected bdr.connections row "
					"(conn_sysid,conn_timeline,conn_dboid) = "
					"("UINT64_FORMAT",%u,%u) "
					"in bdr.bdr_connections",
					sysid, timeline, dboid);

	if (tx_started)
		CommitTransactionCommand();

	list_free(configs);

	return found_config;
}


static int
getattno(const char *colname)
{
	int attno;

	attno = SPI_fnumber(SPI_tuptable->tupdesc, colname);
	if (attno == SPI_ERROR_NOATTRIBUTE)
		elog(ERROR, "SPI error while reading %s from bdr.bdr_connections", colname);

	return attno;
}

/*
 * Given a text[] Datum guaranteed to contain no nulls, return an
 * identifier-quoted comma-separated string allocated in the current memory
 * context.
 */
static char*
bdr_textarr_to_identliststr(ArrayType *textarray)
{
	Datum		   *elems;
	int				nelems, i;
	StringInfoData	si;

	deconstruct_array(textarray,
					  TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	if (nelems == 0)
		return pstrdup("");

	initStringInfo(&si);

	appendStringInfoString(&si,
		quote_identifier(TextDatumGetCString(elems[0])));
	for (i = 1; i < nelems; i++)
	{
		appendStringInfoString(&si, ",");
		appendStringInfoString(&si,
			quote_identifier(TextDatumGetCString(elems[i])));
	}

	/*
	 * The stringinfo is on the stack, but its data element is palloc'd
	 * in the caller's context and can be returned safely.
	 */
	return si.data;

}

/*
 * Helper to format node identity info into buffers, which must already be
 * allocated and big enough to hold a unit64 + terminator (33 bytes).
 */
void
stringify_node_identity(char *sysid_str, Size sysid_str_size,
						char *timeline_str, Size timeline_str_size,
						char *dboid_str, Size dboid_str_size,
						uint64 sysid, TimeLineID timeline, Oid dboid)
{
	snprintf(sysid_str, sysid_str_size, UINT64_FORMAT, sysid);
	sysid_str[sysid_str_size-1] = '\0';

	snprintf(timeline_str, timeline_str_size, "%u", timeline);
	timeline_str[timeline_str_size-1] = '\0';

	snprintf(dboid_str, dboid_str_size, "%u", dboid);
	dboid_str[dboid_str_size-1] = '\0';
}

void
stringify_my_node_identity(char *sysid_str, Size sysid_str_size,
						char *timeline_str, Size timeline_str_size,
						char *dboid_str, Size dboid_str_size)
{
	return stringify_node_identity(sysid_str, sysid_str_size, timeline_str,
			timeline_str_size, dboid_str, dboid_str_size,
			GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId);
}
