/* -------------------------------------------------------------------------
 *
 * bdr_catalogs.c
 *		Access to bdr catalog information like bdr.bdr_nodes
 *
 * Functions usable by both the output plugin and the extension/workers for
 * accessing and manipulating BDR's catalogs, like bdr.bdr_nodes.
 *
 * Copyright (C) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr.c
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

#include "bdr_replication_identifier.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/syscache.h"

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
	char        status;
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
 * Insert a row for the local node's (sysid,tlid,dboid) with the passed status
 * into bdr.bdr_nodes. No existing row for this key may exist.
 *
 * Unlike bdr_set_remote_status, '\0' may not be passed to delete the row, and
 * no upsert is performed. This is a simple insert only.
 *
 * Unlike bdr_nodes_get_local_status, only the status of the local node may
 * be set.
 *
 * SPI must be initialized, and you must be in a running transaction that is
 * not bound to any remote node replication state.
 */
void
bdr_nodes_set_local_status(char status)
{
	int			spi_ret;
	Oid			argtypes[] = { CHAROID, TEXTOID, OIDOID, OIDOID };
	Datum		values[4];
	char		sysid_str[33];

	Assert(status != '\0'); /* Cannot pass \0 to delete */
	Assert(IsTransactionState());
	/* Cannot have replication apply state set in this tx */
	Assert(replication_origin_id == InvalidRepNodeId);

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT,
			 GetSystemIdentifier());
	sysid_str[sizeof(sysid_str)-1] = '\0';

	values[0] = CharGetDatum(status);
	values[1] = CStringGetTextDatum(sysid_str);
	values[2] = ObjectIdGetDatum(ThisTimeLineID);
	values[3] = ObjectIdGetDatum(MyDatabaseId);

	spi_ret = SPI_execute_with_args(
							   "INSERT INTO bdr.bdr_nodes"
							   " (node_status, node_sysid, node_timeline, node_dboid)"
							   " VALUES ($1, $2, $3, $4);",
							   4, argtypes, values, NULL, false, 0);

	if (spi_ret != SPI_OK_INSERT)
		elog(ERROR, "Unable to insert row (status=%c, node_sysid="
					UINT64_FORMAT ", node_timeline=%u, node_dboid=%u) "
					"into bdr.bdr_nodes: SPI error %d",
					status, GetSystemIdentifier(), ThisTimeLineID,
					MyDatabaseId, spi_ret);
}

/*
 * Given a node's local RepNodeId, get its globally unique identifier
 * (sysid, timeline id, database oid)
 */
void
bdr_fetch_sysid_via_node_id(RepNodeId node_id, uint64 *sysid, TimeLineID *tli,
							Oid *dboid)
{
	if (node_id == InvalidRepNodeId)
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

		GetReplicationInfoByIdentifier(node_id, false, &riname);

		if (sscanf(riname, BDR_NODE_ID_FORMAT,
				   &remote_sysid, &remote_tli, &remote_dboid, &local_dboid,
				   NameStr(replication_name)) != 4)
			elog(ERROR, "could not parse sysid: %s", riname);
		pfree(riname);

		*sysid = remote_sysid;
		*tli = remote_tli;
		*dboid = remote_dboid;
	}
}


RepNodeId
bdr_fetch_node_id_via_sysid(uint64 sysid, TimeLineID tli, Oid dboid)
{
	char		ident[256];

	snprintf(ident, sizeof(ident),
			 BDR_NODE_ID_FORMAT,
			 sysid, tli, dboid, MyDatabaseId,
			 "");
	return GetReplicationIdentifier(ident, false);
}
