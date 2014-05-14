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

#include "access/xact.h"

#include "catalog/pg_type.h"

#include "executor/spi.h"

#include "utils/builtins.h"

/*
 * Get the bdr.bdr_nodes status value for the current local node from the local
 * database via SPI, if any such row exists.
 *
 * Returns the status value, or '\0' if no such row exists.
 *
 * SPI must be initialized, and you must be in a running transaction.
 */
char
bdr_nodes_get_local_status(uint64 sysid, Name dbname)
{
	int			spi_ret;
	Oid			argtypes[] = { NUMERICOID, NAMEOID };
	Datum		values[2];
	bool		isnull;
	char        status;
	char		sysid_str[33];

	Assert(IsTransactionState());

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, sysid);
	sysid_str[sizeof(sysid_str)-1] = '\0';

	values[0] = DirectFunctionCall3Coll(numeric_in, InvalidOid,
										CStringGetDatum(sysid_str),
										InvalidOid, Int32GetDatum(-1));
	values[1] = NameGetDatum(dbname);

	spi_ret = SPI_execute_with_args(
			"SELECT node_status FROM bdr.bdr_nodes "
			"WHERE node_sysid = $1 AND node_dbname = $2",
			2, argtypes, values, NULL, false, 1);

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
 * Insert a row for the local node's (sysid,dbname) with the passed status into
 * bdr.bdr_nodes. No existing row for this key may exist.
 *
 * Unlike bdr_set_remote_status, '\0' may not be passed to delete the row, and
 * no upsert is performed. This is a simple insert only.
 *
 * SPI must be initialized, and you must be in a running transaction that is
 * not bound to any remote node replication state.
 */
void
bdr_nodes_set_local_status(uint64 sysid, Name dbname, char status)
{
	int			spi_ret;
	Oid			argtypes[] = { CHAROID, NUMERICOID, NAMEOID };
	Datum		values[3];
	char		sysid_str[33];

	Assert(status != '\0'); /* Cannot pass \0 to delete */
	Assert(IsTransactionState());
	/* Cannot have replication apply state set in this tx */
	Assert(replication_origin_id == InvalidRepNodeId);

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, sysid);
	sysid_str[sizeof(sysid_str)-1] = '\0';

	values[0] = CharGetDatum(status);
	values[1] = DirectFunctionCall3Coll(numeric_in, InvalidOid,
										CStringGetDatum(sysid_str),
										InvalidOid, Int32GetDatum(-1));
	values[2] = NameGetDatum(dbname);

	spi_ret = SPI_execute_with_args(
							   "INSERT INTO bdr.bdr_nodes"
							   "    (node_status, node_sysid, node_dbname)"
							   "    VALUES ($1, $2, $3);",
							   3, argtypes, values, NULL, false, 0);

	if (spi_ret != SPI_OK_INSERT)
		elog(ERROR, "Unable to insert row (status=%c, node_sysid="
					UINT64_FORMAT ", node_dbname=%s) into bdr.bdr_nodes, "
					"SPI error %d",
					status, sysid, NameStr(*dbname), spi_ret);
}
