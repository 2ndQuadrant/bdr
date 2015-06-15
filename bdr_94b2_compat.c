/* -------------------------------------------------------------------------
 *
 * bdr_94b2_compat.c
 *		Wrapper/compatiability functions to compile 0.9.1 with legacy
 *		9.4b2
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_94b2_compat.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"
#include "utils/syscache.h"
#include "replication/replication_identifier.h"

#include "bdr_94b2_compat.h"

void
GetReplicationInfoByIdentifierWrapper(RepNodeId riident, bool missing_ok, char **riname)
{
	HeapTuple tuple;
	Form_pg_replication_identifier ric;

	tuple = GetReplicationInfoByIdentifier(riident, missing_ok);

	if (HeapTupleIsValid(tuple))
	{
		ric = (Form_pg_replication_identifier) GETSTRUCT(tuple);
		*riname = pstrdup(text_to_cstring(&ric->riname));
	}

	ReleaseSysCache(tuple);
}
