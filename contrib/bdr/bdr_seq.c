/* -------------------------------------------------------------------------
 *
 * bdr.c
 *		Replication!!!
 *
 * Replication???
 *
 * Copyright (C) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"

/* sequencer */
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

	/* vote */
const char* vote_sql =
"INSERT INTO bdr_votes (\n"
"    vote_sysid,\n"
"    vote_tlid,\n"
"    vote_dboid,\n"
"    vote_riname,\n"
"    vote_election_id,\n"
"\n"
"    voter_sysid,\n"
"    voter_tlid,\n"
"    voter_dboid,\n"
"    voter_riname,\n"
"\n"
"    vote\n"
")\n"
"SELECT\n"
"    owning_sysid, owning_tlid, owning_dboid, owning_riname, owning_election_id,\n"
"    $1, $2, $3, $4,\n"
"    EXISTS(\n"
"        SELECT *\n"
"        FROM bdr_sequence_values val\n"
"        WHERE true\n"
"            AND val.seqschema = election.seqschema\n"
"            AND val.seqname = election.seqname\n"
"            AND NOT val.confirmed\n"
"            AND val.seqrange && election.seqrange\n"
"    )\n"
"FROM bdr_sequence_elections election\n"
"WHERE\n"
"    election.open\n"
"    AND NOT EXISTS (\n"
"        SELECT *\n"
"        FROM bdr_votes\n"
"        WHERE true\n"
"            AND owning_sysid = vote_sysid\n"
"            AND owning_tlid = vote_tlid\n"
"            AND owning_dboid = vote_dboid\n"
"            AND owning_riname = vote_riname\n"
"            AND owning_election_id = vote_election_id\n"
"\n"
"    	    AND voter_sysid = $1\n"
"    	    AND voter_tlid = $2\n"
"    	    AND voter_dboid = $3\n"
"    	    AND voter_riname = $4\n"
"    )\n"
";";

void
bdr_sequencer_vote(void)
{
	Oid			argtypes[4];
	Datum		values[4];
	bool		nulls[4];
	char		local_sysid[32];
	int			ret;

	snprintf(local_sysid, sizeof(local_sysid), UINT64_FORMAT,
			 GetSystemIdentifier());

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	argtypes[0] = TEXTOID;
	nulls[0] = false;
	values[0] = CStringGetTextDatum(local_sysid);

	argtypes[1] = OIDOID;
	nulls[1] = false;
	values[1] = ObjectIdGetDatum(ThisTimeLineID);

	argtypes[2] = OIDOID;
	values[2] = ObjectIdGetDatum(MyDatabaseId);
	nulls[2] = false;

	argtypes[3] = TEXTOID;
	values[3] = CStringGetTextDatum("");
	nulls[3] = false;

	ret = SPI_execute_with_args(vote_sql, 4, argtypes,
								values, nulls, false, 0);

	if (ret != SPI_OK_INSERT)
		elog(ERROR, "blub");

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

void
bdr_sequencer_tally(void)
{
}


void
bdr_sequencer_start_elections(void)
{
}
