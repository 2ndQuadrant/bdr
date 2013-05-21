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
#include "pgstat.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/proc.h"

typedef struct BdrSequencerSlot
{
	Oid database_oid;
	Latch *proclatch;
} BdrSequencerSlot;

typedef struct BdrSequencerControl
{
	size_t nnodes;
	size_t slot;
	BdrSequencerSlot slots[FLEXIBLE_ARRAY_MEMBER];
} BdrSequencerControl;

static BdrSequencerControl *BdrSequencerCtl = NULL;

/* how many nodes have we built shmem for */
static size_t bdr_seq_nnodes = 0;

/* how many nodes have we built shmem for */
static size_t bdr_seq_nsequencers = 0;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

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
"    -- haven't allocated the value ourselves\n"
"    NOT EXISTS(\n"
"        SELECT *\n"
"        FROM bdr_sequence_values val\n"
"        WHERE true\n"
"            AND val.seqschema = election.seqschema\n"
"            AND val.seqname = election.seqname\n"
"            AND val.seqrange && election.seqrange\n"
"            --AND NOT val.confirmed\n"
"        	 AND val.owning_sysid = $1\n"
"        	 AND val.owning_tlid = $2\n"
"        	 AND val.owning_dboid = $3\n"
"        	 AND val.owning_riname = $4\n"
"    )\n"
"    -- and we haven't allowe anybody else to use it\n"
"    AND NOT EXISTS(\n"
"        SELECT *\n"
"        FROM bdr_votes vote\n"
"            JOIN bdr_sequence_elections other_election ON (\n"
"                other_election.owning_sysid = vote.vote_sysid\n"
"                AND other_election.owning_tlid = vote.vote_tlid\n"
"                AND other_election.owning_dboid = vote.vote_dboid\n"
"                AND other_election.owning_riname = vote.vote_riname\n"
"                AND other_election.owning_election_id = vote.vote_election_id\n"
"            )\n"
"        WHERE true\n"
"        	 AND vote.voter_sysid = $1\n"
"        	 AND vote.voter_tlid = $2\n"
"        	 AND vote.voter_dboid = $3\n"
"        	 AND vote.voter_riname = $4\n"
"            AND other_election.seqrange && election.seqrange \n"
"    )\n"
"\n"
"FROM bdr_sequence_elections election\n"
"WHERE\n"
"    election.open\n"
"    -- not our election\n"
"    AND NOT (\n"
"        owning_sysid = $1\n"
"        AND owning_tlid = $2\n"
"        AND owning_dboid = $3\n"
"        AND owning_riname = $4\n"
"    )\n"
"    -- we haven't voted about this yet\n"
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
"    	     AND voter_sysid = $1\n"
"    	     AND voter_tlid = $2\n"
"    	     AND voter_dboid = $3\n"
"    	     AND voter_riname = $4\n"
"    )\n"
"LIMIT 1\n"
";";

const char *start_elections_sql =
"WITH to_be_updated_sequences AS (\n"
"    SELECT\n"
"        pg_namespace.nspname AS seqschema,\n"
"        pg_class.relname AS seqname,\n"
"        COUNT(bdr_sequence_values) AS open_seq_chunks,\n"
"        COALESCE((\n"
"            SELECT max(upper(seqrange))\n"
"            FROM bdr_sequence_values max_val\n"
"            WHERE\n"
"                max_val.seqschema = pg_namespace.nspname\n"
"                AND max_val.seqname = pg_class.relname\n"
"        ), 0) AS current_max\n"
"    FROM\n"
"        pg_class\n"
"        JOIN pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)\n"
"        LEFT JOIN bdr_sequence_values ON (\n"
"            bdr_sequence_values.seqschema = pg_namespace.nspname\n"
"            AND bdr_sequence_values.seqname = pg_class.relname\n"
"            AND bdr_sequence_values.emptied = false\n"
"            AND bdr_sequence_values.failed = false\n"
"            AND bdr_sequence_values.owning_sysid = $1\n"
"            AND bdr_sequence_values.owning_tlid = $2\n"
"            AND bdr_sequence_values.owning_dboid = $3\n"
"            AND bdr_sequence_values.owning_riname = $4\n"
"        )\n"
"    WHERE\n"
"        pg_class.relkind = 'S'\n"
"        AND pg_class.relam = (SELECT oid FROM pg_seqam WHERE seqamname = 'bdr')\n"
"    GROUP BY\n"
"        pg_class.relname,\n"
"        pg_namespace.nspname\n"
"    HAVING\n"
"        count(bdr_sequence_values) <= 5\n"
"),\n"
"to_be_inserted_chunks AS (\n"
"    SELECT\n"
"        seqschema,\n"
"        seqname,\n"
"        current_max,\n"
"        generate_series(\n"
"            current_max,\n"
"-- 1000 is the chunk size, -1 is to get < instead of <= of generate_series\n"
"            current_max + 1000 * (5 - open_seq_chunks) - 1,\n"
"            1000) chunk_start\n"
"    FROM to_be_updated_sequences\n"
"),\n"
"inserted_chunks AS (\n"
"    INSERT INTO bdr_sequence_values(\n"
"        owning_sysid,\n"
"        owning_tlid,\n"
"        owning_dboid,\n"
"        owning_riname,\n"
"        seqschema,\n"
"        seqname,\n"
"        confirmed,\n"
"        emptied,\n"
"        seqrange\n"
"    )\n"
"    SELECT\n"
"        $1,\n"
"        $2,\n"
"        $3,\n"
"        $4,\n"
"        seqschema,\n"
"        seqname,\n"
"        false AS confirmed,\n"
"        false AS emptied,\n"
"        int8range(chunk_start, chunk_start + 1000)\n"
"    FROM to_be_inserted_chunks\n"
"    RETURNING\n"
"        owning_sysid,\n"
"        owning_tlid,\n"
"        owning_dboid,\n"
"        owning_riname,\n"
"        seqschema,\n"
"        seqname,\n"
"        confirmed,\n"
"        emptied,\n"
"        seqrange\n"
")\n"
"\n"
"INSERT INTO bdr_sequence_elections(\n"
"    owning_sysid,\n"
"    owning_tlid,\n"
"    owning_dboid,\n"
"    owning_riname,\n"
"    owning_election_id,\n"
"    vote_type,\n"
"    open,\n"
"    seqschema,\n"
"    seqname,\n"
"    seqrange\n"
")\n"
"SELECT\n"
"    owning_sysid,\n"
"    owning_tlid,\n"
"    owning_dboid,\n"
"    owning_riname,\n"
"    (\n"
"        SELECT COALESCE(max(owning_election_id), 0)\n"
"        FROM bdr_sequence_elections biggest\n"
"        WHERE\n"
"    	    biggest.owning_sysid = inserted_chunks.owning_sysid\n"
"    	    AND biggest.owning_tlid = inserted_chunks.owning_tlid\n"
"    	    AND biggest.owning_dboid = inserted_chunks.owning_dboid\n"
"    	    AND biggest.owning_riname = inserted_chunks.owning_riname\n"
"     ) + row_number() OVER (),\n"
"    'sequence',\n"
"    true AS open,\n"
"    seqschema,\n"
"    seqname,\n"
"    seqrange\n"
"FROM inserted_chunks\n"
"RETURNING\n"
"    seqschema,\n"
"    seqname,\n"
"    seqrange\n"
;

const char *tally_elections_sql =
"WITH tallied_votes AS (\n"
"SELECT\n"
"    election.owning_sysid,\n"
"    election.owning_tlid,\n"
"    election.owning_dboid,\n"
"    election.owning_riname,\n"
"    election.owning_election_id,\n"
"    election.seqschema,\n"
"    election.seqname,\n"
"    election.seqrange,\n"
"    SUM((vote.vote = true)::int) AS yays,\n"
"    SUM((vote.vote = false)::int) AS nays,\n"
"    COUNT(vote.vote) AS nr_votes,\n"
"    /* majority of others */\n"
"    COUNT(vote.vote) >= ceil(($5 - 1)/ 2.0) AS sufficient\n"
"--    COUNT(vote.vote) >= ($5 - 1) AS sufficient\n"
"FROM\n"
"    bdr_sequence_elections election\n"
"    LEFT JOIN bdr_votes vote ON (\n"
"            election.owning_sysid = vote.vote_sysid\n"
"            AND election.owning_tlid = vote.vote_tlid\n"
"            AND election.owning_dboid = vote.vote_dboid\n"
"            AND election.owning_riname = vote.vote_riname\n"
"            AND election.owning_election_id = vote.vote_election_id\n"
"    )\n"
"WHERE\n"
"    election.open\n"
"    AND election.owning_sysid = $1\n"
"    AND election.owning_tlid = $2\n"
"    AND election.owning_dboid = $3\n"
"    AND election.owning_riname = $4\n"
"GROUP BY\n"
"    election.owning_sysid,\n"
"    election.owning_tlid,\n"
"    election.owning_dboid,\n"
"    election.owning_riname,\n"
"    election.owning_election_id,\n"
"    election.seqschema,\n"
"    election.seqname,\n"
"    election.seqrange\n"
"),\n"
"cast_votes AS (\n"
"    UPDATE bdr_sequence_elections\n"
"    SET\n"
"        open = false,\n"
"        success = (nays = 0)\n"
"    FROM tallied_votes\n"
"    WHERE\n"
"    	bdr_sequence_elections.owning_sysid = tallied_votes.owning_sysid\n"
"    	AND bdr_sequence_elections.owning_tlid = tallied_votes.owning_tlid\n"
"    	AND bdr_sequence_elections.owning_dboid = tallied_votes.owning_dboid\n"
"    	AND bdr_sequence_elections.owning_riname = tallied_votes.owning_riname\n"
"    	AND bdr_sequence_elections.owning_election_id = tallied_votes.owning_election_id\n"
"       AND tallied_votes.sufficient\n"
"    RETURNING bdr_sequence_elections.*\n"
"),\n"
"successfull_sequence_values AS (\n"
"    UPDATE bdr_sequence_values\n"
"    SET\n"
"        confirmed = true\n"
"    FROM cast_votes\n"
"    WHERE\n"
"        cast_votes.success = true\n"
"        AND bdr_sequence_values.seqschema = cast_votes.seqschema\n"
"        AND bdr_sequence_values.seqname = cast_votes.seqname\n"
"        AND bdr_sequence_values.seqrange = cast_votes.seqrange\n"
"    	 AND bdr_sequence_values.owning_sysid = $1\n"
"    	 AND bdr_sequence_values.owning_tlid = $2\n"
"    	 AND bdr_sequence_values.owning_dboid = $3\n"
"    	 AND bdr_sequence_values.owning_riname = $4\n"
"    RETURNING bdr_sequence_values.*\n"
"),\n"
"failed_sequence_values AS (\n"
"    UPDATE bdr_sequence_values\n"
"    SET\n"
"        failed = true\n"
"    FROM cast_votes\n"
"    WHERE\n"
"        cast_votes.success = false\n"
"        AND bdr_sequence_values.seqschema = cast_votes.seqschema\n"
"        AND bdr_sequence_values.seqname = cast_votes.seqname\n"
"        AND bdr_sequence_values.seqrange = cast_votes.seqrange\n"
"    	 AND bdr_sequence_values.owning_sysid = $1\n"
"    	 AND bdr_sequence_values.owning_tlid = $2\n"
"    	 AND bdr_sequence_values.owning_dboid = $3\n"
"    	 AND bdr_sequence_values.owning_riname = $4\n"
"    RETURNING bdr_sequence_values.*\n"
")\n"
"\n"
"SELECT\n"
"    seqschema,\n"
"    seqname,\n"
"    seqrange,\n"
"    'success'::text\n"
"FROM successfull_sequence_values\n"
"\n"
"UNION ALL\n"
"\n"
"SELECT\n"
"    seqschema,\n"
"    seqname,\n"
"    seqrange,\n"
"    'failed'::text\n"
"FROM failed_sequence_values\n"
"\n"
"UNION ALL\n"
"\n"
"SELECT\n"
"    seqschema,\n"
"    seqname,\n"
"    seqrange,\n"
"    'pending'::text\n"
"FROM tallied_votes\n"
"WHERE NOT sufficient\n"
;

static Size
bdr_sequencer_shmem_size(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(BdrSequencerControl));
	size = add_size(size, mul_size(bdr_seq_nsequencers, sizeof(BdrSequencerSlot)));

	return size;
}

static void
bdr_sequencer_shmem_shutdown(int code, Datum arg)
{
	BdrSequencerSlot *slot;
	if (bdr_sequencer_con == NULL)
		return;

	slot = &BdrSequencerCtl->slots[bdr_sequencer_con->slot];

	slot->database_oid = InvalidOid;
	slot->proclatch = NULL;
}

static void
bdr_sequencer_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	BdrSequencerCtl = ShmemInitStruct("bdr_sequencer",
									  bdr_sequencer_shmem_size(),
									  &found);
	if (!found)
	{
		/* initialize */
		memset(BdrSequencerCtl, 0, bdr_sequencer_shmem_size());
	}
	LWLockRelease(AddinShmemInitLock);

	on_shmem_exit(bdr_sequencer_shmem_shutdown, (Datum) 0);
}

void
bdr_sequencer_shmem_init(int nnodes, int sequencers)
{
	Assert(process_shared_preload_libraries_in_progress);

	bdr_seq_nnodes = nnodes;
	bdr_seq_nsequencers = sequencers;

	RequestAddinShmemSpace(bdr_sequencer_shmem_size());

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = bdr_sequencer_shmem_startup;
}

void
bdr_sequencer_wakeup(void)
{
	int off;
	BdrSequencerSlot *slot;


	for (off = 0; off < bdr_seq_nnodes; off++)
	{
		slot = &BdrSequencerCtl->slots[off];

		/* FIXME: locking! */
		if (slot->database_oid == InvalidOid)
			continue;

		if (slot->database_oid != MyDatabaseId)
			continue;

		SetLatch(slot->proclatch);
	}
}

void
bdr_sequencer_init(void)
{
	CreateExtensionStmt create_stmt;
	AlterExtensionStmt alter_stmt;
	BdrSequencerSlot *slot;

	Assert(bdr_sequencer_con != NULL);

	slot = &BdrSequencerCtl->slots[bdr_sequencer_con->slot];
	slot->database_oid = MyDatabaseId;
	slot->proclatch = &MyProc->procLatch;

	create_stmt.if_not_exists = true;
	create_stmt.options = NIL;

	alter_stmt.options = NIL;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	create_stmt.extname = (char *)"btree_gist";
	alter_stmt.extname = (char *)"btree_gist";

	/* create extension if not exists */
	CreateExtension(&create_stmt);
	/* update extension otherwise */
	ExecAlterExtensionStmt(&alter_stmt);

	create_stmt.extname = (char *)"bdr";
	alter_stmt.extname = (char *)"bdr";
	/* create extension if not exists */
	CreateExtension(&create_stmt);
	/* update extension otherwise */
	ExecAlterExtensionStmt(&alter_stmt);

	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void
bdr_sequencer_lock_rel(char *relname)
{
	Oid nspoid;
	Oid relid;

	/* FIXME: fix hardcoded schema */
	nspoid = get_namespace_oid("public", false);
	relid = get_relname_relid(relname, nspoid);
	if (!relid)
		elog(ERROR, "cache lookup failed for relation public.%s", relname);

	SetCurrentStatementStartTimestamp();
	pgstat_report_activity(STATE_RUNNING, relname);
	LockRelationOid(relid, ExclusiveLock);
}

static void
bdr_sequencer_lock(void)
{

	bdr_sequencer_lock_rel("bdr_sequence_elections");
	bdr_sequencer_lock_rel("bdr_sequence_values");
	bdr_sequencer_lock_rel("bdr_votes");
}

void
bdr_sequencer_vote(void)
{
	Oid			argtypes[4];
	Datum		values[4];
	bool		nulls[4];
	char		local_sysid[32];
	int			ret;
	int			my_processed;

	snprintf(local_sysid, sizeof(local_sysid), UINT64_FORMAT,
			 GetSystemIdentifier());
again:
	StartTransactionCommand();
	SPI_connect();

	bdr_sequencer_lock();
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

	SetCurrentStatementStartTimestamp();
	pgstat_report_activity(STATE_RUNNING, vote_sql);
	ret = SPI_execute_with_args(vote_sql, 4, argtypes,
								values, nulls, false, 0);

	if (ret != SPI_OK_INSERT)
		elog(ERROR, "blub");
	my_processed = SPI_processed;
	elog(LOG, "started %d votes", my_processed);

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();

	if (my_processed > 0)
		goto again;
}

void
bdr_sequencer_start_elections(void)
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

	bdr_sequencer_lock();
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

	SetCurrentStatementStartTimestamp();
	pgstat_report_activity(STATE_RUNNING, start_elections_sql);
	ret = SPI_execute_with_args(start_elections_sql, 4, argtypes,
								values, nulls, false, 0);

	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "blub");

	elog(LOG, "started %d elections", SPI_processed);

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

void
bdr_sequencer_tally(void)
{
	Oid			argtypes[5];
	Datum		values[5];
	bool		nulls[5];
	char		local_sysid[32];
	int			ret;

	snprintf(local_sysid, sizeof(local_sysid), UINT64_FORMAT,
			 GetSystemIdentifier());

	StartTransactionCommand();
	SPI_connect();

	bdr_sequencer_lock();
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

	argtypes[4] = INT4OID;
	values[4] = Int32GetDatum(bdr_sequencer_con->num_nodes);
	nulls[4] = false;

	SetCurrentStatementStartTimestamp();
	pgstat_report_activity(STATE_RUNNING, tally_elections_sql);
	ret = SPI_execute_with_args(tally_elections_sql, 5, argtypes,
								values, nulls, false, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "blub");

	elog(LOG, "tallied %d elections", SPI_processed);

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}


void
bdr_sequence_alloc(PG_FUNCTION_ARGS)
{

}

void
bdr_sequence_setval(PG_FUNCTION_ARGS)
{

}

Datum
bdr_sequence_options(PG_FUNCTION_ARGS)
{

}
