/* -------------------------------------------------------------------------
 *
 * bdr_init_replica.c
 *     Populate a new bdr node from the data in an existing node
 *
 * Use dump and restore, then bdr catchup mode, to bring up a new
 * bdr node into a bdr group. Allows a new blank database to be
 * introduced into an existing, already-working bdr group.
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_init_replica.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include "bdr.h"

#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "libpq/pqformat.h"

#include "access/heapam.h"
#include "access/xact.h"

#include "catalog/pg_type.h"

#include "executor/spi.h"

#include "bdr_replication_identifier.h"
#include "replication/walreceiver.h"

#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"


char *bdr_temp_dump_directory = NULL;

static void bdr_init_exec_dump_restore(BDRNodeInfo *node,
									   char *snapshot);

static void bdr_catchup_to_lsn(remote_node_info *ri, XLogRecPtr target_lsn);

/*
 * Make sure remote node has BDR activated (insert the security label).
 *
 * This is only needed for UDR.
 */
static void
bdr_remote_activate(PGconn *pgconn)
{
	PGresult		   *res;

	res = PQexec(pgconn, "SELECT bdr.internal_update_seclabel()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(FATAL, "bdr: Failed to activate remote node during bdr init: state %s: %s\n",
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	PQclear(res);
}

static XLogRecPtr
bdr_get_remote_lsn(PGconn *conn)
{
	XLogRecPtr  lsn;
	PGresult   *res;

	res = PQexec(conn, "SELECT pg_current_xlog_insert_location()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(ERROR, "Unable to get remote LSN: status %s: %s\n",
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	Assert(PQntuples(res) == 1);
	Assert(!PQgetisnull(res, 0, 0));
	lsn = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
					  CStringGetDatum(PQgetvalue(res, 0, 0))));
	PQclear(res);
	return lsn;
}

static void
bdr_get_remote_ext_version(PGconn *pgconn, char **default_version,
						   char **installed_version)
{
	PGresult *res;

	const char *q_bdr_installed =
		"SELECT default_version, installed_version "
		"FROM pg_catalog.pg_available_extensions WHERE name = 'bdr';";

	res = PQexec(pgconn, q_bdr_installed);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(ERROR, "Unable to get remote bdr extension version; query %s failed with %s: %s\n",
			q_bdr_installed, PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	if (PQntuples(res) == 1)
	{
		/*
		 * bdr ext is known to Pg, check install state.
		 */
		*default_version = pstrdup(PQgetvalue(res, 0, 0));
		*installed_version = pstrdup(PQgetvalue(res, 0, 0));
	}
	else if (PQntuples(res) == 0)
	{
		/* bdr ext is not known to Pg at all */
		*default_version = NULL;
		*installed_version = NULL;
	}
	else
	{
		Assert(false); /* Should not get >1 tuples */
	}

	PQclear(res);
}

/*
 * Make sure the bdr extension is installed on the other end. If it's a known
 * extension but not present in the current DB error out and tell the user to
 * activate BDR then try again.
 */
void
bdr_ensure_ext_installed(PGconn *pgconn)
{
	char *default_version = NULL;
	char *installed_version = NULL;

	bdr_get_remote_ext_version(pgconn, &default_version, &installed_version);

	if (default_version == NULL || strcmp(default_version, "") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("Remote PostgreSQL install for bdr connection does not have bdr extension installed"),
				 errdetail("no entry with name 'bdr' in pg_available_extensions."),
				 errhint("You need to install the BDR extension on the remote end")));
	}

	if (installed_version == NULL || strcmp(installed_version, "") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("Remote database for BDR connection does not have the bdr extension active"),
				 errdetail("installed_version for entry 'bdr' in pg_available_extensions is blank"),
				 errhint("Run 'CREATE EXTENSION bdr;'")));
	}

	pfree(default_version);
	pfree(installed_version);
}

static void
bdr_init_replica_cleanup_tmpdir(int errcode, Datum tmpdir)
{
	struct stat st;
	const char* dir = DatumGetCString(tmpdir);

	if (stat(dir, &st) == 0)
		if (!rmtree(dir, true))
			elog(WARNING, "Failed to clean up bdr dump temporary directory %s on exit/error", dir);
}

/*
 * Use a script to copy the contents of a remote node using pg_dump and apply
 * it to the local node. Runs during node join creation to bring up a new
 * logical replica from an existing node. The remote dump is taken from the
 * start position of a slot on the remote end to ensure that we never replay
 * changes included in the dump and never miss changes.
 */
static void
bdr_init_exec_dump_restore(BDRNodeInfo *node,
						   char *snapshot)
{
#ifndef WIN32
	pid_t pid;
	char *bindir;
	char *tmpdir;
	char  bdr_init_replica_script_path[MAXPGPATH];
	char  bdr_dump_path[MAXPGPATH];
	char  bdr_restore_path[MAXPGPATH];
	StringInfoData path;
	StringInfoData origin_dsn;
	StringInfoData local_dsn;
	int   saved_errno;
	uint32	bin_version;

	initStringInfo(&path);
	initStringInfo(&origin_dsn);
	initStringInfo(&local_dsn);

	bindir = pstrdup(my_exec_path);
	get_parent_directory(bindir);

	if (bdr_find_other_exec(my_exec_path, BDR_INIT_REPLICA_CMD,
						&bin_version,
						&bdr_init_replica_script_path[0]) < 0)
	{
		elog(ERROR, "bdr node init failed to find " BDR_INIT_REPLICA_CMD
			 " relative to binary %s",
			 my_exec_path);
	}
	if (bin_version / 100 != PG_VERSION_NUM / 100)
	{
		elog(ERROR, "bdr node init found " BDR_INIT_REPLICA_CMD
			 " with wrong major version %d.%d, expected %d.%d",
			 bin_version / 100 / 100, bin_version / 100 % 100,
			 PG_VERSION_NUM / 100 / 100, PG_VERSION_NUM / 100 % 100);
	}

	if (bdr_find_other_exec(my_exec_path, BDR_DUMP_CMD,
						&bin_version,
						&bdr_dump_path[0]) < 0)
	{
		elog(ERROR, "bdr node init failed to find " BDR_DUMP_CMD
			 " relative to binary %s",
			 my_exec_path);
	}
	if (bin_version / 100 != PG_VERSION_NUM / 100)
	{
		elog(ERROR, "bdr node init found " BDR_DUMP_CMD
			 " with wrong major version %d.%d, expected %d.%d",
			 bin_version / 100 / 100, bin_version / 100 % 100,
			 PG_VERSION_NUM / 100 / 100, PG_VERSION_NUM / 100 % 100);
	}

	if (bdr_find_other_exec(my_exec_path, BDR_RESTORE_CMD,
						&bin_version,
						&bdr_restore_path[0]) < 0)
	{
		elog(ERROR, "bdr node init failed to find " BDR_RESTORE_CMD
			 " relative to binary %s",
			 my_exec_path);
	}
	if (bin_version / 100 != PG_VERSION_NUM / 100)
	{
		elog(ERROR, "bdr node init found " BDR_RESTORE_CMD
			 " with wrong major version %d.%d, expected %d.%d",
			 bin_version / 100 / 100, bin_version / 100 % 100,
			 PG_VERSION_NUM / 100 / 100, PG_VERSION_NUM / 100 % 100);
	}


	appendStringInfo(&origin_dsn,
					 "%s fallback_application_name='"BDR_LOCALID_FORMAT": init_replica dump'",
					 node->init_from_dsn, BDR_LOCALID_FORMAT_ARGS);

	appendStringInfo(&local_dsn,
					 "%s fallback_application_name='"BDR_LOCALID_FORMAT": init_replica restore'",
					 node->local_dsn, BDR_LOCALID_FORMAT_ARGS);

	/*
	 * Suppress replication of changes applied via pg_restore back to
	 * the local node.
	 *
	 * TODO: This should PQconninfoParse, modify the options keyword or add
	 * it, and reconstruct the string using the functions from pg_dumpall
	 * (also to be used for init_copy). Simply appending the options
	 * instead is a bit dodgy.
	 */
	appendStringInfoString(&local_dsn,
						   " options='-c bdr.do_not_replicate=on -c bdr.permit_unsafe_ddl_commands=on -c bdr.skip_ddl_replication=on -c bdr.skip_ddl_locking=on'");

	tmpdir = palloc(strlen(bdr_temp_dump_directory)+32);
	sprintf(tmpdir, "%s/postgres-bdr-%s.%d", bdr_temp_dump_directory,
			snapshot, getpid());

	if (mkdir(tmpdir, 0700))
	{
		saved_errno = errno;
		if (saved_errno == EEXIST)
		{
			/*
			 * Target is an existing dir that somehow wasn't cleaned up or
			 * something more sinister. We'll just die here, and let the
			 * postmaster relaunch us and retry the whole operation.
			 */
			elog(ERROR, "bdr init_replica: Temporary dump directory %s exists: %s",
				 tmpdir, strerror(saved_errno));
		}
		else
		{
			elog(ERROR, "bdr init_replica: Failed to create temp directory: %s",
				 strerror(saved_errno));
		}
	}

	pid = fork();
	if (pid < 0)
		elog(FATAL, "can't fork to create initial replica");
	else if (pid == 0)
	{
		int n = 0;

		char *const argv[] = {
			bdr_init_replica_script_path,
			"--snapshot", snapshot,
			"--source", origin_dsn.data,
			"--target", local_dsn.data,
			"--tmp-directory", tmpdir,
			"--pg-dump-path", bdr_dump_path,
			"--pg-restore-path", bdr_restore_path,
			NULL
		};

		ereport(LOG,
				(errmsg("Creating replica with: %s --snapshot %s --source \"%s\" --target \"%s\" --tmp-directory \"%s\", --pg-dump-path \"%s\", --pg-restore-path \"%s\"",
						bdr_init_replica_script_path, snapshot,
						node->init_from_dsn, node->local_dsn, tmpdir,
						bdr_dump_path, bdr_restore_path)));

		n = execv(bdr_init_replica_script_path, argv);
		if (n < 0)
			_exit(n);
	}
	else
	{
		pid_t res;
		int exitstatus = 0;

		elog(DEBUG3, "Waiting for %s pid %d",
			 bdr_init_replica_script_path, pid);

		PG_ENSURE_ERROR_CLEANUP(bdr_init_replica_cleanup_tmpdir,
								CStringGetDatum(tmpdir));
		{
			do
			{
				res = waitpid(pid, &exitstatus, WNOHANG);
				if (res < 0)
				{
					if (errno == EINTR || errno == EAGAIN)
						continue;
					elog(FATAL, "bdr_exec_init_replica: error calling waitpid");
				}
				else if (res == pid)
					break;

				pg_usleep(10 * 1000);
				CHECK_FOR_INTERRUPTS();
			}
			while (1);

			elog(DEBUG3, "%s exited with waitpid return status %d",
				 bdr_init_replica_script_path, exitstatus);

			if (exitstatus != 0)
			{
				if (WIFEXITED(exitstatus))
					elog(FATAL, "bdr: %s exited with exit code %d",
						 bdr_init_replica_script_path, WEXITSTATUS(exitstatus));
				if (WIFSIGNALED(exitstatus))
					elog(FATAL, "bdr: %s exited due to signal %d",
						 bdr_init_replica_script_path, WTERMSIG(exitstatus));
				elog(FATAL, "bdr: %s exited for an unknown reason with waitpid return %d",
					 bdr_init_replica_script_path, exitstatus);
			}
		}
		PG_END_ENSURE_ERROR_CLEANUP(bdr_init_replica_cleanup_tmpdir,
									PointerGetDatum(tmpdir));
		bdr_init_replica_cleanup_tmpdir(0, CStringGetDatum(tmpdir));
	}

	pfree(tmpdir);
#else
	/*
	 * On Windows we should be using CreateProcessEx instead of fork() and
	 * exec().  We should add an abstraction for this to port/ eventually,
	 * so this code doesn't have to care about the platform.
	 *
	 * TODO
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("init_replica isn't supported on Windows yet")));
#endif
}

/*
 * BDR state synchronization.
 */
static void
bdr_sync_nodes(PGconn *remote_conn, BDRNodeInfo *local_node)
{
	PGconn *local_conn;

	local_conn = bdr_connect_nonrepl(local_node->local_dsn, "init");

	PG_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&local_conn));
	{
		StringInfoData query;
		PGresult   *res;
		char		sysid_str[33];
		const char *const setup_query =
			"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
			"SET LOCAL search_path = bdr, pg_catalog;\n"
			"SET LOCAL bdr.permit_unsafe_ddl_commands = on;\n"
			"SET LOCAL bdr.skip_ddl_replication = on;\n"
			"SET LOCAL bdr.skip_ddl_locking = on;\n"
			"LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;\n"
			"LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;\n";

		/* Setup the environment. */
		res = PQexec(remote_conn, setup_query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			elog(ERROR, "BEGIN or table locking on remote failed: %s",
					PQresultErrorMessage(res));
		PQclear(res);

		res = PQexec(local_conn, setup_query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			elog(ERROR, "BEGIN or table locking on local failed: %s",
					PQresultErrorMessage(res));
		PQclear(res);

		/* Copy remote bdr_nodes entries to the local node. */
		bdr_copytable(remote_conn, local_conn,
					  "COPY (SELECT * FROM bdr.bdr_nodes) TO stdout",
					  "COPY bdr.bdr_nodes FROM stdin");

		/* Copy the local entry to remote node. */
		initStringInfo(&query);
		/* No need to quote as everything is numbers. */
		snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT, local_node->id.sysid);
		sysid_str[sizeof(sysid_str)-1] = '\0';
		appendStringInfo(&query,
						 "COPY (SELECT * FROM bdr.bdr_nodes WHERE "
							"node_sysid = '%s' AND node_timeline = '%u' "
							"AND node_dboid = '%u') TO stdout",
						 sysid_str, local_node->id.timeline, local_node->id.dboid);

		bdr_copytable(local_conn, remote_conn,
					  query.data, "COPY bdr.bdr_nodes FROM stdin");

		/*
		 * Copy remote connections to the local node.
		 *
		 * Adding local connection to remote node is handled separately
		 * because it triggers the connect-back process on the remote node(s).
		 */
		bdr_copytable(remote_conn, local_conn,
					  "COPY (SELECT * FROM bdr.bdr_connections) TO stdout",
					  "COPY bdr.bdr_connections FROM stdin");

		/* Save changes. */
		res = PQexec(remote_conn, "COMMIT");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			elog(ERROR, "COMMIT on remote failed: %s",
					PQresultErrorMessage(res));
		PQclear(res);

		res = PQexec(local_conn, "COMMIT");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			elog(ERROR, "COMMIT on remote failed: %s",
					PQresultErrorMessage(res));
		PQclear(res);
	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
								PointerGetDatum(&local_conn));
	PQfinish(local_conn);
}

static void
bdr_insert_remote_conninfo(PGconn *conn, BdrConnectionConfig *myconfig)
{
#define INTERNAL_NODE_JOIN_NPARAMS 6
	PGresult   *res;
	Oid			types[INTERNAL_NODE_JOIN_NPARAMS] = { TEXTOID, OIDOID, OIDOID, TEXTOID, INT4OID, TEXTARRAYOID };
	const char *values[INTERNAL_NODE_JOIN_NPARAMS];
	StringInfoData		replicationsets;

	/* Needs to fit max length of UINT64_FORMAT */
	char				sysid_str[33];
	char				tlid_str[33];
	char				mydatabaseid_str[33];
	char				apply_delay[33];

	initStringInfo(&replicationsets);

	stringify_my_node_identity(sysid_str, sizeof(sysid_str),
							   tlid_str, sizeof(tlid_str),
							   mydatabaseid_str, sizeof(mydatabaseid_str));

	values[0] = &sysid_str[0];
	values[1] = &tlid_str[0];
	values[2] = &mydatabaseid_str[0];
	values[3] = myconfig->dsn;

	snprintf(&apply_delay[0], 33, "%d", myconfig->apply_delay);
	values[4] = &apply_delay[0];
	/*
	 * Replication sets are stored as a quoted identifier list. To turn
	 * it into an array literal we can just wrap some brackets around it.
	 */
	appendStringInfo(&replicationsets, "{%s}", myconfig->replication_sets);
	values[5] = replicationsets.data;

	res = PQexecParams(conn,
					   "SELECT bdr.internal_node_join($1,$2,$3,$4,$5,$6);",
					   INTERNAL_NODE_JOIN_NPARAMS,
					   types, &values[0], NULL, NULL, 0);

	/*
	 * bdr.internal_node_join() must correctly handle unique violations.
	 * Otherwise init that resumes after slot creation, when we're waiting
	 * for inbound slots, will fail.
	 */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		elog(ERROR, "unable to update remote bdr.bdr_connections: %s",
					PQerrorMessage(conn));

#undef INTERNAL_NODE_JOIN_NPARAMS
}

/*
 * Find all connections other than our own using the copy of
 * bdr.bdr_connections that we acquired from the remote server during
 * apply. Apply workers won't be started yet, we're just making the
 * slots.
 *
 * If the slot already exists from a prior attempt we'll leave it
 * alone. It'll be advanced when we start replaying from it anyway,
 * and it's guaranteed to retain more than the WAL we need.
 */
static void
bdr_init_make_other_slots()
{
	List	   *configs;
	ListCell   *lc;
	MemoryContext old_context;

	Assert(!IsTransactionState());
	StartTransactionCommand();
	old_context = MemoryContextSwitchTo(TopMemoryContext);
	configs = bdr_read_connection_configs();
	MemoryContextSwitchTo(old_context);
	CommitTransactionCommand();

	foreach(lc, configs)
	{
		BdrConnectionConfig *cfg = lfirst(lc);
		PGconn *conn;
		NameData slot_name;
		uint64 sysid;
		TimeLineID timeline;
		Oid dboid;
		RepNodeId replication_identifier;
		char *snapshot;

		if (cfg->sysid == GetSystemIdentifier() &&
			cfg->timeline == ThisTimeLineID &&
			cfg->dboid == MyDatabaseId)
		{
			/* Don't make a slot pointing to ourselves */
			continue;
			bdr_free_connection_config(cfg);
		}

		conn = bdr_establish_connection_and_slot(cfg->dsn, "mkslot", &slot_name,
				&sysid, &timeline, &dboid, &replication_identifier,
				&snapshot);

		/* Ensure the slot points to the node the conn info says it should */
		if (cfg->sysid != sysid ||
			cfg->timeline != timeline ||
			cfg->dboid != dboid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("System identification mismatch between connection and slot"),
					 errdetail("Connection for "BDR_LOCALID_FORMAT" resulted in slot on node "BDR_LOCALID_FORMAT" instead of expected node",
							   cfg->sysid, cfg->timeline, cfg->dboid, EMPTY_REPLICATION_NAME,
							   sysid, timeline, dboid, EMPTY_REPLICATION_NAME)));
		}

		/* We don't require the snapshot IDs here */
		if (snapshot != NULL)
			pfree(snapshot);

		/* No replication for now, just close the connection */
		PQfinish(conn);

		elog(DEBUG2, "Ensured existence of slot %s on "BDR_LOCALID_FORMAT,
					 NameStr(slot_name), cfg->sysid, cfg->timeline, cfg->dboid,
					 EMPTY_REPLICATION_NAME);

		bdr_free_connection_config(cfg);
	}

	list_free(configs);
}

/*
 * For each outbound connection in bdr.bdr_connections we should have a local
 * replication slot created by a remote node using our connection info.
 *
 * Wait until all such entries are created and active, then return.
 */
static void
bdr_init_wait_for_slot_creation()
{
	List	   *configs;
	ListCell   *lc;
	Name*		slot_names;
	Size		n_slots;
	int			tup_idx, arr_idx;

	elog(INFO, "waiting for all inbound slots to be established");

	/*
	 * Determine the list of expected slot identifiers. These are
	 * inbound slots, so they're our db oid + the remote's bdr ident.
	 */
	StartTransactionCommand();
	configs = bdr_read_connection_configs();

	slot_names = (Name*)palloc0(sizeof(Name) * list_length(configs));

	n_slots = 0;
	foreach(lc, configs)
	{
		BdrConnectionConfig *cfg = lfirst(lc);
		Name slot_name;

		if (cfg->sysid == GetSystemIdentifier() &&
			cfg->timeline == ThisTimeLineID &&
			cfg->dboid == MyDatabaseId)
		{
			/* We won't see an inbound slot from our own node */
			continue;
		}

		/* There's no corresponding incoming slot for a unidirectional slot */
		if (cfg->is_unidirectional)
			continue;

		slot_name = (NameData*) palloc0(sizeof(NameData));
		bdr_slot_name(slot_name, cfg->sysid, cfg->timeline, cfg->dboid,
					  MyDatabaseId);

		elog(DEBUG2, "expecting inbound slot named %s", NameStr(*slot_name));

		slot_names[n_slots++] = slot_name;
	}

	/*
	 * Wait for each to be created. There's no useful way to be notified when a
	 * slot gets created, so just scan all slots to see if all the ones we want
	 * are present and active. If not, sleep and retry soon.
	 *
	 * This is a very inefficient approach but for the number of slots we're
	 * interested in it doesn't matter.
	 */
	SPI_connect();

	while (true)
	{
		Datum	values[1] = {MyDatabaseId};
		Oid		types[1] = {OIDOID};
		Size	n_slots_found = 0;

		SPI_execute_with_args("select slot_name "
							  "from pg_catalog.pg_replication_slots "
							  "where plugin = '"BDR_LIBRARY_NAME"' "
							  "and slot_type = 'logical' "
							  "and datoid = $1 and active",
							  1, types, values, NULL, false, 0);

		for (tup_idx = 0; tup_idx < SPI_processed; tup_idx++)
		{
			char	   *slot_name;

			slot_name = SPI_getvalue(SPI_tuptable->vals[tup_idx],
									 SPI_tuptable->tupdesc,
									 1);

			Assert(slot_name != NULL);

			/*
			 * Does this slot appear in the array of expected slots and if so,
			 * have we seen it already?
			 *
			 * This is O(m*n) for m existing slots and n expected slots, but
			 * really, for this many slots, who cares.
			 */
			for (arr_idx = 0; arr_idx < n_slots; arr_idx++)
			{
				if ( strcmp(NameStr(*slot_names[arr_idx]), slot_name) == 0 )
				{
					n_slots_found++;
					break;
				}
			}
		}

		if (n_slots_found == n_slots)
			break;

		elog(DEBUG2, "found %u of %u expected slots, sleeping",
			 (uint32)n_slots_found, (uint32)n_slots);

		pg_usleep(100000);
	}

	SPI_finish();

	CommitTransactionCommand();

	elog(INFO, "all inbound slots established");

	/*
	 * Should this also check all outbound workers are connected? Doing so
	 * isn't simple - checking for replication identifiers doesn't confirm that
	 * the connection is active. We'd need to talk to the apply workers or try
	 * to convey information via pg_stat_activity.
	 */
}

/*
 * TODO DYNCONF perform_pointless_transaction
 *
 * This is temporary code to be removed when the full part/join protocol is
 * introduced, at which point WAL messages should handle this. See comments on
 * call site.
 */
static void
perform_pointless_transaction(PGconn *conn, BDRNodeInfo *node)
{
	PGresult   *res;

	res = PQexec(conn, "BEGIN");
	Assert(PQresultStatus(res) == PGRES_COMMAND_OK);
	PQclear(res);

	/* This forces a real xid to be allocated even though we do no work */
	res = PQexec(conn, "SELECT txid_current()");
	Assert(PQresultStatus(res) == PGRES_TUPLES_OK);
	PQclear(res);

	/* BDR will still replicate the BEGIN and COMMIT of the empty tx */
	res = PQexec(conn, "COMMIT");
	Assert(PQresultStatus(res) == PGRES_COMMAND_OK);
	PQclear(res);
}

/*
 * Initialize the database, from a remote node if necessary.
 */
void
bdr_init_replica(BDRNodeInfo *local_node)
{
	char				status;
	PGconn			   *nonrepl_init_conn;
	StringInfoData		dsn;
	BdrConnectionConfig *local_conn_config;

	initStringInfo(&dsn);

	status = local_node->status;

	Assert(status != 'r');

	elog(DEBUG2, "bdr_init_replica");

	/*
	 * The local SPI transaction we're about to perform must do any writes as a
	 * local transaction, not as a changeset application from a remote node.
	 * That allows rows to be replicated to other nodes. So no replication_origin_id
	 * may be set.
	 */
	Assert(replication_origin_id == InvalidRepNodeId);

	/*
	 * Before starting workers we must determine if we need to copy initial
	 * state from a remote node. This is necessary unless we are the first node
	 * created or we've already completed init. If we'd already completed init
	 * we would've exited above.
	 */
	if (local_node->init_from_dsn == NULL)
	{
		if (status != 'b')
		{
			/*
			 * Even though there's no init_replica worker, the local bdr.bdr_nodes table
			 * has an entry for our (sysid,dbname) and it isn't status=r (checked above),
			 * this should never happen
			 */
			ereport(ERROR, (errmsg("bdr.bdr_nodes row with "BDR_LOCALID_FORMAT" exists and has status=%c, "
								   "but has init_from_dsn set to NULL",
					GetSystemIdentifier(), ThisTimeLineID, MyDatabaseId, EMPTY_REPLICATION_NAME, status)));
		}
		/*
		 * No connections have init_replica=t, so there's no remote copy to do.
		 * We still have to ensure that bdr.bdr_nodes.status is 'r' for this
		 * node so that slot creation is permitted.
		 *
		 * XXX: is this actually a good idea?
		 */
		elog(DEBUG2, "init_replica: Marking as root/standalone node");
		bdr_nodes_set_local_status('r');

		return;
	}

	local_conn_config = bdr_get_connection_config(
			local_node->id.sysid,
			local_node->id.timeline,
			local_node->id.dboid,
			true);

	elog(DEBUG1, "init_replica init from remote %s",
		 local_node->init_from_dsn);

	nonrepl_init_conn =
		bdr_connect_nonrepl(local_node->init_from_dsn, "init");

	PG_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&nonrepl_init_conn));
	{
		bdr_ensure_ext_installed(nonrepl_init_conn);

		switch (status)
		{
			case 'b':
				elog(DEBUG2, "initializing from clean state");
				break;

			case 'r':
				elog(ERROR, "unexpected state");

			case 'c':
				/*
				 * We were in catchup mode when we died. We need to resume catchup
				 * mode up to the expected LSN before switching over.
				 *
				 * To do that all we need to do is fall through without doing any
				 * slot re-creation, dump/apply, etc, and pick up where we do
				 * catchup.
				 *
				 * We won't know what the original catchup target point is, but we
				 * can just catch up to whatever xlog position the server is
				 * currently at, it's guaranteed to be later than the target
				 * position.
				 */
				elog(DEBUG2, "dump applied, need to continue catchup");
				break;

			case 'o':
				elog(DEBUG2, "dump applied and catchup completed, need to continue slot creation");
				break;

			case 'i':
				/*
				 * A previous init attempt seems to have failed.
				 * Clean up, then fall through to start setup
				 * again.
				 *
				 * We can't just re-use the slot and replication
				 * identifier that were created last time (if
				 * they were), because we have no way of getting
				 * the slot's exported snapshot after
				 * CREATE_REPLICATION_SLOT.
				 *
				 * We could drop and re-create the slot, but...
				 *
				 * We also have no way to undo a failed
				 * pg_restore, so if that phase fails it's
				 * necessary to do manual cleanup, dropping and
				 * re-creating the db.
				 *
				 * To avoid that We need to be able to run
				 * pg_restore --clean, and that needs a way to
				 * exclude the bdr schema, the bdr extension,
				 * and their dependencies like plpgsql and
				 * btree_gist. (TODO patch pg_restore for that)
				 */
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("previous init failed, manual cleanup is required"),
						 errdetail("Found bdr.bdr_nodes entry for "BDR_LOCALID_FORMAT" with state=i in remote bdr.bdr_nodes", BDR_LOCALID_FORMAT_ARGS),
						 errhint("Remove all replication identifiers and slots corresponding to this node from the init target node then drop and recreate this database and try again")));
				break;

			default:
				elog(ERROR, "unreachable %c", status); /* Unhandled case */
				break;
		}

		if (status == 'b')
		{
			char	   *init_snapshot = NULL;
			PGconn	   *init_repl_conn = NULL;
			NameData	slot_name;
			uint64		remote_sysid;
			TimeLineID  remote_timeline;
			Oid			remote_dboid;
			RepNodeId	repnodeid;

			elog(INFO, "initializing node");

			/*
			 * We're starting from scratch or have cleaned up a previous failed
			 * attempt.
			 */
			status = 'i';
			bdr_nodes_set_local_status(status);

			/*
			 * This is unidirectional subscribe, let the other node know that
			 * it should behave as BDR node (as it might be UDR node which does
			 * not require init).
			 */
			if (local_conn_config == NULL)
				bdr_remote_activate(nonrepl_init_conn);

			/*
			 * Now establish our slot on the target node, so we can replay
			 * changes from that node. It'll be used in catchup mode.
			 */
			init_repl_conn = bdr_establish_connection_and_slot(
								local_node->init_from_dsn,
								"init", &slot_name,
								&remote_sysid, &remote_timeline, &remote_dboid,
								&repnodeid, &init_snapshot);

			elog(INFO, "connected to target node "BDR_LOCALID_FORMAT
				 " with snapshot %s",
				 remote_sysid, remote_timeline, remote_dboid,
				 EMPTY_REPLICATION_NAME, init_snapshot);

			/*
			 * Take the remote dump and apply it. This will give us a local
			 * copy of bdr_connections to work from. It's guaranteed that
			 * everything after this dump will be accessible via the catchup
			 * mode slot created earlier.
			 */
			bdr_init_exec_dump_restore(local_node, init_snapshot);

			/*
			 * TODO DYNCONF copy replication identifier state
			 *
			 * Should copy the target node's pg_catalog.pg_replication_identifier
			 * state for each node to the local node, using the same snapshot
			 * we used to take the dump from the remote. Doing this ensures
			 * that when we create slots to the target nodes they'll begin
			 * replay from a position that's exactly consistent with what's
			 * in the dump.
			 *
			 * We'll still need catchup mode because there's no guarantee our
			 * newly created slots will force all WAL we'd need to be retained
			 * on each node. The target might be behind. So we should catchup
			 * replay until the replication identifier positions received from
			 * catchup are >= the creation positions of the slots we made.
			 *
			 * (We don't need to do this if we instead send a replay confirmation
			 * request and wait for a reply from each node.)
			 */

			PQfinish(init_repl_conn);
			pfree(init_snapshot);

			/*
			 * This is group join, copy the state (bdr_nodes and
			 * bdr_connections) over from the init node to our node.
			 */
			if (local_conn_config != NULL)
			{
				elog(DEBUG1, "syncing bdr_nodes and bdr_connections");
				bdr_sync_nodes(nonrepl_init_conn, local_node);
			}

			status = 'c';
			bdr_nodes_set_local_status(status);
			elog(DEBUG1, "dump and apply finished, preparing for catchup replay");
		}

		Assert(status != 'b');

		if (status == 'c')
		{
			XLogRecPtr min_remote_lsn;
			remote_node_info ri;

			/*
			 * Launch outbound connections to all other nodes. It doesn't
			 * matter that their slot horizons are after the dump was taken on
			 * the origin node, so we could never replay all the data we need
			 * if we switched to replaying from these slots now.  We'll be
			 * advancing them in catchup mode until they overtake their current
			 * position before switching to replaying from them directly.
			 */
			bdr_init_make_other_slots();

			/*
			 * Enter catchup mode and wait until we've replayed up to the LSN
			 * the remote was at when we started catchup.
			 *
			 * TODO: It's possible that this step can lose transactions that
			 * were committed on a 3rd party node before we made our slot on it
			 * but not replicated to the init target node until after we exit
			 * catchup mode. If we acquire the DDL lock during join we can know
			 * that can't happen, so we should do that.
			 */
			elog(DEBUG3, "getting LSN to replay to in catchup mode");
			min_remote_lsn = bdr_get_remote_lsn(nonrepl_init_conn);

			/*
			 * Catchup cannot complete if there isn't at least one remote transaction
			 * to replay. So we perform a dummy transaction on the target node.
			 *
			 * XXX This is a hack. What we really *should* be doing is asking
			 * the target node to send a catchup confirmation wal message, then
			 * wait until all its current peers (we aren' one yet) reply with
			 * confirmation. Then we should be replaying until we get
			 * confirmation of this from the init target node, rather than
			 * replaying to some specific LSN. The full part/join
			 * protocol should take care of this.
			 */
			elog(DEBUG3, "forcing a new transaction on the target node");
			perform_pointless_transaction(nonrepl_init_conn, local_node);

			bdr_get_remote_nodeinfo_internal(nonrepl_init_conn, &ri);

			/* Launch the catchup worker and wait for it to finish */
			elog(DEBUG1, "launching catchup mode apply worker");
			bdr_catchup_to_lsn(&ri, min_remote_lsn);

			free_remote_node_info(&ri);

			/*
			 * We're done with catchup. The next phase is inserting our
			 * conninfo, so set status=o
			 */
			status = 'o';
			bdr_nodes_set_local_status(status);
			elog(DEBUG1, "catchup worker finished, requesting slot creation");
		}

		/* To reach here we must be waiting for slot creation */
		Assert(status == 'o');

		/*
		 * It is now safe to start apply workers, as we've finished catchup.
		 * Doing so ensures that we will replay our own bdr.bdr_nodes changes
		 * from the target node and also makes sure we stay more up-to-date,
		 * reducing slot lag on other nodes.
		 */
		bdr_maintain_db_workers();

		/*
		 * Insert our connection info on the remote end. This will prompt
		 * the other end to connect back to us and make a slot, and will
		 * cause the other nodes to do the same when they receive the new
		 * row.
		 *
		 * It makes no sense to do this with UDR, where the peer doesn't
		 * connect back to us.
		 */
		if (local_conn_config != NULL)
		{
			elog(DEBUG1, "inserting our connection into into remote end");
			bdr_insert_remote_conninfo(nonrepl_init_conn, local_conn_config);
		}

		/*
		 * Wait for all outbound and inbound slot creation to be complete.
		 *
		 * The inbound slots aren't yet required to relay local writes to
		 * remote nodes, but they'll be used to write our catchup
		 * confirmation request WAL message, so we need them to exist.
		 *
		 * This makes no sense on UDR, where the init target doesn't
		 * connect back to us and no other inbound or outbound connections
		 * exist. It still gets run, but we won't find any inbound
		 * slots to look for.
		 */
		elog(DEBUG1, "waiting for all inbound slots to be created");
		bdr_init_wait_for_slot_creation();

		/*
		 * We now have inbound and outbound slots for all nodes, and
		 * we're caught up to a reasonably recent state from the target
		 * node thanks to the dump and catchup mode operation.
		 *
		 * Set the node state to 'r'eady and allow writes.
		 *
		 * TODO: Before we can really be sure we're ready we should be
		 * sending a replay confirmation request and waiting for all
		 * nodes to reply, so we know we have full communication.
		 */
		status = 'r';
		bdr_nodes_set_local_status(status);
		elog(INFO, "finished init_replica, ready to enter normal replication");
	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_cleanup_conn_close,
							PointerGetDatum(&nonrepl_init_conn));

	Assert(status == 'r');

	PQfinish(nonrepl_init_conn);
}

/*
 * Cleanup function after catchup; makes sure we free the bgworker
 * slot for the catchup worker.
 */
static void
bdr_catchup_to_lsn_cleanup(int code, Datum offset)
{
	uint32 worker_shmem_idx = DatumGetInt32(offset);

	/*
	 * Clear the worker's shared memory struct now we're done with it.
	 *
	 * There's no need to unregister the worker as it was registered with
	 * BGW_NEVER_RESTART.
	 */
	bdr_worker_shmem_free(&BdrWorkerCtl->slots[worker_shmem_idx], NULL);
}

/*
 * Launch a temporary apply worker in catchup mode (forward_changesets=t),
 * set to replay until the passed LSN.
 *
 * This worker will receive and apply all changes the remote server has
 * received since the snapshot we got our dump from was taken, including
 * those from other servers, and will advance the replication identifiers
 * associated with each remote node appropriately.
 *
 * When we finish applying and the worker exits, we'll be caught up with the
 * remote and in a consistent state where all our local replication identifiers
 * are consistent with the actual state of the local DB.
 */
static void
bdr_catchup_to_lsn(remote_node_info *ri, XLogRecPtr target_lsn)
{
	uint32 worker_shmem_idx;
	BdrWorker *worker;
	BdrApplyWorker *catchup_worker;

	elog(DEBUG1, "Registering bdr apply catchup worker for "BDR_LOCALID_FORMAT" to lsn %X/%X",
		 ri->sysid, ri->timeline, ri->dboid, EMPTY_REPLICATION_NAME,
		 (uint32)(target_lsn>>32), (uint32)target_lsn);

	Assert(bdr_worker_type == BDR_WORKER_PERDB);
	/* Create the shmem entry for the catchup worker */
	LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);
	worker = bdr_worker_shmem_alloc(BDR_WORKER_APPLY, &worker_shmem_idx);
	catchup_worker = &worker->data.apply;
	catchup_worker->dboid = MyDatabaseId;
	catchup_worker->remote_sysid = ri->sysid;
	catchup_worker->remote_timeline = ri->timeline;
	catchup_worker->remote_dboid = ri->dboid;
	catchup_worker->perdb = bdr_worker_slot;
	LWLockRelease(BdrWorkerCtl->lock);

	/*
	 * Launch the catchup worker, ensuring that we free the shmem slot for the
	 * catchup worker even if we hit an error.
	 *
	 * There's a small race between claiming the worker and entering the ensure
	 * cleanup block. The real consequences are pretty much nil, since this is
	 * really just startup code and all we leak is one shmem slot.
	 */
	PG_ENSURE_ERROR_CLEANUP(bdr_catchup_to_lsn_cleanup,
							Int32GetDatum(worker_shmem_idx));
	{
		BgwHandleStatus bgw_status;
		BackgroundWorker bgw;
		BackgroundWorkerHandle *bgw_handle;
		pid_t bgw_pid;
		pid_t prev_bgw_pid = 0;
		uint32 worker_arg;

		/* Special parameters for a catchup worker only */
		catchup_worker->replay_stop_lsn = target_lsn;
		catchup_worker->forward_changesets = true;

		/* and the BackgroundWorker, which is a regular apply worker */
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
		bgw.bgw_main = NULL;
		strncpy(bgw.bgw_library_name, BDR_LIBRARY_NAME, BGW_MAXLEN);
		strncpy(bgw.bgw_function_name, "bdr_apply_main", BGW_MAXLEN);

		bgw.bgw_restart_time = BGW_NEVER_RESTART;
		Assert(MyProc->pid != 0);
		bgw.bgw_notify_pid = MyProc->pid;

		Assert(worker_shmem_idx <= UINT16_MAX);
		worker_arg = (((uint32)BdrWorkerCtl->worker_generation) << 16) | (uint32)worker_shmem_idx;
		bgw.bgw_main_arg = Int32GetDatum(worker_arg);

		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "bdr: catchup apply to %X/%X",
				 (uint32)(target_lsn >> 32), (uint32)target_lsn);
		bgw.bgw_name[BGW_MAXLEN-1] = '\0';

		/* Launch the catchup worker and wait for it to start */
		RegisterDynamicBackgroundWorker(&bgw, &bgw_handle);
		bgw_status = WaitForBackgroundWorkerStartup(bgw_handle, &bgw_pid);
		prev_bgw_pid = bgw_pid;

		/*
		 * Sleep on our latch until we're woken by SIGUSR1 on bgworker state
		 * change, or by timeout. (We need a timeout because there's a race
		 * between bgworker start and our setting the latch; if it starts and
		 * dies again quickly we'll miss it and sleep forever w/o a timeout).
		 */
		while (bgw_status == BGWH_STARTED && bgw_pid == prev_bgw_pid)
		{
			int rc;
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   1000L);

			ResetLatch(&MyProc->procLatch);

			/* emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);

			/* Is our worker still replaying? */
			bgw_status = GetBackgroundWorkerPid(bgw_handle, &bgw_pid);
		}
		switch(bgw_status)
		{
			case BGWH_POSTMASTER_DIED:
				proc_exit(1);
				break;
			case BGWH_STOPPED:
				TerminateBackgroundWorker(bgw_handle);
				break;
			case BGWH_NOT_YET_STARTED:
			case BGWH_STARTED:
				/* Should be unreachable */
				elog(ERROR, "Unreachable case, bgw status %d", bgw_status);
				break;
		}
		pfree(bgw_handle);

		/*
		 * Stopped doesn't mean *successful*. The worker might've errored
		 * out. We have no way of getting its exit status, so we have to rely
		 * on it setting something in shmem on successful exit. In this case
		 * it will set replay_stop_lsn to InvalidXLogRecPtr to indicate that
		 * replay is done.
		 */
		if (catchup_worker->replay_stop_lsn != InvalidXLogRecPtr)
		{
			/* Worker must've died before it finished */
			elog(ERROR,
				 "catchup worker exited before catching up to target LSN %X/%X",
				 (uint32)(target_lsn>>32), (uint32)target_lsn);
		}
		else
		{
			elog(DEBUG1, "catchup worker caught up to target LSN");
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_catchup_to_lsn_cleanup,
								Int32GetDatum(worker_shmem_idx));

	bdr_catchup_to_lsn_cleanup(0, Int32GetDatum(worker_shmem_idx));

	/* We're caught up! */
}
