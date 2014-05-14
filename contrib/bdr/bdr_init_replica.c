/* -------------------------------------------------------------------------
 *
 * bdr_init_replica.c
 *     Populate a new bdr node from the data in an existing node
 *
 * Use dump and restore, then bdr catchup mode, to bring up a new
 * bdr node into a bdr group. Allows a new blank database to be
 * introduced into an existing, already-working bdr group.
 *
 * Copyright (C) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr_init_replica.c
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
#include "libpq-fe.h"
#include "miscadmin.h"

#include "libpq/pqformat.h"

#include "access/heapam.h"
#include "access/xact.h"

#include "catalog/pg_type.h"

#include "executor/spi.h"

#include "replication/replication_identifier.h"
#include "replication/walreceiver.h"

#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"

char *bdr_temp_dump_directory = NULL;

static void bdr_exec_init_replica(BdrConnectionConfig *cfg, char *snapshot);

static void bdr_catchup_to_lsn(int cfg_index,
							   XLogRecPtr target_lsn);

/*
 * Search BdrWorkerCtl for a worker in dbname with init_replica set and
 * return it. The first worker found is returned (previous code should've
 * ensured there can only be one). If no match is found, return null.
 *
 * Must be called with at least a share lock on BdrWorkerCtl->lock
 *
 */
static BdrWorker*
find_init_replica_worker(Name dbname)
{
	int off;

	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));
	/* Check whether one of our connections has init_replica set */
	for (off = 0; off < bdr_max_workers; off++)
	{
		BdrApplyWorker 	       *aw;
		BdrConnectionConfig	   *cfg;

		if (BdrWorkerCtl->slots[off].worker_type != BDR_WORKER_APPLY)
			continue;

		aw = &BdrWorkerCtl->slots[off].worker_data.apply_worker;
		cfg = bdr_connection_configs[aw->connection_config_idx];

		if ((strcmp(NameStr(cfg->dbname), NameStr(*dbname)) == 0)
			&& cfg->init_replica)
		{
			return &BdrWorkerCtl->slots[off];
		}
	}
	return NULL;
}

/*
 * Get this node's status value from the remote's bdr.bdr_nodes table
 * and return it.
 *
 * If no row is found, '\0' is returned.
 */
static char
bdr_get_remote_status(PGconn *pgconn, Name dbname)
{
	PGresult 		   *res;
	char 				status;
	Oid 				param_types[] = {NUMERICOID, TEXTOID};
	const char 		   *param_values[2];
	/* Needs to fit max length of UINT64_FORMAT */
	char				sysid_str[33];

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT,
			 GetSystemIdentifier());
	sysid_str[sizeof(sysid_str)-1] = '\0';

	param_values[0] = sysid_str;
	param_types[0] = NUMERICOID;

	param_values[1] = NameStr(*dbname);
	param_types[1] = TEXTOID;

	res = PQexecParams(pgconn,
					   "SELECT node_status FROM bdr.bdr_nodes "
					   "WHERE node_sysid = $1 AND node_dbname = $2 "
					   "FOR UPDATE",
					   2, param_types, param_values, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(FATAL, "bdr %s: Failed to get remote status during bdr init: "
			 "state %s: %s\n", NameStr(*dbname),
			 PQresStatus(PQresultStatus(res)),
			 PQresultErrorMessage(res));
	}
	if (PQntuples(res) == 0)
		/* No row found on remote, we're starting from scratch */
		status = '\0';
	else
	{
		char *status_str = PQgetvalue(res, 0, 0);
		Assert(strlen(status_str) == 1);
		status = status_str[0];
	}
	PQclear(res);

	return status;
}

/*
 * Update/delete/insert in bdr.bdr_nodes to ensure that the bdr.bdr_nodes row
 * for this worker's node ID matches the passed status before returning.
 *
 * The special case '\0' means "remove the row".
 *
 * No fancy upsert games are required here because we ensure that only one
 * worker can be initing any one database, and that node IDs are unique across
 * a group of BDR nodes.
 */
static char
bdr_set_remote_status(PGconn *pgconn, Name dbname,
					  const char status, const char prev_status)
{
	PGresult 		   *res;
	char			   *status_str;
	const uint64		sysid = GetSystemIdentifier();
	/* Needs to fit max length of UINT64_FORMAT */
	char 				sysid_str[33];

	if (status == prev_status)
		/* No action required (we could check the remote, but meh) */
		return status;

	snprintf(sysid_str, sizeof(sysid_str), UINT64_FORMAT,
			 GetSystemIdentifier());
	sysid_str[sizeof(sysid_str)-1] = '\0';

	if (status == '\0')
	{
		Oid			param_types[] = {NUMERICOID, TEXTOID};
		const char *param_values[2];
		char    	new_status;

		param_values[0] = sysid_str;
		param_values[1] = NameStr(*dbname);

		res = PQexecParams(pgconn,
						   "DELETE FROM bdr.bdr_nodes WHERE node_sysid = $1"
						   " AND node_dbname = $2 RETURNING node_status",
						   2, param_types, param_values, NULL, NULL, 0);

		elog(DEBUG2, "bdr %s: deleting bdr_nodes row with id " UINT64_FORMAT
			 " and node_dbname %s ", NameStr(*dbname), sysid, NameStr(*dbname));

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(FATAL, "bdr %s: Failed to delete row from bdr_nodes: status %s: %s\n",
				 NameStr(*dbname), PQresStatus(PQresultStatus(res)),
				 PQresultErrorMessage(res));
		}
		if (PQntuples(res) == 0)
		{
			/*
			 * If prev_status was '\0' we wouldn't be here, so we should've
			 * got a returned value.
			 */
			elog(FATAL, "bdr %s: bdr.bdr_nodes row for node_sysid="
				 UINT64_FORMAT
				 ", dbname='%s' missing, expected row with status=%c",
				 NameStr(*dbname), sysid, NameStr(*dbname), (int)prev_status);
		}
		status_str = PQgetvalue(res, 0, 0);
		Assert(strlen(status_str) == 1);
		new_status = status_str[0];

		if (new_status != prev_status)
		{
			elog(FATAL, "bdr %s: bdr.bdr_nodes row for node_sysid="
				 UINT64_FORMAT
				 ", dbname='%s' had status=%c, expected status=%c",
				 NameStr(*dbname), sysid, NameStr(*dbname),
				 (int) new_status, (int) prev_status);
		}

		PQclear(res);
	}
	else
	{
		Oid			param_types[] = {CHAROID, NUMERICOID, TEXTOID};
		const char *param_values[3];
		char		new_status;
		char		status_str[2];

		snprintf(status_str, 2, "%c", (int)status);
		param_values[0] = status_str;
		param_values[1] = sysid_str;
		param_values[2] = NameStr(*dbname);

		res = PQexecParams(pgconn,
						   "UPDATE bdr.bdr_nodes "
						   "SET node_status = $1 "
						   "WHERE node_sysid = $2 AND node_dbname = $3 "
						   "RETURNING ("
						   "SELECT node_status FROM bdr.bdr_nodes "
						   "WHERE node_sysid = $2 AND node_dbname = $3)",
						   3, param_types, param_values, NULL, NULL, 0);

		elog(DEBUG2, "bdr %s: update row with id "
			 UINT64_FORMAT
			 " and node_dbname %s from %c to %c",
			 NameStr(*dbname), sysid, NameStr(*dbname), prev_status, status);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(FATAL,
				 "bdr %s: Failed to update bdr.nodes row: status %s: %s\n",
				 NameStr(*dbname),
				 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}
		if (PQntuples(res) != 0)
		{
			char *new_status_str;
			/* Updated a row */
			new_status_str = PQgetvalue(res, 0, 0);
			Assert(strlen(status_str) == 1);
			new_status = new_status_str[0];
			if (new_status != prev_status)
			{
				elog(FATAL,
					 "bdr %s: bdr.bdr_nodes row for node_sysid=" UINT64_FORMAT
					 ", dbname='%s' had status=%c, expected status=%c",
					 NameStr(*dbname), sysid, NameStr(*dbname), (int)new_status,
					 (int)prev_status);
			}

			PQclear(res);
		}
		else
		{
			/* No rows affected, insert a new row instead. We re-use the previous
			 * query parameters. */
			PQclear(res);
			res = PQexecParams(pgconn,
							   "INSERT INTO bdr.bdr_nodes"
							   "    (node_status, node_sysid, node_dbname)"
							   "    VALUES ($1, $2, $3);",
							   3, param_types, param_values, NULL, NULL, 0);

			elog(DEBUG2, "bdr %s: insert row with id " UINT64_FORMAT
				 " and node_dbname %s from %c to %c",
				 NameStr(*dbname), sysid, NameStr(*dbname), prev_status, status);

			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				elog(FATAL,
					 "bdr %s: Failed to insert row into bdr.bdr_nodes: status %s: %s\n",
					 NameStr(*dbname), PQresStatus(PQresultStatus(res)),
					 PQresultErrorMessage(res));
			}
			PQclear(res);
		}
	}

	return status;
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

/*
 * Make sure the bdr extension is installed on the other end. If it's a known
 * extension but not present in the current DB error out and tell the user to
 * activate BDR then try again.
 */
static void
bdr_ensure_ext_installed(PGconn *pgconn, Name bdr_conn_name)
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
		char *default_version PG_USED_FOR_ASSERTS_ONLY;
		/*
		 * bdr ext is known to Pg, check install state.
		 *
		 * Right now we don't check the installed version or try to install/upgrade.
		 */
		default_version = PQgetvalue(res, 0, 0);
		Assert(default_version != NULL);
		if (PQgetisnull(res, 0, 1))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("Remote database for BDR connection %s does not have the bdr extension active",
					 NameStr(*bdr_conn_name)),
					 errdetail("no entry with name 'bdr' in pg_extensions"),
					 errhint("add 'bdr' to shared_preload_libraries in postgresql.conf "
					 		 "on the target server and restart it.")));
		}
	}
	else if (PQntuples(res) == 0)
	{
		/* bdr ext is not known to Pg at all */
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("Remote PostgreSQL install for bdr connection %s does not have bdr extension installed",
				 NameStr(*bdr_conn_name)),
				 errdetail("no entry with name 'bdr' in pg_available_extensions; did you install BDR?")));
	}
	else
	{
		Assert(false); /* Should not get >1 tuples */
	}
}

/*
 * Delete a replication identifier.
 *
 * This should really be in the replication identifier support code in
 * changeset extraction, as DeleteReplicationIdentifier or
 * DropReplicationIdentifier.
 *
 * If no matching identifier is found, takes no action.
 */
static void
bdr_delete_replication_identifier(RepNodeId repid)
{
	HeapTuple		tuple = NULL;
	Relation		rel;
	ItemPointerData	tid;

	/*
	 * Exclusively lock pg_replication_identifier
	 */
	rel = heap_open(ReplicationIdentifierRelationId, RowExclusiveLock);

	/*
	 * Look it up from the syscache and get a copy we can safely
	 * modify.
	 */
	tuple = GetReplicationInfoByIdentifier(repid, true);
	if (HeapTupleIsValid(tuple))
	{
		tid = tuple->t_self;
		ReleaseSysCache(tuple);
		simple_heap_delete(rel, &tid);
	}
	heap_close(rel, RowExclusiveLock);

	/*
	 * We should CHECKPOINT after this to make sure replication
	 * identifier state gets flushed.
	 */
	RequestCheckpoint(CHECKPOINT_IMMEDIATE|CHECKPOINT_FORCE);
}

static void
bdr_drop_slot_and_replication_identifier(BdrConnectionConfig *cfg)
{

	char		conninfo_repl[MAXCONNINFO + 75];
	char		remote_ident[256];
	PGconn	   *streamConn;
	RepNodeId   replication_identifier;
	NameData	slot_name;
	TimeLineID  timeline;
	uint64		sysid;
	PGresult   *res;
	StringInfoData query;
	char	   *sqlstate;

	elog(DEBUG1, "bdr %s: Dropping slot and local ident from connection %s",
		 NameStr(cfg->dbname), NameStr(cfg->name));

	snprintf(conninfo_repl, sizeof(conninfo_repl),
			 "%s replication=database fallback_application_name=bdr",
			 cfg->dsn);

	/* Establish BDR conn and IDENTIFY_SYSTEM */
	streamConn = bdr_connect(
		conninfo_repl,
		remote_ident, sizeof(remote_ident),
		&slot_name, &sysid, &timeline
		);

	StartTransactionCommand();
	replication_identifier = GetReplicationIdentifier(remote_ident, true);

	if (OidIsValid(replication_identifier))
	{
		/* Local replication identifier exists and must be dropped. */
		elog(DEBUG2, "bdr %s: Deleting local replication identifier %hu",
			 NameStr(cfg->dbname), replication_identifier);
		bdr_delete_replication_identifier(replication_identifier);
	}
	else
	{
		elog(DEBUG2, "bdr %s: No local replication identifier to delete",
			 NameStr(cfg->dbname));
	}

	/*
	 * Remove corresponding remote slot if it exists. We can't query
	 * whether it exists or not silently over the replication protocol,
	 * so we just try it and cope if it's missing.
	 */
	initStringInfo(&query);
	appendStringInfo(&query, "DROP_REPLICATION_SLOT %s", NameStr(slot_name));
	res = PQexec(streamConn, query.data);
	if (PQresultStatus(res) == PGRES_COMMAND_OK)
	{
		elog(DEBUG2, "bdr %s: remote replication slot %s deleted",
			 NameStr(cfg->dbname), NameStr(slot_name));
	}
	else
	{
		/* SQLSTATE 42704 expected; others are error conditions */
		sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		if (strcmp(sqlstate, "42704") != 0)
		{
			ereport(ERROR,
					(errmsg("'DROP_REPLICATION_SLOT %s' on bdr connection %s failed with sqlstate %s: %s",
							NameStr(slot_name), NameStr(cfg->name),
							sqlstate,PQresultErrorMessage(res))));
		}
		else
		{
			elog(DEBUG2, "bdr %s: No slot to delete", NameStr(cfg->dbname));
		}
	}
	CommitTransactionCommand();
	PQclear(res);
	PQfinish(streamConn);
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
 * it to the local node. Runs during slot creation to bring up a new logical
 * replica from an existing node.
 */
static void
bdr_exec_init_replica(BdrConnectionConfig *cfg, char *snapshot)
{
#ifndef WIN32
	pid_t pid;
	char *bindir;
	char *tmpdir;
	char  bdr_init_replica_script_path[MAXPGPATH];
	const char *envvar;
	StringInfoData path;
	int   saved_errno;

	initStringInfo(&path);

	bindir = pstrdup(my_exec_path);
	get_parent_directory(bindir);

	if (!find_other_exec(my_exec_path, BDR_INIT_REPLICA_CMD,
						 BDR_INIT_REPLICA_CMD " " PG_VERSION,
						 &bdr_init_replica_script_path[0]))
	{
		elog(ERROR, "bdr: failed to find " BDR_INIT_REPLICA_CMD
			 " in Pg bin dir or wrong version (expected %s)",
			 PG_VERSION);
	}

	if (cfg->replica_local_dsn == NULL)
		elog(FATAL, "bdr init_replica: no replica_local_dsn specified");

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

		char * envp[] = {
			NULL, /* to be replaced with PATH */
			NULL
		};
		char *const argv[] = {
			bdr_init_replica_script_path,
			"--snapshot", snapshot,
			"--source", cfg->dsn,
			"--target", cfg->replica_local_dsn,
			"--tmp-directory", tmpdir,
			NULL
		};

		envvar = getenv("PATH");
		appendStringInfoString(&path, "PATH=");
		appendStringInfoString(&path, bindir);
		if (envvar != NULL)
		{
			appendStringInfoString(&path, ":");
			appendStringInfoString(&path, envvar);
		}
		envp[0] = path.data;

		elog(DEBUG1, "Creating replica with: %s --snapshot %s --source \"%s\" --target \"%s\" --tmp-directory \"%s\"",
			 bdr_init_replica_script_path, snapshot, cfg->dsn,
			 cfg->replica_local_dsn, tmpdir);

		n = execve(bdr_init_replica_script_path, argv, envp);
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
 * Determine whether we need to initialize the database from a remote
 * node and perform the required initialization if so.
 */
void
bdr_init_replica(Name dbname)
{
	char status;
	XLogRecPtr min_remote_lsn;
	PGconn *nonrepl_init_conn;
	StringInfoData query;
	BdrWorker  *init_replica_worker;
	BdrConnectionConfig *init_replica_config;
	int spi_ret;

	initStringInfo(&query);

	elog(DEBUG2, "bdr %s: bdr_init_replica",
		 NameStr(*dbname));

	/*
	 * The local SPI transaction we're about to perform must do any writes as a
	 * local transaction, not as a changeset application from a remote node.
	 * That allows rows to be repliated to other nodes. So no replication_origin_id
	 * may be set.
	 */
	Assert(replication_origin_id == InvalidRepNodeId);

	/*
	 * Check the local bdr.bdr_nodes over SPI or direct scan to see if
	 * there's an entry for ourselves in ready mode already.
	 *
	 * Note that we don't have to explicitly SPI_finish(...) on error paths;
	 * that's taken care of for us.
	 */
	StartTransactionCommand();
	spi_ret = SPI_connect();
	if (spi_ret != SPI_OK_CONNECT)
		elog(ERROR, "SPI already connected; this shouldn't be possible");

	status = bdr_nodes_get_local_status(GetSystemIdentifier(), dbname);
	if (status == 'r')
	{
		/* Already in ready state, nothing more to do */
		SPI_finish();
		CommitTransactionCommand();
		return;
	}

	/*
	 * Before starting workers we must determine if we need to copy
	 * initial state from a remote node. This is only necessary if
	 * there is a connection with init_replica set and we do not yet
	 * have an entry in the local "bdr.bdr_nodes" table for our node
	 * ID showing initialisation to be complete.
	 */
	LWLockAcquire(BdrWorkerCtl->lock, LW_SHARED);
	init_replica_worker = find_init_replica_worker(dbname);
	LWLockRelease(BdrWorkerCtl->lock);
	if (!init_replica_worker)
	{
		if (status != '\0')
		{
			/*
			 * Even though there's no init_replica worker, the local bdr.bdr_nodes table
			 * has an entry for our (sysid,dbname) and it isn't status=r (checked above),
			 * we must've had an init_replica configured before, then removed.
			 */
			ereport(ERROR, (errmsg("bdr.bdr_nodes row with (sysid="
					UINT64_FORMAT ", dbname=%s) exists and has status=%c, but "
					"no connection with init_replica=t is configured for this "
					"database. ",
					GetSystemIdentifier(), NameStr(*dbname), status),
					errdetail("You probably configured initial setup with "
					"init_replica on a connection, then removed or changed that "
					"connection before setup completed properly. "),
					errhint("DROP and re-create the database if it has no "
					"existing content of value, or add the init_replica setting "
					"to one of the connections.")));
		}
		/*
		 * No connections have init_replica=t, so there's no remote copy to do.
		 * We still have to ensure that bdr.bdr_nodes.status is 'r' for this
		 * node so that slot creation is permitted.
		 */
		bdr_nodes_set_local_status(GetSystemIdentifier(), dbname, 'r');
	}
	/*
	 * We no longer require the transaction for SPI; further work gets done on
	 * the remote machine's bdr.bdr_nodes table and replicated back to us via
	 * pg_dump/pg_restore, or over the walsender protocol once we start
	 * replay. If we aren't just about to exit anyway.
	 */
	SPI_finish();
	CommitTransactionCommand();

	if (!init_replica_worker)
		/* Cleanup done and nothing more to do */
		return;


	init_replica_config = bdr_connection_configs
		[init_replica_worker->worker_data.apply_worker.connection_config_idx];
	elog(DEBUG2, "bdr %s: bdr_init_replica init from connection %s",
		 NameStr(*dbname), NameStr(init_replica_config->name));

	/*
	 * Test to see if there's an entry in the remote's bdr.bdr_nodes for our
	 * system identifier. If there is, that'll tell us what stage of startup
	 * we are up to and let us resume an incomplete start.
	 */
	nonrepl_init_conn = PQconnectdb(init_replica_config->dsn);
	if (PQstatus(nonrepl_init_conn) != CONNECTION_OK)
	{
		ereport(FATAL,
				(errmsg("bdr %s: could not connect to the upstream server in non-replication mode: %s",
						NameStr(*dbname),
						PQerrorMessage(nonrepl_init_conn))));
	}

	bdr_ensure_ext_installed(nonrepl_init_conn, dbname);
	elog(DEBUG2, "bdr %s: bdr extension is installed in remote",
		 NameStr(*dbname));

	/* Get the bdr.bdr_nodes status field for our node id from the remote */
	status = bdr_get_remote_status(nonrepl_init_conn, dbname);
	switch (status)
	{
		case '\0':
			elog(DEBUG2, "bdr %s: initializing from clean state",
				 NameStr(*dbname));
			break;

		case 'r':
			/*
			 * Init has been completed, but we didn't check our local
			 * bdr.bdr_nodes, or the final update hasn't propagated yet.
			 *
			 * All we need to do is catch up, we already replayed enough to be
			 * consistent and start up in normal mode last time around
			 */
			elog(DEBUG2, "bdr %s: init already completed, nothing to do",
				 NameStr(*dbname));
			return;

		case 'c':
			/*
			 * We were in catchup mode when we died. We need to resume catchup
			 * mode up to the expected LSN before switching over.
			 *
			 * To do that all we need to do is fall through without doing any
			 * slot re-creation, dump/apply, etc, and pick up when we do
			 * catchup.
			 *
			 * We won't know what the original catchup target point is, but we
			 * can just catch up to whatever xlog position the server is
			 * currently at.
			 */
			elog(DEBUG2, "bdr %s: dump applied, need to continue catchup",
				 NameStr(*dbname));
			break;

		case 'i':
			/*
			 * A previous init attempt seems to have failed. Clean up, then
			 * fall through to start setup again.
			 *
			 * We can't just re-use the slot and replication identifier that
			 * were created last time (if they were), because we have no way
			 * of getting the slot's exported snapshot after
			 * CREATE_REPLICATION_SLOT.
			 */
			elog(DEBUG2, "bdr %s: previous failed initalization detected, cleaning up",
				 NameStr(*dbname));
			bdr_drop_slot_and_replication_identifier(init_replica_config);
			status = bdr_set_remote_status(nonrepl_init_conn, dbname,
										   '\0', status);
			break;

		default:
			elog(ERROR, "unreachable"); /* Unhandled case */
			break;
	}

	if (status == '\0')
	{
		int			off;
		int		   *my_conn_idxs;
		int			n_conns = 0;
		char	   *init_snapshot = NULL;
		PGconn	   *init_repl_conn = NULL;

		elog(LOG, "bdr %s: initializing from remote db", NameStr(*dbname));

		/*
		 * We're starting from scratch or have cleaned up a previous failed
		 * attempt.
		 */
		status = bdr_set_remote_status(nonrepl_init_conn, dbname,
									   'i', status);

		my_conn_idxs = (int*)palloc(sizeof(Size) * bdr_max_workers);

		/* Collect a list of connections to make slots for. */
		LWLockAcquire(BdrWorkerCtl->lock, LW_SHARED);
		for (off = 0; off < bdr_max_workers; off++)
		{
			BdrWorker 			   *worker = &BdrWorkerCtl->slots[off];
			BdrConnectionConfig	   *cfg;

			cfg = bdr_connection_configs
				[worker->worker_data.apply_worker.connection_config_idx];

			if (worker->worker_type == BDR_WORKER_APPLY
				&& strcmp(NameStr(cfg->dbname), NameStr(*dbname)) == 0)
				my_conn_idxs[n_conns++] = off;
		}
		LWLockRelease(BdrWorkerCtl->lock);

		elog(DEBUG2, "bdr %s: creating slots for %d nodes",
			 NameStr(*dbname), n_conns);

		/*
		 * For each connection, ensure its slot exists.
		 *
		 * Do it one by one rather than fiddling with async libpq queries. If
		 * this needs to be parallelized later, it should probably be done by
		 * launching each apply worker and letting them create their own
		 * slots, then having them wait until signalled/unlatched before
		 * proceeding with actual replication. That'll save us another round
		 * of connections too.
		 *
		 * We don't attempt any cleanup if slot creation fails, we just bail out
		 * and leave any already-created slots in place.
		 */
		for (off = 0; off < n_conns; off++)
		{
			BdrWorker *w = &BdrWorkerCtl->slots[my_conn_idxs[off]];
			BdrConnectionConfig *cfg;
			char *snapshot = NULL;
			PGconn *conn = NULL;
			RepNodeId replication_identifier;
			NameData slot_name;
			uint64 sysid;
			TimeLineID timeline;

			cfg = bdr_connection_configs
				[w->worker_data.apply_worker.connection_config_idx];

			elog(DEBUG1, "bdr %s: checking/creating slot for %s",
				 NameStr(*dbname), NameStr(cfg->name));
			/*
			 * Create the slot on the remote. The returned remote sysid and
			 * timeline, the slot name, and the local replication identifier
			 * are all discarded; they're not needed here, and will be obtained
			 * again by the apply workers when they're launched after init.
			 */
			conn = bdr_establish_connection_and_slot(cfg, &slot_name, &sysid,
				&timeline, &replication_identifier, &snapshot);

			/* Always throws rather than returning failure */
			Assert(conn);

			if (&BdrWorkerCtl->slots[off] == init_replica_worker)
			{
				/*
				 * We need to keep the snapshot ID returned by CREATE SLOT so
				 * we can pass it to pg_dump to get a consistent dump from the
				 * remote slot's start point.
				 *
				 * The snapshot is only valid for the lifetime of the
				 * replication connection we created it with, so we must keep
				 * that connection around until the dump finishes.
				 */
				if (!snapshot)
					elog(ERROR, "bdr %s: init_replica failed to create snapshot!",
						 NameStr(*dbname));
				init_snapshot = snapshot;
				init_repl_conn = conn;
			}
			else
			{
				/*
				 * Just throw the returned info away; we only needed to create
				 * the slot so its replication identifier can be advanced
				 * during catchup.
				 */
				if (snapshot)
					pfree(snapshot);
				PQfinish(conn);
			}
		}

		pfree(my_conn_idxs);

		/* If we get here, we should have a valid snapshot to dump */
		Assert(init_snapshot != NULL);
		Assert(init_repl_conn != NULL);

		/*
		 * Execute the dump and apply its self.
		 *
		 * Note that the bdr extension tables override pg_dump's default and
		 * ask to be included in dumps. In particular, bdr.bdr_nodes will get
		 * copied over.
		 */
		elog(DEBUG1, "bdr %s: creating and restoring dump for %s",
			 NameStr(*dbname), NameStr(init_replica_config->name));
		bdr_exec_init_replica(init_replica_config, init_snapshot);
		PQfinish(init_repl_conn);

		pfree(init_snapshot);
		status = bdr_set_remote_status(nonrepl_init_conn, dbname, 'c', status);
	}

	Assert(status == 'c');

	/* Launch the catchup worker and wait for it to finish */
	elog(DEBUG1, "bdr %s: launching catchup mode apply worker", NameStr(*dbname));
	min_remote_lsn = bdr_get_remote_lsn(nonrepl_init_conn);
	bdr_catchup_to_lsn(
		init_replica_worker->worker_data.apply_worker.connection_config_idx,
		min_remote_lsn);
	status = bdr_set_remote_status(nonrepl_init_conn, dbname, 'r', status);

	elog(INFO, "bdr %s: catchup worker finished, ready for normal replication",
		 NameStr(*dbname));
	PQfinish(nonrepl_init_conn);
}

/*
 * Cleanup function after catchup; makes sure we free the bgworker
 * slot for the catchup worker.
 */
static void
bdr_catchup_to_lsn_cleanup(int code, Datum offset)
{
	int worker_shmem_idx = DatumGetInt32(offset);

	/* Clear the worker's shared memory struct now we're done with it */
	bdr_worker_shmem_release(&BdrWorkerCtl->slots[worker_shmem_idx], NULL);
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
 *
 * Arguments:
 *
 * cfg_index: Index of the bdr connection for this dbname with init_worker=t
 * set within bdr_connection_configs. Used to start the worker.
 *
 * target_lsn: LSN of immediate origin node at which catchup should stop.
 */
static void
bdr_catchup_to_lsn(int cfg_index,
				   XLogRecPtr target_lsn)
{
	int worker_shmem_idx;
	pid_t bgw_pid;
	BdrApplyWorker *catchup_worker;
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;
	BgwHandleStatus bgw_status;
	BdrConnectionConfig *cfg;

	cfg = bdr_connection_configs[cfg_index];
	Assert(cfg != NULL);
	Assert(cfg->init_replica);

	elog(DEBUG1, "Registering bdr apply catchup worker %s for db %s to lsn %X/%X",
		 NameStr(cfg->name), NameStr(cfg->dbname),
		 (uint32)(target_lsn>>32), (uint32)target_lsn);

	/* Create the shmem entry for the catchup worker */
	LWLockAcquire(BdrWorkerCtl->lock, LW_SHARED);
	for (worker_shmem_idx = 0; worker_shmem_idx < bdr_max_workers; worker_shmem_idx++)
	{
		BdrWorker *worker = &BdrWorkerCtl->slots[worker_shmem_idx];
		if (worker->worker_type == BDR_WORKER_EMPTY_SLOT)
			break;
	}
	if (worker_shmem_idx == bdr_max_workers)
	{
		LWLockRelease(BdrWorkerCtl->lock);
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("No free bdr worker slots, bdr_max_workers=%d too low",
						bdr_max_workers)));
	}
	BdrWorkerCtl->slots[worker_shmem_idx].worker_type = BDR_WORKER_APPLY;
	catchup_worker = &BdrWorkerCtl->slots[worker_shmem_idx].worker_data.apply_worker;
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
		pid_t prev_bgw_pid = 0;

		/* Make sure the catchup worker can find its bdr.xxx_ GUCs */
		catchup_worker->connection_config_idx = cfg_index;

		/* Special parameters for a catchup worker only */
		catchup_worker->replay_stop_lsn = target_lsn;
		catchup_worker->forward_changesets = true;

		/* and the BackgroundWorker, which is a regular apply worker */
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
		/* TODO: For EXEC_BACKEND we must use bgw_library_name & bgw_function_name */
		bgw.bgw_main = bdr_apply_main;
		/*
		 * Would prefer this to be BGW_NEVER_RESTART but it's not honoured for
		 * exit 0 anyway. Set a small delay to give us time to unregister the
		 * worker after it exits, before the replacement is started. See below
		 * in the switch handling the exit cases for more detail.
		 */
		bgw.bgw_restart_time = 5;
		bgw.bgw_notify_pid = MyProc->pid;
		bgw.bgw_main_arg = Int32GetDatum(worker_shmem_idx);

		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "bdr %s: catchup apply to %X/%X on %s",
				 NameStr(cfg->dbname),
				 (uint32)(target_lsn >> 32), (uint32)target_lsn,
				 NameStr(cfg->name));
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
			case BGWH_NOT_YET_STARTED:
			case BGWH_STARTED:
				/*
				 * You'd think we'd only get here in the STOPPED case, but Pg
				 * restarts our bgworker even if we set BGW_NEVER_RESTART if we
				 * exit 0.
				 *
				 * If the pid changes, we know the worker exited, even if it's
				 * reported as running. So we make sure we always terminate it.
				 */
				TerminateBackgroundWorker(bgw_handle);
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
				 "bdr %s: catchup worker exited before catching up to target LSN %X/%X",
				 NameStr(cfg->dbname),
				 (uint32)(target_lsn>>32), (uint32)target_lsn);
		}
		else
		{
			elog(DEBUG1, "bdr %s: catchup worker caught up to target LSN",
				 NameStr(cfg->dbname));
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(bdr_catchup_to_lsn_cleanup,
								Int32GetDatum(worker_shmem_idx));

	bdr_catchup_to_lsn_cleanup(0, Int32GetDatum(worker_shmem_idx));

	/* We're caught up! */
}
