/* -------------------------------------------------------------------------
 *
 * bdr_init_copy.c
 *		Initialize a new bdr node from a physical base backup
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_conflict_logging.c
 *
 * -------------------------------------------------------------------------
 */

#include <dirent.h>
#include <fcntl.h>
#include <locale.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>

#include "libpq-fe.h"
#include "libpq-int.h"

#include "postgres_fe.h"

#include "getopt_long.h"

#include "port.h"

#include "miscadmin.h"

#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"

#include "bdr_config.h"
#include "bdr_internal.h"

#define LLOGCDIR "pg_logical/checkpoints"

typedef struct RemoteInfo {
	uint64		sysid;
	TimeLineID	tlid;
	int			version;
	int			numdbs;
	Oid		   *dboids;
	char	  **dbnames;
	char	  **replication_sets;
} RemoteInfo;

typedef struct NodeInfo {
	uint64		remote_sysid;
	TimeLineID	remote_tlid;
	uint64		local_sysid;
	TimeLineID	local_tlid;
} NodeInfo;

typedef enum {
	VERBOSITY_NORMAL,
	VERBOSITY_VERBOSE,
	VERBOSITY_DEBUG
} VerbosityLevelEnum;

static char		   *argv0 = NULL;
static const char  *progname;
static char		   *data_dir = NULL;
static char			pid_file[MAXPGPATH];
static time_t		start_time;
static VerbosityLevelEnum	verbosity = VERBOSITY_NORMAL;

/* defined as static so that die() can close them */
static PGconn		*local_conn = NULL;
static PGconn		*remote_conn = NULL;

static void signal_handler(int sig);
static void usage(void);
static void BDR_NORETURN die(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
static void print_msg(VerbosityLevelEnum level, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

static int BDR_WARN_UNUSED run_pg_ctl(const char *arg);
static void run_basebackup(const char *remote_connstr, const char *data_dir);
static void wait_postmaster_connection(const char *connstr);
static void wait_for_end_recovery(const char *connstr);
static void wait_postmaster_shutdown(void);

static char *validate_replication_set_input(char *replication_sets);

static void initialize_node_entry(PGconn *conn, NodeInfo *ni, char *node_name,
								  Oid dboid, char *remote_connstr, char *local_connstr);
static void remove_unwanted_files(void);
static void remove_unwanted_data(PGconn *conn);
static void reset_bdr_sequence_cache(PGconn *conn);
static void initialize_replication_identifier(PGconn *conn, NodeInfo *ni, Oid dboid, char *remote_lsn);
static char *create_restore_point(PGconn *conn, char *restore_point_name);
static void initialize_replication_slot(PGconn *conn, NodeInfo *ni, Oid dboid);
static void bdr_node_start(PGconn *conn, char *node_name, char *remote_connstr,
						   char *local_connstr, char *replication_sets);

static RemoteInfo *get_remote_info(char* connstr);

static void initialize_data_dir(char *data_dir, char *connstr,
					char *postgresql_conf, char *pg_hba_conf);
static bool check_data_dir(char *data_dir, RemoteInfo *remoteinfo);

static uint64 GenerateSystemIdentifier(void);
static uint64 read_sysid(const char *data_dir);
static void set_sysid(uint64 sysid);

static void WriteRecoveryConf(PQExpBuffer contents);
static void CopyConfFile(char *fromfile, char *tofile);

char *get_connstr(char *connstr, char *dbname, char *dbhost, char *dbport, char *dbuser);
static char *PQconninfoParamsToConnstr(const char *const * keywords, const char *const * values);
static void appendPQExpBufferConnstrValue(PQExpBuffer buf, const char *str);

static bool file_exists(const char *path);
static bool is_pg_dir(const char *path);
static void copy_file(char *fromfile, char *tofile);
static char *find_other_exec_or_die(const char *argv0, const char *target, const char *versionstr);
static bool postmaster_is_alive(pid_t pid);
static long get_pgpid(void);

static PGconn *
connectdb(char *connstr)
{
	PGconn *conn;

	conn = PQconnectdb(connstr);
	if (PQstatus(conn) != CONNECTION_OK)
		die(_("Connection to database failed: %s, connection string was: %s\n"), PQerrorMessage(conn), connstr);

	return conn;
}

void signal_handler(int sig)
{
	if (sig == SIGINT)
	{
		die(_("\nCanceling...\n"));
	}
}


int
main(int argc, char **argv)
{
	int	i;
	int	c;
	PQExpBuffer recoveryconfcontents = createPQExpBuffer();
	RemoteInfo *remote_info;
	NodeInfo	node_info;
	char		restore_point_name[NAMEDATALEN];
	char	   *remote_lsn;
	bool		stop = false;
	int			optindex;
	char	   *node_name = NULL;
	char	   *local_connstr = NULL;
	char	   *local_dbhost = NULL,
			   *local_dbport = NULL,
			   *local_dbuser = NULL;
	char	   *remote_connstr = NULL;
	char	   *remote_dbhost = NULL,
			   *remote_dbport = NULL,
			   *remote_dbuser = NULL;
	char	   *postgresql_conf = NULL,
			   *pg_hba_conf = NULL,
			   *recovery_conf = NULL;
	char	   *replication_sets = NULL;
	bool		use_existing_data_dir;
	int			pg_ctl_ret,
				logfd;

	static struct option long_options[] = {
		{"node-name", required_argument, NULL, 'n'},
		{"pgdata", required_argument, NULL, 'D'},
		{"remote-dbname", required_argument, NULL, 'd'},
		{"remote-host", required_argument, NULL, 'h'},
		{"remote-port", required_argument, NULL, 'p'},
		{"remote-user", required_argument, NULL, 'U'},
		{"local-dbname", required_argument, NULL, 2},
		{"local-host", required_argument, NULL, 3},
		{"local-port", required_argument, NULL, 4},
		{"local-user", required_argument, NULL, 5},
		{"postgresql-conf", required_argument, NULL, 6},
		{"hba-conf", required_argument, NULL, 7},
		{"recovery-conf", required_argument, NULL, 8},
		{"stop", no_argument, NULL, 's'},
		{"replication-sets", required_argument, NULL, 9},
		{NULL, 0, NULL, 0}
	};

	argv0 = argv[0];
	progname = get_progname(argv[0]);
	start_time = time(NULL);
	signal(SIGINT, signal_handler);

	/* check for --help */
	if (argc > 1)
	{
		for (i = 1; i < argc; i++)
		{
			if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-?") == 0)
			{
				usage();
				exit(0);
			}
		}
	}

	/* Option parsing and validation */
	while ((c = getopt_long(argc, argv, "D:d:h:n:p:sU:v", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'D':
				data_dir = pg_strdup(optarg);
				break;
			case 'd':
				remote_connstr = pg_strdup(optarg);
				break;
			case 'h':
				remote_dbhost = pg_strdup(optarg);
				break;
			case 'n':
				node_name = pg_strdup(optarg);
				break;
			case 'p':
				remote_dbport = pg_strdup(optarg);
				break;
			case 'U':
				remote_dbuser = pg_strdup(optarg);
				break;
			case 'v':
				verbosity++;
				break;
			case 2:
				local_connstr = pg_strdup(optarg);
				break;
			case 3:
				local_dbhost = pg_strdup(optarg);
				break;
			case 4:
				local_dbport = pg_strdup(optarg);
				break;
			case 5:
				local_dbuser = pg_strdup(optarg);
				break;
			case 6:
				{
					postgresql_conf = pg_strdup(optarg);
					if (postgresql_conf != NULL && !file_exists(postgresql_conf))
						die(_("The specified postgresql.conf file does not exist."));
					break;
				}
			case 7:
				{
					pg_hba_conf = pg_strdup(optarg);
					if (pg_hba_conf != NULL && !file_exists(pg_hba_conf))
						die(_("The specified pg_hba.conf file does not exist."));
					break;
				}
			case 8:
				{
					recovery_conf = pg_strdup(optarg);
					if (recovery_conf != NULL && !file_exists(recovery_conf))
						die(_("The specified recovery.conf file does not exist."));
					break;
				}
			case 9:
				replication_sets = validate_replication_set_input(optarg);
				break;
			case 's':
				stop = true;
				break;
			default:
				fprintf(stderr, _("Unknown option\n"));
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	/*
	 * Sanity checks
	 */

	if (data_dir == NULL)
	{
		fprintf(stderr, _("No data directory specified\n"));
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}
	else if (node_name == NULL)
	{
		fprintf(stderr, _("No node name specified\n"));
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	remote_connstr = get_connstr(remote_connstr, NULL, remote_dbhost,
								 remote_dbport, remote_dbuser);
	local_connstr = get_connstr(local_connstr, NULL, local_dbhost,
								local_dbport, local_dbuser);

	if (!remote_connstr || !strlen(remote_connstr))
		die(_("Remote connection must be specified.\n"));
	if (!local_connstr || !strlen(local_connstr))
		die(_("Local connection must be specified.\n"));

	logfd = open("bdr_init_copy_postgres.log", O_CREAT | O_RDWR | O_TRUNC,
				 S_IRUSR | S_IWUSR);
	if (logfd == -1)
	{
		die(_("Creating bdr_init_copy_postgres.log failed: %s"),
				strerror(errno));
	}
	/* Safe to close() unchecked, we didn't write */
	(void) close(logfd);

	print_msg(VERBOSITY_NORMAL, _("%s: starting ...\n"), progname);

	/* Read the remote server indetification. */
	print_msg(VERBOSITY_NORMAL,
			  _("Getting remote server identification ...\n"));
	remote_info = get_remote_info(remote_connstr);

	/* If there are no BDR enabled dbs, just bail. */
	if (remote_info->numdbs < 1)
		die(_("Remote node does not have any BDR enabled databases.\n"));

	/*
	 * Check if we either detected symmetric rep sets on the remote node
	 * or user provided replication sets on command line.
	 */
	if (remote_info->replication_sets == NULL && replication_sets == NULL)
		die(_("Replication sets parameter is required when adding node to cluster with asymetric replication sets.\n"));

	use_existing_data_dir = check_data_dir(data_dir, remote_info);

	if (use_existing_data_dir &&
		remote_info->sysid != read_sysid(data_dir))
		die(_("Local data directory is not basebackup of remote node.\n"));

	print_msg(VERBOSITY_NORMAL,
			  _("Detected %d BDR database(s) on remote server\n"),
			  remote_info->numdbs);

	/*
	 * Start the cloning process
	 */

	node_info.remote_sysid = remote_info->sysid;
	node_info.remote_tlid = remote_info->tlid;
	/*
	 * Once the physical replication reaches the restore point, it will
	 * bump the timeline by one.
	 */
	node_info.local_tlid = remote_info->tlid + 1;

	/* Generate new identifier for local node. */
	node_info.local_sysid = GenerateSystemIdentifier();
	print_msg(VERBOSITY_VERBOSE,
			  _("Generated new local system identifier: "UINT64_FORMAT"\n"),
			  node_info.local_sysid);

	print_msg(VERBOSITY_NORMAL,
			  _("Updating BDR configuration on the remote node:\n"));

	/* Initialize remote node. */
	for (i = 0; i < remote_info->numdbs; i++)
	{
		char *dbname = remote_info->dbnames[i];
		char *db_local_connstr = get_connstr(local_connstr, dbname,
											 NULL, NULL, NULL);
		char *db_remote_connstr = get_connstr(remote_connstr, dbname,
											  NULL, NULL, NULL);

		remote_conn = connectdb(db_remote_connstr);

		/*
		 * Create replication slots on remote node.
		 */
		print_msg(VERBOSITY_NORMAL,
				  _(" %s: creating replication slot ...\n"), dbname);
		initialize_replication_slot(remote_conn, &node_info,
									remote_info->dboids[i]);

		/*
		 * Create node entry for future local node.
		 */
		print_msg(VERBOSITY_NORMAL,
				  _(" %s: creating node entry for local node ...\n"), dbname);
		initialize_node_entry(remote_conn, &node_info, node_name,
							  remote_info->dboids[i],
							  db_remote_connstr, db_local_connstr);

		/* Don't hold connection since the next step might take long time. */
		PQfinish(remote_conn);
		remote_conn = NULL;
	}

	/*
	 * Create basebackup or use existing one
	 */
	initialize_data_dir(data_dir,
						use_existing_data_dir ? NULL : remote_connstr,
						postgresql_conf, pg_hba_conf);
	snprintf(pid_file, MAXPGPATH, "%s/postmaster.pid", data_dir);

	/*
	 * Create restore point to which we will catchup via physical replication.
	 */
	remote_conn = connectdb(remote_connstr);

	print_msg(VERBOSITY_NORMAL, _("Creating restore point on remote node ...\n"));

	snprintf(restore_point_name, NAMEDATALEN,
			 "bdr_"UINT64_FORMAT, node_info.local_sysid);
	remote_lsn = create_restore_point(remote_conn, restore_point_name);

	PQfinish(remote_conn);
	remote_conn = NULL;

	/*
	 * Get local db to consistent state (for lsn after slot creation).
	 */
	print_msg(VERBOSITY_NORMAL,
			  _("Bringing local node to the restore point ...\n"));
	if (recovery_conf)
	{
		CopyConfFile(recovery_conf, "recovery.conf");
	}
	else
	{
		appendPQExpBuffer(recoveryconfcontents, "standby_mode = 'on'\n");
		appendPQExpBuffer(recoveryconfcontents, "primary_conninfo = '%s'\n",
								escape_single_quotes_ascii(remote_connstr));
	}
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_name = '%s'\n", restore_point_name);
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_inclusive = true\n");
	if (PG_VERSION_NUM/100 == 904)
	{
		appendPQExpBuffer(recoveryconfcontents, "pause_at_recovery_target = off");
	}
	else if (PG_VERSION_NUM >= 90600)
	{
		appendPQExpBuffer(recoveryconfcontents, "recovery_target_action = promote");
	}
	else
	{
		die(_("Only 9.4bdr and 9.6 are supported"));
	}

	WriteRecoveryConf(recoveryconfcontents);

	/*
	 * Start local node with BDR disabled, and wait until it starts accepting
	 * connections which means it has caught up to the restore point.
	 *
	 * Note that pg_ctl won't return nonzero if postmaster starts then
	 * immediately exits due to issues like port conflicts. We'll detect that
	 * in wait_postmaster_connection().
	 */
	pg_ctl_ret = run_pg_ctl("start -l \"bdr_init_copy_postgres.log\" -o \"-c shared_preload_libraries=''\"");
	if (pg_ctl_ret != 0)
		die(_("postgres startup for restore point catchup failed with %d. See bdr_init_copy_postgres.log."), pg_ctl_ret);

	wait_postmaster_connection(local_connstr);

	/*
	 * The postmaster is in standby mode and has caught up. Now we have to
	 * promote it so we can perform read/write transactions and wait for
	 * it to notice that it has been promoted.
	 *
	 * When pg_is_in_recovery() no longer returns true, we're ready.
	 */
	wait_for_end_recovery(local_connstr);

	/*
	 * Clean any per-node data that were copied by pg_basebackup.
	 */
	for (i = 0; i < remote_info->numdbs; i++)
	{
		char *dbname = remote_info->dbnames[i];
		char *db_connstr = get_connstr(local_connstr, dbname,
									   NULL, NULL, NULL);

		local_conn = connectdb(db_connstr);

		remove_unwanted_data(local_conn);

		PQfinish(local_conn);
		local_conn = NULL;
	}

	/* Stop Postgres so we can reset system id and start it with BDR loaded. */
	pg_ctl_ret = run_pg_ctl("stop");
	if (pg_ctl_ret != 0)
		die(_("postgres stop after restore point catchup failed with %d. See bdr_init_copy_postgres.log."), pg_ctl_ret);
	wait_postmaster_shutdown();

	/*
	 * Individualize the local node by changing the system identifier.
	 *
	 * We can't rely on the timeline ID alone, even though it's incremented
	 * on promotion of the copy, because we can't make sure it's globally
	 * unique. If node A is copied to node B, then node A is copied to node C,
	 * both nodes B and C will have the same tlid.
	 *
	 * For 9.6 this means using a patched pg_resetxlog since the stock one
	 * doesn't know how to alter the sysid.
	 */
	set_sysid(node_info.local_sysid);

	/*
	 * Start the node again, now with BDR active so that we can join the node
	 * to the BDR cluster. This is final start, so don't log to to special log
	 * file anymore.
	 */
	print_msg(VERBOSITY_NORMAL,
			  _("Initializing BDR on the local node:\n"));

	pg_ctl_ret = run_pg_ctl("start -l \"bdr_init_copy_postgres.log\"");
	if (pg_ctl_ret != 0)
		die(_("postgres restart with bdr enabled failed with %d. See bdr_init_copy_postgres.log."), pg_ctl_ret);
	wait_postmaster_connection(local_connstr);

	for (i = 0; i < remote_info->numdbs; i++)
	{
		char *dbname = remote_info->dbnames[i];
		char *db_local_connstr = get_connstr(local_connstr, dbname,
											 NULL, NULL, NULL);
		char *db_remote_connstr = get_connstr(remote_connstr, dbname,
											  NULL, NULL, NULL);

		if (replication_sets == NULL)
			replication_sets = remote_info->replication_sets[i];

		local_conn = connectdb(db_local_connstr);

		/*
		 * Clean the sequence amdata cache which was copied from the remote
		 * server verbatim but isn't valid on the new node and would cause
		 * duplicate values being returned by the sequence on both servers.
		 */
		reset_bdr_sequence_cache(local_conn);

		/*
		 * Create the identifier which is setup with the position to which we
		 * already caught up using physical replication.
		 */
		print_msg(VERBOSITY_VERBOSE,
				  _(" %s: creating replication identifier ...\n"), dbname);
		initialize_replication_identifier(local_conn, &node_info,
										  remote_info->dboids[i], remote_lsn);

		/*
		 * And finally add the node to the cluster.
		 */
		print_msg(VERBOSITY_NORMAL,
				  _(" %s: adding the database to BDR cluster ...\n"), dbname);
		print_msg(VERBOSITY_VERBOSE,
				  _(" %s: replication sets: %s"), dbname, replication_sets);
		bdr_node_start(local_conn, node_name, db_remote_connstr,
					   db_local_connstr, replication_sets);

		PQfinish(local_conn);
		local_conn = NULL;
	}

	/* If user does not want the node to be running at the end, stop it. */
	if (stop)
	{
		print_msg(VERBOSITY_NORMAL, _("Stopping the local node ...\n"));
		pg_ctl_ret = run_pg_ctl("stop");
		if (pg_ctl_ret != 0)
			die(_("Stopping postgres after successful join failed with %d. See bdr_init_copy_postgres.log."), pg_ctl_ret);
		wait_postmaster_shutdown();
	}

	print_msg(VERBOSITY_NORMAL, _("All done\n"));

	return 0;
}


/*
 * Print help.
 */
static void
usage(void)
{
	printf(_("%s initializes new BDR node from existing BDR instance.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nGeneral options:\n"));
	printf(_("  -D, --pgdata=DIRECTORY  data directory to be used for new node,\n"));
	printf(_("                          can be either empty/non-existing directory,\n"));
	printf(_("                          or directory populated using pg_basebackup -X stream\n"));
	printf(_("                          command\n"));
	printf(_("  -n, --node-name=NAME    name of the newly created node\n"));
	printf(_("  --replication-sets=SETS comma separated list of replication set names to use\n"));
	printf(_("  -s, --stop              stop the server once the initialization is done\n"));
	printf(_("  -v                      increase logging verbosity\n"));
	printf(_("\nConfiguration files override:\n"));
	printf(_("  --hba-conf              path to the new pg_hba.conf\n"));
	printf(_("  --postgresql-conf       path to the new postgresql.conf\n"));
	printf(_("  --recovery-conf         path to the template recovery.conf\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -d, --remote-dbname=CONNSTR\n"));
	printf(_("                          dbname or connection string for remote node\n"));
	printf(_("  -h, --remote-host=HOSTNAME\n"));
	printf(_("                          server host or socket directory for remote node\n"));
	printf(_("  -p, --remote-port=PORT  server port number for remote node\n"));
	printf(_("  -U, --remote-user=NAME  connect as specified database user to the remote node\n"));
	printf(_("  --local-dbname=CONNSTR  dbname or connection string for local node\n"));
	printf(_("  --local-host=HOSTNAME   server host or socket directory for local node\n"));
	printf(_("  --local-port=PORT       server port number for local node. Must match\n"));
	printf(_("                          postgresql.conf, does not set port server is"));
	printf(_("                          started with."));
	printf(_("  --local-user=NAME       connect as specified database user to the local node\n"));
}

/*
 * Print error and exit.
 */
static void
die(const char *fmt,...)
{
	va_list argptr;
	va_start(argptr, fmt);
	vfprintf(stderr, fmt, argptr);
	va_end(argptr);

	if (local_conn)
		PQfinish(local_conn);
	if (remote_conn)
		PQfinish(remote_conn);

	if (get_pgpid())
	{
		if (!run_pg_ctl("stop -s"))
		{
			fprintf(stderr, _("WARNING: postgres seems to be running, but could not be stopped"));
		}
	}

	exit(1);
}

/*
 * Print message to stdout and flush
 */
static void
print_msg(VerbosityLevelEnum level, const char *fmt,...)
{
	if (verbosity >= level)
	{
		va_list argptr;
		va_start(argptr, fmt);
		vfprintf(stdout, fmt, argptr);
		va_end(argptr);
		fflush(stdout);
	}
}


/*
 * Start pg_ctl with given argument(s) - used to start/stop postgres
 *
 * Returns the exit code reported by pg_ctl. If pg_ctl exits due to a
 * signal this call will die and not return.
 */
static int
run_pg_ctl(const char *arg)
{
	int			 ret;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path = find_other_exec_or_die(argv0, "pg_ctl", NULL);

	appendPQExpBuffer(cmd, "%s %s -D \"%s\" -s", exec_path, arg, data_dir);

	/* Run pg_ctl in silent mode unless we run in debug mode. */
	if (verbosity < VERBOSITY_DEBUG)
		appendPQExpBuffer(cmd, " -s");

	print_msg(VERBOSITY_DEBUG, _("Running pg_ctl: %s.\n"), cmd->data);
	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	if (WIFEXITED(ret))
		return WEXITSTATUS(ret);
	else if (WIFSIGNALED(ret))
		die(_("pg_ctl exited with signal %d"), WTERMSIG(ret));
	else
		die(_("pg_ctl exited for an unknown reason (system() returned %d)"), ret);
}


/*
 * Run pg_basebackup to create the copy of the origin node.
 */
static void
run_basebackup(const char *remote_connstr, const char *data_dir)
{
	int			 ret;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path = find_other_exec_or_die(argv0, "pg_basebackup", NULL);

	appendPQExpBuffer(cmd, "%s -D \"%s\" -d \"%s\" -X s -P", exec_path, data_dir, remote_connstr);

	/* Run pg_basebackup in verbose mode if we are running in verbose mode. */
	if (verbosity >= VERBOSITY_VERBOSE)
		appendPQExpBuffer(cmd, " -v");

	print_msg(VERBOSITY_DEBUG, _("Running pg_basebackup: %s.\n"), cmd->data);
	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	if (WIFEXITED(ret) && WEXITSTATUS(ret) == 0)
		return;
	if (WIFEXITED(ret))
		die(_("pg_basebackup failed with exit status %d, cannot continue.\n"), WEXITSTATUS(ret));
	else if (WIFSIGNALED(ret))
		die(_("pg_basebackup exited with signal %d, cannot continue"), WTERMSIG(ret));
	else
		die(_("pg_basebackup exited for an unknown reason (system() returned %d)"), ret);
}

/*
 * Set system identifier to system id we used for registering the slots.
 */
static void
set_sysid(uint64 sysid)
{
	int			 ret;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path, *cmdname;

	if (PG_VERSION_NUM/100 == 904)
	{
		exec_path = find_other_exec_or_die(argv0, "pg_resetxlog", "pg_resetxlog (PostgreSQL) " PG_VERSION "\n");
		cmdname = "pg_resetxlog";
	}
	else
	{
		exec_path = find_other_exec_or_die(argv0, "bdr_resetxlog", "bdr_resetxlog (PostgreSQL) " PG_VERSION "\n");
		cmdname = "bdr_resetxlog";
	}

	appendPQExpBuffer(cmd, "%s \"-s "UINT64_FORMAT"\" \"%s\"", exec_path, sysid, data_dir);

	print_msg(VERBOSITY_DEBUG, _("Running %s: %s.\n"), cmdname, cmd->data);
	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	if (WIFEXITED(ret) && WEXITSTATUS(ret) == 0)
		return;
	if (WIFEXITED(ret))
		die(_("%s failed with exit status %d, cannot continue.\n"), cmdname, WEXITSTATUS(ret));
	else if (WIFSIGNALED(ret))
		die(_("%s exited with signal %d, cannot continue"), cmdname, WTERMSIG(ret));
	else
		die(_("%s exited for an unknown reason (system() returned %d)"), cmdname, ret);
}

/*
 * Cleans everything that was replicated via basebackup but we don't want it.
 */
static void
remove_unwanted_files(void)
{
	/*
	 * 9.4's pg_basebackup copies pg_logical/checkpoints; 9.6 does
	 * not since there's no such thing on 9.6.
	 */
	if (PG_VERSION_NUM/100 == 904)
	{
		DIR				*lldir;
		struct dirent	*llde;
		PQExpBuffer		 llpath = createPQExpBuffer();
		PQExpBuffer		 filename = createPQExpBuffer();

		printfPQExpBuffer(llpath, "%s/%s", data_dir, LLOGCDIR);

		print_msg(VERBOSITY_DEBUG, _("Removing data from \"%s\" directory.\n"),
				  llpath->data);

		/*
		 * Remove stray logical replication checkpoints
		 */
		lldir = opendir(llpath->data);
		if (lldir == NULL)
		{
			die(_("Could not open directory \"%s\": %s\n"),
				llpath->data, strerror(errno));
		}

		while (errno = 0, (llde = readdir(lldir)) != NULL)
		{
			size_t len = strlen(llde->d_name);
			if (len > 5 && !strcmp(llde->d_name + len - 5, ".ckpt"))
			{
				printfPQExpBuffer(filename, "%s/%s", llpath->data, llde->d_name);

				if (unlink(filename->data) != 0)
				{
					die(_("Could not unlink checkpoint file \"%s\": %s\n"),
						filename->data, strerror(errno));
				}
			}
		}

		destroyPQExpBuffer(llpath);
		destroyPQExpBuffer(filename);

		if (errno)
		{
			die(_("Could not read directory \"%s\": %s\n"),
				LLOGCDIR, strerror(errno));
		}

		if (closedir(lldir))
		{
			die(_("Could not close directory \"%s\": %s\n"),
				LLOGCDIR, strerror(errno));
		}
	}
}

/*
 * Init the datadir
 *
 * This function can either ensure provided datadir is a postgres datadir,
 * or create it using pg_basebackup.
 *
 * In any case, new postresql.conf and pg_hba.conf will be copied to the
 * datadir if they are provided.
 */
static void
initialize_data_dir(char *data_dir, char *connstr,
					char *postgresql_conf, char *pg_hba_conf)
{
	if (connstr)
	{
		print_msg(VERBOSITY_NORMAL,
				  _("Creating base backup of the remote node...\n"));
		run_basebackup(connstr, data_dir);
	}

	remove_unwanted_files();

	if (postgresql_conf)
		CopyConfFile(postgresql_conf, "postgresql.conf");
	if (pg_hba_conf)
		CopyConfFile(pg_hba_conf, "pg_hba.conf");
}

/*
 * This function checks if provided datadir is clone of the remote node
 * described by the remote info, or if it's emtpy directory that can be used
 * as new datadir.
 */
static bool
check_data_dir(char *data_dir, RemoteInfo *remoteinfo)
{
	/* Run basebackup as needed. */
	switch (pg_check_dir(data_dir))
	{
		case 0:		/* Does not exist */
		case 1:		/* Exists, empty */
				return false;
		case 2:
		case 3:		/* Exists, not empty */
		case 4:
			{
				if (!is_pg_dir(data_dir))
					die(_("Directory \"%s\" exists but is not valid postgres data directory.\n"),
						data_dir);
				return true;
			}
		case -1:	/* Access problem */
			die(_("Could not access directory \"%s\": %s.\n"),
				data_dir, strerror(errno));
	}

	/* Unreachable */
	die(_("Unexpected result from pg_check_dir() call"));
	return false;
}

/*
 * Initialize replication slots
 *
 * Get connection configs from bdr and use the info
 * to register replication slots for future use.
 */
static void
initialize_replication_slot(PGconn *conn, NodeInfo *ni, Oid dboid)
{
	NameData	slotname;
	PQExpBuffer	query = createPQExpBuffer();
	PGresult   *res;
	BDRNodeId	node;

	/* dboids are the same, because we just cloned... */
	node.sysid = ni->local_sysid;
	node.timeline = ni->local_tlid;
	node.dboid = dboid;
	bdr_slot_name(&slotname, &node, dboid);
	appendPQExpBuffer(query, "SELECT pg_create_logical_replication_slot(%s, '%s');",
					  PQescapeLiteral(conn, NameStr(slotname), NAMEDATALEN), "bdr");

	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create replication slot, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}

/*
 * Read replication info about remote connection
 */
static RemoteInfo *
get_remote_info(char* remote_connstr)
{
	RemoteInfo *ri = (RemoteInfo *)pg_malloc0(sizeof(RemoteInfo));
	char	   *remote_sysid;
	char	   *remote_tlid;
	int			i;
	PGresult   *res;
	PQExpBuffer	conninfo = createPQExpBuffer();

	/*
	 * Fetch the system identification info (sysid, tlid) via replication
	 * connection - there is no way to get this info via SQL.
	 */
	printfPQExpBuffer(conninfo, "%s replication=database", remote_connstr);
	remote_conn = PQconnectdb(conninfo->data);
	destroyPQExpBuffer(conninfo);

	if (PQstatus(remote_conn) != CONNECTION_OK)
	{
		die(_("Could not connect to the remote server: %s\n"),
					PQerrorMessage(remote_conn));
	}

	ri->version = PQserverVersion(remote_conn);
	if (ri->version == 0)
		die(_("Could not determine remote server's PostgreSQL version\n"));

	if (ri->version/100 != PG_VERSION_NUM/100)
	{
		die(_("Target server is version %s but we are version %s. bdr_init_copy can only be used when the join target is the same major version. See bdr.bdr_group_join()."),
			PQparameterStatus(remote_conn, "server_version"), PG_VERSION);
	}

	res = PQexec(remote_conn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		die(_("Could not send replication command \"%s\": %s\n"),
			 "IDENTIFY_SYSTEM", PQerrorMessage(remote_conn));
	}

	if (PQntuples(res) != 1 || PQnfields(res) < 4 || PQnfields(res) > 5)
	{
		PQclear(res);
		die(_("Could not identify system: got %d rows and %d fields, expected %d rows and %d or %d fields\n"),
			 PQntuples(res), PQnfields(res), 1, 4, 5);
	}

	remote_sysid = PQgetvalue(res, 0, 0);
	remote_tlid = PQgetvalue(res, 0, 1);

#ifdef HAVE_STRTOULL
	ri->sysid = strtoull(remote_sysid, NULL, 10);
#else
	ri->sysid = strtoul(remote_sysid, NULL, 10);
#endif

	if (sscanf(remote_tlid, "%u", &ri->tlid) != 1)
		die(_("Could not parse remote tlid %s\n"), remote_tlid);

	PQclear(res);
	PQfinish(remote_conn);
	remote_conn = NULL;

	/*
	 * Fetch list of BDR enabled databases via standard SQL connection.
	 */
	remote_conn = PQconnectdb(remote_connstr);
	if (PQstatus(remote_conn) != CONNECTION_OK)
	{
		die(_("Could not connect to the remote server: %s"), PQerrorMessage(remote_conn));
	}

	res = PQexec(remote_conn, "SELECT d.oid, d.datname "
				 "FROM pg_catalog.pg_database d, pg_catalog.pg_shseclabel l "
				 "WHERE l.provider = 'bdr' "
				 "  AND l.classoid = 'pg_database'::regclass "
				 "  AND d.oid = l.objoid;");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die(_("Could fetch remote database list: %s"), PQerrorMessage(remote_conn));

	ri->numdbs = PQntuples(res);

	ri->dboids = (Oid *) pg_malloc(ri->numdbs * sizeof(Oid));
	ri->dbnames = (char **) pg_malloc(ri->numdbs * sizeof(char *));

	for (i = 0; i < ri->numdbs; i++)
	{
		char   *remote_dboid = PQgetvalue(res, i, 0);
		char   *remote_dbname = PQgetvalue(res, i, 1);
		Oid		remote_dboid_i;

		if (sscanf(remote_dboid, "%u", &remote_dboid_i) != 1)
			die(_("Could not parse database OID %s"), remote_dboid);

		ri->dboids[i] = remote_dboid_i;
		ri->dbnames[i] = pstrdup(remote_dbname);
	}

	PQclear(res);

	PQfinish(remote_conn);
	remote_conn = NULL;

	/* Check/get replication sets. */
	ri->replication_sets = (char **) pg_malloc(ri->numdbs * sizeof(char *));

	for (i = 0; i < ri->numdbs; i++)
	{
		char *dbname = ri->dbnames[i];
		char *db_connstr = get_connstr(remote_connstr, dbname,
									   NULL, NULL, NULL);

		remote_conn = connectdb(db_connstr);

		res = PQexec(remote_conn, "SELECT array_to_string(conn_replication_sets, ',')\n"
					 "FROM bdr.bdr_connections c, bdr.bdr_nodes n\n"
					 "WHERE c.conn_sysid = n.node_sysid AND\n"
					 "      c.conn_timeline = n.node_timeline AND\n"
					 "      c.conn_dboid = n.node_dboid AND\n"
					 "      c.conn_is_unidirectional = false AND\n"
					 "      n.node_status = "BDR_NODE_STATUS_READY_S"\n"
					 "GROUP BY conn_replication_sets");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			die(_("Could fetch replication set info from database %s: %s"),
				dbname, PQerrorMessage(remote_conn));

		/* No nodes found? */
		if (PQntuples(res) == 0)
			die(_("The remote node is not configured as a BDR node.\n"));

		/*
		 * Node has different replication sets on different nodes,
		 * we can't autodetect replication sets for new node.
		 */
		if (PQntuples(res) > 1)
		{
			/* XXX: free individual items as well */
			pg_free(ri->replication_sets);
			ri->replication_sets = NULL;
			PQclear(res);
			PQfinish(remote_conn);
			remote_conn = NULL;
			break;
		}

		ri->replication_sets[i] = pstrdup(PQgetvalue(res, 0, 0));

		PQclear(res);
		PQfinish(remote_conn);
		remote_conn = NULL;
	}

	return ri;
}


/*
 * Check if extension exists.
 */
static bool
extension_exists(PGconn *conn, const char *extname)
{
	PQExpBuffer		query = createPQExpBuffer();
	PGresult	   *res;
	bool			ret;

	printfPQExpBuffer(query, "SELECT 1 FROM pg_catalog.pg_extension WHERE extname = %s;",
					  PQescapeLiteral(conn, extname, strlen(extname)));
	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		die(_("Could not read extension info: %s\n"), PQerrorMessage(conn));
	}

	ret = PQntuples(res) == 1;

	PQclear(res);
	destroyPQExpBuffer(query);

	return ret;
}

/*
 * Create extension.
 */
static void
install_extension(PGconn *conn, const char *extname)
{
	PQExpBuffer		query = createPQExpBuffer();
	PGresult	   *res;

	printfPQExpBuffer(query, "CREATE EXTENSION %s;",
					  PQescapeIdentifier(conn, extname, strlen(extname)));
	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		die(_("Could not install %s extension: %s\n"), extname, PQerrorMessage(conn));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}

/*
 * Validates input of the replication sets and returns normalized data.
 *
 * The rules enforced here should be same as the ones in
 * bdr_validate_replication_set_name.
 */
static char *
validate_replication_set_input(char *replication_sets)
{
	char	   *name;
	PQExpBuffer	retbuf = createPQExpBuffer();
	char	   *ret;
	bool		first = true;

	if (!replication_sets)
		return NULL;

	name = strtok(replication_sets, " ,");
	while (name != NULL)
	{
		const char *cp;

		if (strlen(name) == 0)
			die(_("replication set name \"%s\" is too short\n"), name);

		if (strlen(name) > NAMEDATALEN)
			die(_("replication set name \"%s\" is too long\n"), name);

		for (cp = name; *cp; cp++)
		{
			if (!((*cp >= 'a' && *cp <= 'z')
				  || (*cp >= '0' && *cp <= '9')
				  || (*cp == '_')
				  || (*cp == '-')))
			{
				die(_("replication set name \"%s\" contains invalid character\n"),
					name);
			}
		}

		if (first)
			first = false;
		else
			appendPQExpBufferStr(retbuf, ", ");
		appendPQExpBufferStr(retbuf, name);

		name = strtok(NULL, " ,");
	}

	ret = pg_strdup(retbuf->data);
	destroyPQExpBuffer(retbuf);

	return ret;
}

/*
 * Insert node entry for local node to the remote's bdr_nodes.
 */
void
initialize_node_entry(PGconn *conn, NodeInfo *ni, char* node_name, Oid dboid,
					  char *remote_connstr, char *local_connstr)
{
	PQExpBuffer		query = createPQExpBuffer();
	PGresult	   *res;

	res = PQexec(conn,
		"DO LANGUAGE plpgsql $$\n"
		"BEGIN\n"
		"	IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'acquire_global_lock' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'bdr')) THEN\n"
		"		PERFORM bdr.acquire_global_lock('ddl');\n"
		"	END IF;\n"
		"END; $$;\n");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		die(_("Failed to acquire global DDL lock before inserting row into bdr.bdr_nodes: %s\n"), PQerrorMessage(conn));
	}

	PQclear(res);

	printfPQExpBuffer(query, "INSERT INTO bdr.bdr_nodes"
							 " (node_status, node_sysid, node_timeline,"
							 "	node_dboid, node_name, node_init_from_dsn,"
							 "  node_local_dsn)"
							 " VALUES ("BDR_NODE_STATUS_CATCHUP_S", '"UINT64_FORMAT"', %u, %u, %s, %s, %s);",
					  ni->local_sysid, ni->local_tlid, dboid,
					  PQescapeLiteral(conn, node_name, strlen(node_name)),
					  PQescapeLiteral(conn, remote_connstr, strlen(remote_connstr)),
					  PQescapeLiteral(conn, local_connstr, strlen(local_connstr)));
	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		die(_("Failed to insert row into bdr.bdr_nodes: %s\n"), PQerrorMessage(conn));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}

/*
 * Clean all the data that was copied from remote node but we don't
 * want it here (currently shared security labels and replication identifiers).
 */
static void
remove_unwanted_data(PGconn *conn)
{
	PGresult	   *res;
	const char	   *dropident_sql;

	/* Remove any BDR security labels. */
	res = PQexec(conn, "DELETE FROM pg_catalog.pg_shseclabel WHERE provider = 'bdr';");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		die(_("Could not update security label: %s\n"), PQerrorMessage(conn));
	}
	PQclear(res);

	if (PG_VERSION_NUM/100 == 904)
		dropident_sql = "SELECT pg_catalog.pg_replication_identifier_drop(riname) FROM pg_catalog.pg_replication_identifier;";
	else
		dropident_sql = "SELECT pg_catalog.pg_replication_origin_drop(roname) FROM pg_catalog.pg_replication_origin;";

	/* Remove replication identifiers. */
	res = PQexec(conn, dropident_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		die(_("Could not remove existing replication origins: %s\n"), PQerrorMessage(conn));
	}
	PQclear(res);
}

/*
 * Cleans up sequence cache, has to be run when BDR so it can't be in the
 * remove_unwanted_data function.
 */
static void
reset_bdr_sequence_cache(PGconn *conn)
{
	PGresult	   *res;

	if (PG_VERSION_NUM/100 == 904)
	{
		/* Cleanup sequence cache */
		res = PQexec(conn,
					 "SELECT\n"
					 "    bdr.bdr_internal_sequence_reset_cache(pg_class.oid)\n"
					 "FROM pg_class\n"
					 "    JOIN pg_seqam ON (pg_seqam.oid = pg_class.relam)\n"
					 "    JOIN pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)\n"
					 "WHERE\n"
					 "    relkind = 'S'\n"
					 "    AND seqamname = 'bdr'\n");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			die(_("Could not clean sequence cache: %s\n"), PQerrorMessage(conn));
		}
		PQclear(res);
	}
}

/*
 * Initialize new remote identifier to specific position.
 */
static void
initialize_replication_identifier(PGconn *conn, NodeInfo *ni, Oid dboid, char *remote_lsn)
{
	PGresult   *res;
	char		remote_ident[256];
	PQExpBuffer query = createPQExpBuffer();
	const char *origin_or_identifier;

	snprintf(remote_ident, sizeof(remote_ident), BDR_REPORIGIN_ID_FORMAT,
				ni->remote_sysid, ni->remote_tlid, dboid, dboid, "");

	if (PG_VERSION_NUM/100 == 904)
		origin_or_identifier = "identifier";
	else
		origin_or_identifier = "origin";

	printfPQExpBuffer(query, "SELECT pg_catalog.pg_replication_%s_create('%s')",
					 origin_or_identifier, remote_ident);

	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create replication %s \"%s\": status %s: %s\n"),
			 origin_or_identifier, query->data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	PQclear(res);

	if (remote_lsn)
	{
		/*
		 * This mess is to handle renaming of pg_replication_identifier_advance
		 * to pg_replication_origin_advance and removal of the local_lsn param
		 * in 9.6.
 		 */
		printfPQExpBuffer(query, "SELECT pg_catalog.pg_replication_%s_advance('%s', '%s'%s)",
						 origin_or_identifier, remote_ident, remote_lsn,
						 (PG_VERSION_NUM/100 == 904 ? ", '0/0'" : ""));

		res = PQexec(conn, query->data);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			die(_("Could not advance replication %s \"%s\": status %s: %s\n"),
				 origin_or_identifier, query->data,
				 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}
		PQclear(res);
	}

	destroyPQExpBuffer(query);
}


/*
 * Create remote restore point which will be used to get into synchronized
 * state through physical replay.
 */
static char *
create_restore_point(PGconn *conn, char *restore_point_name)
{
	PQExpBuffer  query = createPQExpBuffer();
	PGresult	*res;
	char		*remote_lsn = NULL;

	printfPQExpBuffer(query, "SELECT pg_create_restore_point('%s')", restore_point_name);
	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create restore point, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	remote_lsn = pstrdup(PQgetvalue(res, 0, 0));

	PQclear(res);
	destroyPQExpBuffer(query);

	return remote_lsn;
}


static void
bdr_node_start(PGconn *conn, char *node_name, char *remote_connstr,
			   char *local_connstr, char *replication_sets)
{
	PQExpBuffer  query = createPQExpBuffer();
	PQExpBuffer  repsets = createPQExpBuffer();
	PGresult	*res;

	/* Install required extensions if needed. */
	if (!extension_exists(conn, "btree_gist"))
		install_extension(conn, "btree_gist");
	if (!extension_exists(conn, "bdr"))
		install_extension(conn, "bdr");

	/*
	 * replication_sets is comma separated list of strings so all we need to do
	 * is put the brackets around it to make it valid input for pg array
	 */
	printfPQExpBuffer(repsets, "{%s}", replication_sets);

	/* Add the node to the cluster. */
	printfPQExpBuffer(query, "SELECT bdr.bdr_group_join(%s, %s, %s, replication_sets := %s);",
					  PQescapeLiteral(conn, node_name, strlen(node_name)),
					  PQescapeLiteral(conn, local_connstr, strlen(local_connstr)),
					  PQescapeLiteral(conn, remote_connstr, strlen(remote_connstr)),
					  PQescapeLiteral(conn, repsets->data, repsets->len));

	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not add local node to cluster, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	PQclear(res);
	destroyPQExpBuffer(repsets);
	destroyPQExpBuffer(query);
}

/*
 * Build connection string from individual parameter.
 *
 * dbname can be specified in connstr parameter
 */
char *
get_connstr(char *connstr, char *dbname, char *dbhost, char *dbport, char *dbuser)
{
	char		*ret;
	int			argcount = 4;	/* dbname, host, user, port */
	int			i;
	const char **keywords;
	const char **values;
	PQconninfoOption *conn_opts = NULL;
	PQconninfoOption *conn_opt;
	char	   *err_msg = NULL;

	/*
	 * Merge the connection info inputs given in form of connection string
	 * and options
	 */
	i = 0;
	if (connstr &&
		(strncmp(connstr, "postgresql://", 13) == 0 ||
		 strncmp(connstr, "postgres://", 11) == 0 ||
		 strchr(connstr, '=') != NULL))
	{
		conn_opts = PQconninfoParse(connstr, &err_msg);
		if (conn_opts == NULL)
		{
			die(_("Invalid connection string: %s\n"), err_msg);
		}

		for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
		{
			if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
				argcount++;
		}

		keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
		values = pg_malloc0((argcount + 1) * sizeof(*values));

		for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
		{
			/* If db* parameters were provided, we'll fill them later. */
			if (dbname && strcmp(conn_opt->keyword, "dbname") == 0)
				continue;
			if (dbhost && strcmp(conn_opt->keyword, "host") == 0)
				continue;
			if (dbuser && strcmp(conn_opt->keyword, "user") == 0)
				continue;
			if (dbport && strcmp(conn_opt->keyword, "port") == 0)
				continue;

			if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
			{
				keywords[i] = conn_opt->keyword;
				values[i] = conn_opt->val;
				i++;
			}
		}
	}
	else
	{
		keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
		values = pg_malloc0((argcount + 1) * sizeof(*values));

		/*
		 * If connstr was provided but it's not in connection string format and
		 * the dbname wasn't provided then connstr is actually dbname.
		 */
		if (connstr && !dbname)
			dbname = connstr;
	}

	if (dbname)
	{
		keywords[i] = "dbname";
		values[i] = dbname;
		i++;
	}

	if (dbhost)
	{
		keywords[i] = "host";
		values[i] = dbhost;
		i++;
	}
	if (dbuser)
	{
		keywords[i] = "user";
		values[i] = dbuser;
		i++;
	}
	if (dbport)
	{
		keywords[i] = "port";
		values[i] = dbport;
		i++;
	}

	ret = PQconninfoParamsToConnstr(keywords, values);

	/* Connection ok! */
	pg_free(values);
	pg_free(keywords);
	if (conn_opts)
		PQconninfoFree(conn_opts);

	return ret;
}


/*
 * Create a new unique installation identifier.
 *
 * See notes in xlog.c about the algorithm.
 *
 * XXX: how to reuse the code between xlog.c, pg_resetxlog.c and this file
 */
static uint64
GenerateSystemIdentifier(void)
{
	uint64			sysidentifier;
	struct timeval	tv;

	gettimeofday(&tv, NULL);
	sysidentifier = ((uint64) tv.tv_sec) << 32;
	sysidentifier |= ((uint64) tv.tv_usec) << 12;
	sysidentifier |= getpid() & 0xFFF;

	return sysidentifier;
}

/*
 * Reads the pg_control file of the existing data dir.
 */
static uint64
read_sysid(const char *data_dir)
{
	ControlFileData ControlFile;
	int			fd;
	char		ControlFilePath[MAXPGPATH];

	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", data_dir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
		die(_("%s: could not open file \"%s\" for reading: %s\n"),
			progname, ControlFilePath, strerror(errno));

	if (read(fd, &ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
		die(_("%s: could not read file \"%s\": %s\n"),
			progname, ControlFilePath, strerror(errno));

	close(fd);

	return ControlFile.system_identifier;
}

/*
 * Write contents of recovery.conf
 */
static void
WriteRecoveryConf(PQExpBuffer contents)
{
	char		filename[MAXPGPATH];
	FILE	   *cf;

	sprintf(filename, "%s/recovery.conf", data_dir);

	cf = fopen(filename, "w");
	if (cf == NULL)
	{
		die(_("%s: could not create file \"%s\": %s\n"), progname, filename, strerror(errno));
	}

	if (fwrite(contents->data, contents->len, 1, cf) != 1)
	{
		die(_("%s: could not write to file \"%s\": %s\n"),
				progname, filename, strerror(errno));
	}

	fclose(cf);
}

/*
 * Copy file to data
 */
static void
CopyConfFile(char *fromfile, char *tofile)
{
	char		filename[MAXPGPATH];

	sprintf(filename, "%s/%s", data_dir, tofile);

	print_msg(VERBOSITY_DEBUG, _("Copying \"%s\" to \"%s\".\n"),
			  fromfile, filename);
	copy_file(fromfile, filename);
}


/*
 * Convert PQconninfoOption array into conninfo string
 */
static char *
PQconninfoParamsToConnstr(const char *const * keywords, const char *const * values)
{
	PQExpBuffer	 retbuf = createPQExpBuffer();
	char		*ret;
	int			 i = 0;

	for (i = 0; keywords[i] != NULL; i++)
	{
		if (i > 0)
			appendPQExpBufferChar(retbuf, ' ');
		appendPQExpBuffer(retbuf, "%s=", keywords[i]);
		appendPQExpBufferConnstrValue(retbuf, values[i]);
	}

	ret = pg_strdup(retbuf->data);
	destroyPQExpBuffer(retbuf);

	return ret;
}

/*
 * Escape connection info value
 */
static void
appendPQExpBufferConnstrValue(PQExpBuffer buf, const char *str)
{
	const char *s;
	bool		needquotes;

	/*
	 * If the string consists entirely of plain ASCII characters, no need to
	 * quote it. This is quite conservative, but better safe than sorry.
	 */
	needquotes = false;
	for (s = str; *s; s++)
	{
		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			needquotes = true;
			break;
		}
	}

	if (needquotes)
	{
		appendPQExpBufferChar(buf, '\'');
		while (*str)
		{
			/* ' and \ must be escaped by to \' and \\ */
			if (*str == '\'' || *str == '\\')
				appendPQExpBufferChar(buf, '\\');

			appendPQExpBufferChar(buf, *str);
			str++;
		}
		appendPQExpBufferChar(buf, '\'');
	}
	else
		appendPQExpBufferStr(buf, str);
}


/*
 * Find the pgport and try a connection until it reports not in recovery
 */
static void
wait_postmaster_connection(const char *connstr)
{
	PGPing		res;
	long		pmpid = 0;
	int			start_seconds_waited = 0;
	static const int start_seconds_to_wait = 30;

	print_msg(VERBOSITY_VERBOSE, "Waiting for PostgreSQL to accept connections ...");

	/*
	 * First wait for Postmaster to come up.
	 *
	 * It's possible for the postmaster to launch then quit immediately due to
	 * things like port conflicts. We won't get SIGCHLD for this because pg_ctl
	 * acts as an intermediary, so we just have to time out. We can't use
	 * pg_ctl -w because it waits for connection. pg_ctl status doesn't help
	 * us since it has the same race.
	 *
	 * So we just time out after a while.
	 */
	while (start_seconds_waited < start_seconds_to_wait)
	{
		if ((pmpid = get_pgpid()) != 0 &&
			postmaster_is_alive((pid_t) pmpid))
			break;

		pg_usleep(1000000);		/* 1 sec */
		print_msg(VERBOSITY_VERBOSE, ".");
		start_seconds_waited += 1;
	}

	if (start_seconds_waited == start_seconds_to_wait)
	{
		die(_("\nTimed out waiting for postmaster start after %d seconds, check bdr_init_copy_postgres.log\n"),
			start_seconds_waited);
	}
	else
	{
		print_msg(VERBOSITY_VERBOSE, _("\npostmaster started (pid=%ld), waiting for connection"), pmpid);
	}

	/*
	 * Now wait for Postmaster to either accept r/w (non-recovery) connections
	 * or die.
	 */
	for (;;)
	{
		res = PQping(connstr);
		if (res == PQPING_OK)
			break;
		else if (res == PQPING_NO_ATTEMPT)
			break;

		/*
		 * Check if the process is still alive. This covers cases where the
		 * postmaster successfully created the pidfile but then crashed without
		 * removing it.
		 */
		if (!postmaster_is_alive((pid_t) pmpid))
			break;

		

		/* No response; wait */
		pg_usleep(1000000);		/* 1 sec */
		print_msg(VERBOSITY_VERBOSE, ".");
	}

	print_msg(VERBOSITY_VERBOSE, "\n");
}

static void
wait_for_end_recovery(const char *connstr)
{
	PGconn *conn = connectdb((char*)connstr);

	print_msg(VERBOSITY_VERBOSE, _("Waiting for PostgreSQL to become read/write"));

	for (;;)
	{
		PGresult   *res;
		char*		inrecovery;

		res = PQexec(conn, "SELECT pg_catalog.pg_is_in_recovery()");

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			die(_("error while waiting for database to become read/write: %s: %s\n"),
				 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}

		if (PQntuples(res) != 1 || PQnfields(res) != 1 || PQgetisnull(res, 0, 0))
		{
			die(gettext_noop("nonsensical result from pg_is_in_recovery()"));
		}

		inrecovery = PQgetvalue(res, 0, 0);
		if (inrecovery[0] == 'f')
		{
			break;
		}
		else if (inrecovery[0] != 't')
		{
			die(gettext_noop("nonsensical result from pg_is_in_recovery, expected t|f, got %s"),
				inrecovery);
		}

		PQclear(res);

		/* Keep waiting */
		pg_usleep(1000000);		/* 1 sec */
		print_msg(VERBOSITY_VERBOSE, ".");
	}

	PQfinish(conn);

	print_msg(VERBOSITY_VERBOSE, " ready\n");
}

/*
 * Wait for postmaster to die
 */
static void
wait_postmaster_shutdown(void)
{
	long pid;

	print_msg(VERBOSITY_VERBOSE, "Waiting for PostgreSQL to shutdown ...");

	for (;;)
	{
		if ((pid = get_pgpid()) != 0)
		{
			pg_usleep(1000000);		/* 1 sec */
			print_msg(VERBOSITY_NORMAL, ".");
		}
		else
			break;
	}

	print_msg(VERBOSITY_VERBOSE, "\n");
}

static bool
file_exists(const char *path)
{
	struct stat statbuf;

	if (stat(path, &statbuf) != 0)
		return false;

	return true;
}

static bool
is_pg_dir(const char *path)
{
	struct stat statbuf;
	char		version_file[MAXPGPATH];

	if (stat(path, &statbuf) != 0)
		return false;

	snprintf(version_file, MAXPGPATH, "%s/PG_VERSION", data_dir);
	if (stat(version_file, &statbuf) != 0 && errno == ENOENT)
	{
		return false;
	}

	return true;
}

/*
 * copy one file
 */
static void
copy_file(char *fromfile, char *tofile)
{
	char	   *buffer;
	int			srcfd;
	int			dstfd;
	int			nbytes;
	off_t		offset;

#define COPY_BUF_SIZE (8 * BLCKSZ)

	buffer = malloc(COPY_BUF_SIZE);

	/*
	 * Open the files
	 */
	srcfd = open(fromfile, O_RDONLY | PG_BINARY, 0);
	if (srcfd < 0)
		die(_("could not open file \"%s\""), fromfile);

	dstfd = open(tofile, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY,
							  S_IRUSR | S_IWUSR);
	if (dstfd < 0)
		die(_("could not create file \"%s\""), tofile);

	/*
	 * Do the data copying.
	 */
	for (offset = 0;; offset += nbytes)
	{
		nbytes = read(srcfd, buffer, COPY_BUF_SIZE);
		if (nbytes < 0)
			die(_("could not read file \"%s\""), fromfile);
		if (nbytes == 0)
			break;
		errno = 0;
		if ((int) write(dstfd, buffer, nbytes) != nbytes)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			die(_("could not write to file \"%s\""), tofile);
		}
	}

	if (close(dstfd))
		die(_("could not close file \"%s\""), tofile);

	/* we don't care about errors here */
	close(srcfd);

	free(buffer);
}


static char *
find_other_exec_or_die(const char *argv0, const char *target, const char *versionstr)
{
	int			ret;
	char	   *found_path;
	uint32		bin_version;

	found_path = pg_malloc(MAXPGPATH);

	if (versionstr)
		ret = find_other_exec(argv0, target, versionstr, found_path);
	else
		ret = bdr_find_other_exec(argv0, target, &bin_version, found_path);

	if (ret < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv0, full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
			die(_("The program \"%s\" is needed by %s "
						   "but was not found in the\n"
						   "same directory as \"%s\".\n"
						   "Check your installation.\n"),
						 target, progname, full_path);
		else
			die(_("The program \"%s\" was found by \"%s\"\n"
						   "but was not the same version as %s.\n"
						   "Check your installation.\n"),
						 target, full_path, progname);
	}
	else if (!versionstr)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv0, full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (bin_version / 100 != PG_VERSION_NUM / 100)
			die(_("The program \"%s\" was found by \"%s\"\n"
						   "but was not the same version as %s.\n"
						   "Check your installation.\n"),
						 target, full_path, progname);

	}

	return found_path;
}

static bool
postmaster_is_alive(pid_t pid)
{
	/*
	 * Test to see if the process is still there.  Note that we do not
	 * consider an EPERM failure to mean that the process is still there;
	 * EPERM must mean that the given PID belongs to some other userid, and
	 * considering the permissions on $PGDATA, that means it's not the
	 * postmaster we are after.
	 *
	 * Don't believe that our own PID or parent shell's PID is the postmaster,
	 * either.  (Windows hasn't got getppid(), though.)
	 */
	if (pid == 0)
		return false;

	if (pid == getpid())
		return false;
#ifndef WIN32
	if (pid == getppid())
		return false;
#endif
	if (kill(pid, 0) == 0)
		return true;
	return false;
}

static long
get_pgpid(void)
{
	FILE	   *pidf;
	long		pid;

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		return 0;
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		return 0;
	}
	fclose(pidf);
	return pid;
}
