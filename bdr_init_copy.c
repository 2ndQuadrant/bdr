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

#include "postgres_fe.h"

#include "port.h"

#include "libpq-fe.h"
#include "libpq-int.h"

#include "miscadmin.h"

#include "access/timeline.h"

#include <dirent.h>
#include <fcntl.h>
#include <locale.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "bdr_config.h"
#include "bdr_internal.h"

#define LLOGCDIR "pg_logical/checkpoints"

#ifdef BUILDING_BDR
#define RIINTERFACE_PREFIX "pg_catalog.pg_"
#else
#define RIINTERFACE_PREFIX "bdr.bdr_"
#endif

typedef struct RemoteInfo {
	uint64		sysid;
	TimeLineID	tlid;
	Oid			dboid;
} RemoteInfo;

static char			*argv0 = NULL;
static const char	*progname;
static uint64		 system_identifier;
static NameData		 restore_point_name;
static char			*data_dir = NULL;
static char			*config_options = "";
static char			 pid_file[MAXPGPATH];
static time_t		 start_time;

/* defined as static so that die() can close them */
static PGconn		*local_conn = NULL;
static PGconn		*remote_conn = NULL;

BdrConnectionConfig	**bdr_connection_configs;
size_t				 bdr_connection_config_count;

static void signal_handler(int sig);
static void usage(void);
static void die(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
static void print_msg(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

static int run_pg_ctl(const char *arg, const char *opts);
static char *get_postgres_guc_value(char *guc, char *defval);
static bool wait_postmaster_connection(void);
static void wait_postgres_shutdown(void);

#ifdef BUILDING_UDR
static void initialize_bdr(PGconn *conn);
#endif
static void remove_unwanted_state(void);
static void initialize_replication_identifiers(char *remote_lsn);
static void create_replication_identifier(PGconn *conn,
				const char *remote_ident, char *remote_lsn);
static char *create_restore_point(char *remote_connstr);
static void initialize_replication_slots(bool init_replica);
static void create_replication_slot(PGconn *conn, Name slot_name);
static RemoteInfo *get_remote_info(PGconn *conn, char* aux_connstr);
static Oid get_dboid_from_dbname(PGconn *conn, const char* dbname);

static uint64 GenerateSystemIdentifier(void);
static int set_sysid(void);

static void read_bdr_config(void);
static void WriteRecoveryConf(PQExpBuffer contents);

static char *detect_local_conninfo(void);
static char *detect_remote_conninfo(void);
char *get_conninfo(char *dbname, char *dbhost, char *dbport, char *dbuser);
static char *PQconninfoParams_to_conninfo(const char *const * keywords, const char *const * values);
static char *escapeConninfoValue(const char *val);

static bool parse_bool(const char *value, bool *result);
static bool parse_bool_with_len(const char *value, size_t len, bool *result);
static char *trimwhitespace(const char *str);
static char	**split_list_guc(char *str, size_t *count);

static bool is_pg_dir(char *path);
static char *find_other_exec_or_die(const char *argv0, const char *target, const char *versionstr);
static bool postmaster_is_alive(pid_t pid);
static long get_pgpid(void);
static char **readfile(const char *path);
static void free_readfile(char **optlines);


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
	char *remote_lsn;
	bool hot_standby;
	char *local_connstr = NULL;
	char *remote_connstr = NULL;
	char *dbhost = NULL,
		 *dbport = NULL,
		 *dbuser = NULL;

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
	while ((c = getopt(argc, argv, "D:d:h:o:p:U:")) != -1)
	{
		switch (c)
		{
			case 'D':
				data_dir = pg_strdup(optarg);
				break;
			case 'o':
				config_options = pg_strdup(optarg);
				break;
			case 'd':
				remote_connstr = pg_strdup(optarg);
				break;
			case 'h':
				dbhost = pg_strdup(optarg);
				break;
			case 'p':
				dbport = pg_strdup(optarg);
				break;
			case 'U':
				dbuser = pg_strdup(optarg);
				break;
			default:
				fprintf(stderr, _("%s: unknown option\n"), progname);
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	if (data_dir == NULL)
	{
		fprintf(stderr, _("%s: no data directory specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}
	if (!is_pg_dir(data_dir))
	{
		die(_("%s: \"%s\" is not valid postgres data directory\n"), progname, data_dir);
	}
	snprintf(pid_file, MAXPGPATH, "%s/postmaster.pid", data_dir);

	print_msg(_("%s: starting...\n"), progname);

	/*
	 * Initialization
	 */
	system_identifier = GenerateSystemIdentifier();
	print_msg(_("Assigning new system identifier: "UINT64_FORMAT"...\n"), system_identifier);

	read_bdr_config();

	if (!remote_connstr && !dbhost && !dbport && !dbuser)
		remote_connstr = detect_remote_conninfo();
	else
		remote_connstr = get_conninfo(remote_connstr, dbhost, dbport, dbuser);

	if (!remote_connstr || !strlen(remote_connstr))
		die(_("Could not detect remote connection\n"));

	local_connstr = detect_local_conninfo();
	if (local_connstr == NULL)
		die(_("Failed to detect local connection info. Please specify replica_local_dsn in the postgresql.conf.\n"));

	/* Hot standby would start cluster in read only mode, we don't want that. */
	if (!parse_bool(get_postgres_guc_value("hot_standby", NULL), &hot_standby))
		die(_("Invalid boolean value for configuration parameter \"hot_standby\"\n"));
	if (hot_standby)
		die(_("Cluster cannot be configured with hot_standby = on when using bdr\n"));

	remove_unwanted_state();

	/*
	 * Initialization done, create replication slots to init node
	 * and restore point on remote side.
	 */
	print_msg(_("Creating primary replication slots...\n"));
	initialize_replication_slots(true);

	print_msg(_("Creating restore point...\n"));
	snprintf(NameStr(restore_point_name), NAMEDATALEN,
			 "bdr_"UINT64_FORMAT, system_identifier);
	remote_lsn = create_restore_point(remote_connstr);

	/*
	 * Get local db to consistent state (for lsn after slot creation).
	 */
	print_msg(_("Bringing cluster to the restore point...\n"));
	appendPQExpBuffer(recoveryconfcontents, "standby_mode = 'on'\n");
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_name = '%s'\n", NameStr(restore_point_name));
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_inclusive = true\n");
	appendPQExpBuffer(recoveryconfcontents, "primary_conninfo = '%s'\n", remote_connstr);
	WriteRecoveryConf(recoveryconfcontents);

	run_pg_ctl("start -w -l \"bdr_init_copy_postgres.log\"",
#ifdef BUILDING_BDR
			   "-c shared_preload_libraries=''"
#else
			   ""
#endif
			   );
	if (!wait_postmaster_connection())
		die(_("Could not connect to local node"));

	/*
	 * Postgres should have reached restore point and is accepting connections,
	 * create slots to other nodes and local replication identifiers.
	 */
	local_conn = PQconnectdb(local_connstr);
	if (PQstatus(local_conn) != CONNECTION_OK)
		die(_("Connection to database failed: %s"), PQerrorMessage(local_conn));

#ifdef BUILDING_UDR
	print_msg(_("Ensuring bdr extension is installed...\n"));
	initialize_bdr(remote_conn);
	initialize_bdr(local_conn);
#endif

	print_msg(_("Creating secondary replication slots...\n"));
	initialize_replication_slots(false);
	print_msg(_("Creating local replication identifier...\n"));
	initialize_replication_identifiers(remote_lsn);

	PQfinish(local_conn);
	local_conn = NULL;

	/*
	 * Make this node functional as individual bdr node and start it.
	 */
	run_pg_ctl("stop", "");
	wait_postgres_shutdown();

	set_sysid();

	print_msg(_("Starting the cluster...\n"));
	run_pg_ctl("start -w", "-c bdr.init_from_basedump=true");

	return 0;
}


/*
 * Print help.
 */
static void
usage(void)
{
	printf(_("%s initializes bdr from PostgreSQL instance made using pg_basebackup.\n\n"), progname);
	printf(_("pg_basebackup -X stream must be used to populate the data directory before\n"));
	printf(_("running %s to initialize BDR on it.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nGeneral options:\n"));
	printf(_("  -D, --pgdata=DIRECTORY base backup directory\n"));
	printf(_("  -o                     configuration options passed to pg_ctl's -o\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -d, --dbname=CONNSTR   connection string\n"));
	printf(_("  -h, --host=HOSTNAME    database server host or socket directory\n"));
	printf(_("  -p, --port=PORT        database server port number\n"));
	printf(_("  -U, --username=NAME    connect as specified database user\n"));
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

	PQfinish(local_conn);
	PQfinish(remote_conn);

	if (get_pgpid())
		run_pg_ctl("stop -s", "");

	exit(1);
}

/*
 * Print message to stdout and flush
 */
static void
print_msg(const char *fmt,...)
{
	va_list argptr;
	va_start(argptr, fmt);
	vfprintf(stdout, fmt, argptr);
	va_end(argptr);
	fflush(stdout);
}


/*
 * Start pg_ctl with given argument(s) - used to start/stop postgres
 */
static int
run_pg_ctl(const char *arg, const char *opts)
{
	int			 ret;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path = find_other_exec_or_die(argv0, "pg_ctl", "pg_ctl (PostgreSQL) " PG_VERSION "\n");

	appendPQExpBuffer(cmd, "%s %s -D \"%s\" -o \"%s %s\"", exec_path, arg, data_dir,
					  opts, config_options);

	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	return ret;
}


/*
 * Ugly way to read postgresql.conf
 */
static char *
get_postgres_guc_value(char *guc, char *defval)
{
	FILE		*fp;
	int			 status;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path = find_other_exec_or_die(argv0, "postgres", PG_BACKEND_VERSIONSTR);
	PQExpBuffer	 retbuf = createPQExpBuffer();
	char		 buf[8192];
	char		*ret;

	printfPQExpBuffer(cmd, "%s -D \"%s\" %s -C \"%s\" 2>\"%s\"",
					  exec_path, data_dir, config_options, guc, DEVNULL);

	fp = popen(cmd->data, "r");
	while (fgets(buf, sizeof(buf), fp) != NULL)
		appendPQExpBufferStr(retbuf, buf);

	status = pclose(fp);
	destroyPQExpBuffer(cmd);

	if (status != 0)
	{
		destroyPQExpBuffer(retbuf);
		return defval;
	}

	ret = trimwhitespace(retbuf->data);
	destroyPQExpBuffer(retbuf);

	return ret;
}

/*
 * Set system identifier to system id we used for registering the slots.
 */
static int
set_sysid(void)
{
	int			 ret;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path = find_other_exec_or_die(argv0, "bdr_resetxlog", "bdr_resetxlog (PostgreSQL) " PG_VERSION "\n");

	appendPQExpBuffer(cmd, "%s \"-s "UINT64_FORMAT"\" \"%s\"", exec_path, system_identifier, data_dir);

	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	return ret;
}


/*
 * Read bdr configuration
 *
 * This is somewhat ugly version of bdr_create_con_gucs and parts of _PG_init
 */
static void
read_bdr_config(void)
{
	char		*connections;
	char		*errormsg = NULL;
	int			connection_config_idx;
	size_t		connection_count = 0;
	char		**connames;
	PQconninfoOption *options;
	PQconninfoOption *cur_option;

	connections = get_postgres_guc_value("bdr.connections", NULL);
	if (!connections)
		die(_("bdr.connections is empty\n"));

	connames = split_list_guc(connections, &connection_count);
	pg_free(connections);

	bdr_connection_config_count = connection_count;
	bdr_connection_configs = (BdrConnectionConfig**)
		pg_malloc0(bdr_connection_config_count * sizeof(BdrConnectionConfig*));

	for (connection_config_idx = 0; connection_config_idx < connection_count; connection_config_idx++)
	{
		char	*name = (char *) connames[connection_config_idx];
		char	*optname_dsn = pg_malloc(strlen(name) + 30);
		char	*optname_local_dsn = pg_malloc(strlen(name) + 30);
		char	*optname_replica = pg_malloc(strlen(name) + 30);
		char	*optname_local_dbname = pg_malloc(strlen(name) + 30);
		BdrConnectionConfig *opts;

		sprintf(optname_dsn, "bdr.%s_dsn", name);
		sprintf(optname_local_dsn, "bdr.%s_replica_local_dsn", name);
		sprintf(optname_replica, "bdr.%s_init_replica", name);
		sprintf(optname_local_dbname, "bdr.%s_local_dbname", name);

		opts = pg_malloc0(sizeof(BdrConnectionConfig));
		opts->name = pg_strdup(name);
		opts->is_valid = false;

		bdr_connection_configs[connection_config_idx] = opts;

		opts->dsn = get_postgres_guc_value(optname_dsn, NULL);
		if (!opts->dsn)
			continue;

		opts->replica_local_dsn = get_postgres_guc_value(optname_local_dsn, NULL);

		if (!parse_bool(get_postgres_guc_value(optname_replica, "false"), &opts->init_replica))
			die(_("Invalid boolean value for configuration parameter \"%s\"\n"), optname_replica);

		opts->dbname = get_postgres_guc_value(optname_local_dbname, NULL);

		options = PQconninfoParse(opts->dsn, &errormsg);
		if (errormsg != NULL)
		{
			char *str = pg_strdup(errormsg);

			PQfreemem(errormsg);
			die(_("bdr %s: error in dsn: %s\n"), name, str);
		}

		if (opts->dbname == NULL)
		{
			cur_option = options;
			while (cur_option->keyword != NULL)
			{
				if (strcmp(cur_option->keyword, "dbname") == 0)
				{
					if (cur_option->val == NULL)
						die(_("bdr %s: no dbname set\n"), name);

					opts->dbname = pg_strdup(cur_option->val);
				}
				cur_option++;
			}
		}


		opts->is_valid = true;

		/* cleanup */
		PQconninfoFree(options);
	}
}



/*
 * Cleans everything that was replicated via basebackup but we don't want it.
 */
static void
remove_unwanted_state(void)
{
#ifdef BUILDING_BDR
	DIR				*lldir;
	struct dirent	*llde;
	PQExpBuffer		 llpath = createPQExpBuffer();
	PQExpBuffer		 filename = createPQExpBuffer();

	printfPQExpBuffer(llpath, "%s/%s", data_dir, LLOGCDIR);

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
#endif
}


/*
 * Initialize replication slots
 *
 * Get connection configs from bdr and use the info
 * to register replication slots for future use.
 */
static void
initialize_replication_slots(bool init_replica)
{
	int		 i;

	for (i = 0; i < bdr_connection_config_count; i++)
	{
		NameData	 slot_name;
		char		 remote_ident[256];
		RemoteInfo	*ri;
		TimeLineID	 tlid;
		Oid			 dboid;
		char		 system_identifier_s[32];
		BdrConnectionConfig *cfg = bdr_connection_configs[i];
		PQExpBuffer		 conninfo = createPQExpBuffer();

		if (!cfg || !cfg->is_valid || cfg->init_replica != init_replica)
			continue;

		printfPQExpBuffer(conninfo, "%s replication=database", cfg->dsn);
		remote_conn = PQconnectdb(conninfo->data);
		destroyPQExpBuffer(conninfo);

		if (PQstatus(remote_conn) != CONNECTION_OK)
		{
			die(_("Could not connect to the remote server: %s\n"),
						PQerrorMessage(remote_conn));
		}

		ri = get_remote_info(remote_conn, cfg->dsn);
		dboid = cfg->init_replica ? ri->dboid : get_dboid_from_dbname(local_conn, cfg->dbname);

		/* XXX: this might break if timeline switch happens in meantime */
		tlid = cfg->init_replica ? ri->tlid + 1 : ri->tlid;

		snprintf(system_identifier_s, sizeof(system_identifier_s), UINT64_FORMAT, system_identifier);
 		snprintf(NameStr(slot_name), NAMEDATALEN, BDR_SLOT_NAME_FORMAT,
				 ri->dboid, system_identifier_s, tlid,
				 dboid, "");
		NameStr(slot_name)[NAMEDATALEN - 1] = '\0';

		create_replication_slot(remote_conn, &slot_name);

		PQfinish(remote_conn);
		remote_conn = NULL;

		snprintf(remote_ident, sizeof(remote_ident),
				BDR_NODE_ID_FORMAT,
				ri->sysid, ri->tlid, ri->dboid, dboid,
				"");
	}
}

/*
 * Get database Oid of the remotedb.
 *
 * Can't use the bdr_get_remote_dboid because it needs elog :(
 */
static Oid
get_remote_dboid(char *conninfo_db)
{
	PGconn	   *dbConn;
	PGresult   *res;
	char	   *remote_dboid;
	Oid			remote_dboid_i;

	dbConn = PQconnectdb(conninfo_db);
	if (PQstatus(dbConn) != CONNECTION_OK)
	{
		die(_("Could not connect to the primary server: %s"), PQerrorMessage(dbConn));
	}

	res = PQexec(dbConn, "SELECT oid FROM pg_database WHERE datname = current_database()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die(_("Could fetch database oid: %s"), PQerrorMessage(dbConn));

	if (PQntuples(res) != 1 || PQnfields(res) != 1)
		die(_("Could not identify system: got %d rows and %d fields, expected %d rows and %d fields\n"),
			 PQntuples(res), PQnfields(res), 1, 1);

	remote_dboid = PQgetvalue(res, 0, 0);
	if (sscanf(remote_dboid, "%u", &remote_dboid_i) != 1)
		die(_("could not parse remote database OID %s"), remote_dboid);

	PQclear(res);
	PQfinish(dbConn);

	return remote_dboid_i;
}

/*
 * Read replication info about remote connection
 */
static RemoteInfo *
get_remote_info(PGconn *conn, char* aux_connstr)
{
	RemoteInfo	*ri = (RemoteInfo *)pg_malloc(sizeof(RemoteInfo));
	char	   *remote_sysid;
	char	   *remote_tlid;
	char	   *remote_dboid;
	PGresult   *res;

	res = PQexec(conn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		die(_("Could not send replication command \"%s\": %s\n"),
			 "IDENTIFY_SYSTEM", PQerrorMessage(conn));
	}

	if (PQntuples(res) != 1 || PQnfields(res) < 4 || PQnfields(res) > 5)
	{
		PQclear(res);
		die(_("Could not identify system: got %d rows and %d fields, expected %d rows and %d or %d fields\n"),
			 PQntuples(res), PQnfields(res), 1, 4, 5);
	}

	remote_sysid = PQgetvalue(res, 0, 0);
	remote_tlid = PQgetvalue(res, 0, 1);

	if (PQnfields(res) == 5)
	{
		remote_dboid = PQgetvalue(res, 0, 4);
		if (sscanf(remote_dboid, "%u", &ri->dboid) != 1)
			die(_("could not parse remote database OID %s"), remote_dboid);
	}
	else
	{
		ri->dboid = get_remote_dboid(aux_connstr);
	}

#ifdef HAVE_STRTOULL
	ri->sysid = strtoull(remote_sysid, NULL, 10);
#else
	ri->sysid = strtoul(remote_sysid, NULL, 10);
#endif

	if (sscanf(remote_tlid, "%u", &ri->tlid) != 1)
		die(_("Could not parse remote tlid %s\n"), remote_tlid);

	PQclear(res);

	return ri;
}

/*
 * Get dboid based on dbname
 */
static Oid
get_dboid_from_dbname(PGconn *conn, const char* dbname)
{
	char		*dboid_str;
	Oid			 dboid;
	PQExpBuffer	 query = createPQExpBuffer();
	PGresult	*res;

	appendPQExpBuffer(query, "SELECT oid FROM pg_catalog.pg_database WHERE datname = '%s'",
					 dbname);

	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1)
	{
		PQclear(res);
		die(_("Could not get database id for \"%s\": %s\n"),
			 dbname, PQerrorMessage(conn));
	}

	dboid_str = PQgetvalue(res, 0, 0);
	if (sscanf(dboid_str, "%u", &dboid) != 1)
		die(_("Could not parse database OID %s\n"), dboid_str);

	PQclear(res);
	destroyPQExpBuffer(query);

	return dboid;
}

/*
 * Create replication slot
 */
static void
create_replication_slot(PGconn *conn, Name slot_name)
{
	PQExpBuffer query = createPQExpBuffer();
	PGresult   *res;

	appendPQExpBuffer(query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
					 NameStr(*slot_name), "bdr");

	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not send replication command \"%s\": status %s: %s\n"),
			 query->data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}

#ifdef BUILDING_UDR
static void
install_extension_if_not_exists(PGconn *conn, const char *extname)
{
	PQExpBuffer		query = createPQExpBuffer();
	PGresult	   *res;

	printfPQExpBuffer(query, "SELECT 1 FROM pg_catalog.pg_extension WHERE extname = %s;",
					  PQescapeLiteral(conn, extname, strlen(extname)));
	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		die(_("Could not read extension info: %s\n"), PQerrorMessage(conn));
	}

	if (PQntuples(res) != 1)
	{
		PQclear(res);

		printfPQExpBuffer(query, "CREATE EXTENSION %s;",
						  PQescapeIdentifier(conn, extname, strlen(extname)));
		res = PQexec(conn, query->data);

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			PQclear(res);
			die(_("Could not install %s extension: %s\n"), extname, PQerrorMessage(conn));
		}
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}

/*
 * Initialize bdr extension (if not already initialized).
 *
 * Should have similar logic as bdr_maintain_schema in bdr.c.
 */
static void
initialize_bdr(PGconn *conn)
{
	install_extension_if_not_exists(conn, "btree_gist");
	install_extension_if_not_exists(conn,"bdr");
}
#endif

/*
 * Initialize new remote identifiers to specific position.
 */
static void
initialize_replication_identifiers(char *remote_lsn)
{
	int				 i;
	PGresult		*res;

	/* Remove replication identifiers */
	res = PQexec(local_conn, "SELECT "RIINTERFACE_PREFIX"replication_identifier_drop(riname) FROM "RIINTERFACE_PREFIX"replication_identifier;");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		die(_("Could not remove replication identifier: %s\n"), PQerrorMessage(local_conn));
	}

	/* Initialize new replication identifiers */
	for (i = 0; i < bdr_connection_config_count; i++)
	{
		char		remote_ident[256];
		Oid			dboid;
		RemoteInfo	*ri;
		BdrConnectionConfig *cfg = bdr_connection_configs[i];
		PQExpBuffer	conninfo = createPQExpBuffer();

		if (!cfg || !cfg->is_valid)
			continue;

		printfPQExpBuffer(conninfo, "%s replication=database", cfg->dsn);
		remote_conn = PQconnectdb(conninfo->data);
		destroyPQExpBuffer(conninfo);

		if (PQstatus(remote_conn) != CONNECTION_OK)
		{
			die(_("Could not connect to the remote server: %s\n"),
						PQerrorMessage(remote_conn));
		}

		ri = get_remote_info(remote_conn, cfg->dsn);
		dboid = cfg->init_replica ? ri->dboid : get_dboid_from_dbname(local_conn, cfg->dbname);

		PQfinish(remote_conn);
		remote_conn = NULL;

		snprintf(remote_ident, sizeof(remote_ident),
				BDR_NODE_ID_FORMAT,
				ri->sysid, ri->tlid, ri->dboid, dboid,
				"");

		create_replication_identifier(local_conn, remote_ident,
									  cfg->init_replica ? remote_lsn : NULL);
	}
}

/*
 * Create local replication identifier
 */
static void
create_replication_identifier(PGconn *conn, const char *remote_ident, char *remote_lsn)
{
	PQExpBuffer query = createPQExpBuffer();
	PGresult   *res;

	printfPQExpBuffer(query, "SELECT "RIINTERFACE_PREFIX"replication_identifier_create('%s')",
					 remote_ident);

	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create replication indentifier \"%s\": status %s: %s\n"),
			 query->data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	PQclear(res);

	if (remote_lsn)
	{
		printfPQExpBuffer(query, "SELECT "RIINTERFACE_PREFIX"replication_identifier_advance('%s', '%s', '0/0')",
						 remote_ident, remote_lsn);

		res = PQexec(conn, query->data);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			die(_("Could not advance replication indentifier \"%s\": status %s: %s\n"),
				 query->data,
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
create_restore_point(char *remote_connstr)
{
	PQExpBuffer  query = createPQExpBuffer();
	PGresult	*res;
	char		*remote_lsn = NULL;

	remote_conn = PQconnectdb(remote_connstr);
	if (PQstatus(remote_conn) != CONNECTION_OK)
	{
		die(_("Could not connect to the remote server: %s\n"),
					PQerrorMessage(remote_conn));
	}

	printfPQExpBuffer(query, "SELECT pg_create_restore_point('%s')", NameStr(restore_point_name));
	res = PQexec(remote_conn, query->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create restore point \"%s\": status %s: %s\n"),
			 query->data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	remote_lsn = pstrdup(PQgetvalue(res, 0, 0));

	PQclear(res);
	PQfinish(remote_conn);
	remote_conn = NULL;
	destroyPQExpBuffer(query);

	return remote_lsn;
}

static char *
detect_local_conninfo(void)
{
	int i;

	for (i = 0; i < bdr_connection_config_count; i++)
	{
		BdrConnectionConfig *cfg = bdr_connection_configs[i];

		if (!cfg || !cfg->is_valid || !cfg->init_replica ||
			!cfg->replica_local_dsn)
			continue;

		return pg_strdup(cfg->replica_local_dsn);
	}

	return NULL;
}

static char *
detect_remote_conninfo(void)
{
	int i;

	for (i = 0; i < bdr_connection_config_count; i++)
	{
		BdrConnectionConfig *cfg = bdr_connection_configs[i];

		if (!cfg || !cfg->is_valid || !cfg->init_replica)
			continue;

		return pg_strdup(cfg->dsn);
	}

	return NULL;
}

char *
get_conninfo(char *dbname, char *dbhost, char *dbport, char *dbuser)
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
	if (dbname)
	{
		conn_opts = PQconninfoParse(dbname, &err_msg);
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

		keywords[i] = "dbname";
		values[i] = dbname == NULL ? "postgres" : dbname;
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

	ret = PQconninfoParams_to_conninfo(keywords, values);

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
 * Convert PQconninfoOption array into conninfo string
 */
static char *
PQconninfoParams_to_conninfo(const char *const * keywords, const char *const * values)
{
	PQExpBuffer	 retbuf = createPQExpBuffer();
	char		*ret;
	int			 i = 0;

	while (keywords[i])
	{
		char *tmpval = escapeConninfoValue(values[i]);
		appendPQExpBuffer(retbuf, "%s = '%s' ", keywords[i], tmpval);
		pg_free(tmpval);
		i++;
	}

	ret = pg_strdup(retbuf->data);
	destroyPQExpBuffer(retbuf);

	return ret;
}

/*
 * Escape connection info value
 */
static char *
escapeConninfoValue(const char *val)
{
	int i, j;
	char *ret = pg_malloc(strlen(val) * 2 + 1);

	j = 0;
	for (i = 0; i < strlen(val); i++)
	{
		switch (val[i])
		{
			case '\\':
			case '\'':
				ret[j++] = '\\';
			default:
				break;
		}

		ret[j++] = val[i];
	}

	ret[j] = '\0';

	return ret;
}


/*
 * Taken from adt/bool.c
 *
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
static bool
parse_bool(const char *value, bool *result)
{
	return parse_bool_with_len(value, strlen(value), result);
}

static bool
parse_bool_with_len(const char *value, size_t len, bool *result)
{
	switch (*value)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'y':
		case 'Y':
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'n':
		case 'N':
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = false;		/* suppress compiler warning */
	return false;
}

/*
 * Remove leading and trailing whitespace from the string,
 * does not change input
 */
static char *
trimwhitespace(const char *str)
{
	const char *end;
	char *res;
	size_t len;

	while(isspace(*str))
		str++;

	if(*str == 0)
		return NULL;

	end = str + strlen(str) - 1;
	while(end > str && isspace(*end))
		end--;

	len = end-str;
	if (!len)
		return NULL;

	len++;
	res = pg_malloc(len+1);
	memcpy(res, str, len);
	res[len] = '\0';

	return res;
}

/*
 * Split guc list paramenter into array
 * Note that this is not 100% compatible with that is in core
 * but seems good enough for our purposes
 */
static char	**
split_list_guc(char *str, size_t *count)
{
	char	**ret = NULL;
	char	 *t = strtok (str, ",");
	size_t	  i = 0;

	while (t) {
		ret = realloc(ret, sizeof(char*)* ++i);

		if (ret == NULL)
			die(_("Out of memory\n"));

		t = trimwhitespace(t);
		if (!t)
			die(_("Bad input for list: %s\n"), str);

		ret[i-1] = t;

		t = strtok(NULL, ",");
	}

	*count = i;
	return ret;
}


/*
 * Find the pgport and try a connection
 *
 * Based on pg_ctl.c:test_postmaster_connection
 */
static bool
wait_postmaster_connection(void)
{
	PGPing		res;
	long		pm_pid = 0;
	char		connstr[MAXPGPATH * 2 + 256];

	connstr[0] = '\0';

	for (;;)
	{
		/* Do we need a connection string? */
		if (connstr[0] == '\0')
		{
			/*----------
			 * The number of lines in postmaster.pid tells us several things:
			 *
			 * # of lines
			 *		0	lock file created but status not written
			 *		2	pre-9.1 server, shared memory not created
			 *		3	pre-9.1 server, shared memory created
			 *		5	9.1+ server, ports not opened
			 *		6	9.1+ server, shared memory not created
			 *		7	9.1+ server, shared memory created
			 *
			 * If we see less than 6 lines in postmaster.pid, just keep
			 * waiting.
			 *----------
			 */
			char	  **optlines;

			/* Try to read the postmaster.pid file */
			if ((optlines = readfile(pid_file)) != NULL &&
				optlines[0] != NULL &&
				optlines[1] != NULL &&
				optlines[2] != NULL &&
				optlines[3] != NULL &&
				optlines[4] != NULL &&
				optlines[5] != NULL)
			{
				/* File is complete enough for us, parse it */
				long		pmpid;
				time_t		pmstart;

				/*
				 * Make sanity checks.  If it's for a standalone backend
				 * (negative PID), or the recorded start time is before
				 * pg_ctl started, then either we are looking at the wrong
				 * data directory, or this is a pre-existing pidfile that
				 * hasn't (yet?) been overwritten by our child postmaster.
				 * Allow 2 seconds slop for possible cross-process clock
				 * skew.
				 */
				pmpid = atol(optlines[LOCK_FILE_LINE_PID - 1]);
				pmstart = atol(optlines[LOCK_FILE_LINE_START_TIME - 1]);
				if (pmpid > 0 || pmstart > start_time - 3)
				{
					/*
					 * OK, seems to be a valid pidfile from our child.
					 */
					int			portnum;
					char	   *sockdir;
					char	   *hostaddr;
					char		host_str[MAXPGPATH];

					pm_pid = pmpid;

					/*
					 * Extract port number and host string to use. Prefer
					 * using Unix socket if available.
					 */
					portnum = atoi(optlines[LOCK_FILE_LINE_PORT - 1]);
					sockdir = optlines[LOCK_FILE_LINE_SOCKET_DIR - 1];
					hostaddr = optlines[LOCK_FILE_LINE_LISTEN_ADDR - 1];

					/*
					 * While unix_socket_directories can accept relative
					 * directories, libpq's host parameter must have a
					 * leading slash to indicate a socket directory.  So,
					 * ignore sockdir if it's relative, and try to use TCP
					 * instead.
					 */
					if (sockdir[0] == '/')
						strlcpy(host_str, sockdir, sizeof(host_str));
					else
						strlcpy(host_str, hostaddr, sizeof(host_str));

					/* remove trailing newline */
					if (strchr(host_str, '\n') != NULL)
						*strchr(host_str, '\n') = '\0';

					/* Fail if couldn't get either sockdir or host addr */
					if (host_str[0] == '\0')
					{
						fprintf(stderr, _("Relative socket directory is not supported\n"));
						return false;
					}

					/* If postmaster is listening on "*", use localhost */
					if (strcmp(host_str, "*") == 0)
						strcpy(host_str, "localhost");

					/*
					 * We need to set connect_timeout otherwise on Windows
					 * the Service Control Manager (SCM) will probably
					 * timeout first.
					 */
					snprintf(connstr, sizeof(connstr),
					"dbname=postgres port=%d host='%s' connect_timeout=5",
							 portnum, host_str);
				}
			}

			/*
			 * Free the results of readfile.
			 *
			 * This is safe to call even if optlines is NULL.
			 */
			free_readfile(optlines);
		}

		/* If we have a connection string, ping the server */
		if (connstr[0] != '\0')
		{
			res = PQping(connstr);
			if (res == PQPING_OK)
			{
				break;
			}
			else if (res == PQPING_NO_ATTEMPT)
				return false;
		}

		/*
		 * If we've been able to identify the child postmaster's PID, check
		 * the process is still alive.  This covers cases where the postmaster
		 * successfully created the pidfile but then crashed without removing
		 * it.
		 */
		if (pm_pid > 0 && !postmaster_is_alive((pid_t) pm_pid))
			return false;

		/* No response, or startup still in process; wait */
		pg_usleep(1000000);		/* 1 sec */
		print_msg(".");
	}

	return true;
}

/*
 * Wait for postmaster to die
 */
static void
wait_postgres_shutdown(void)
{
	long pid;

	for (;;)
	{
		if ((pid = get_pgpid()) != 0)
		{
			pg_usleep(1000000);		/* 1 sec */
			print_msg(".");
		}
		else
			break;
	}
}

static bool
is_pg_dir(char *path)
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
 * Utility functions taken from pg_ctl
 */

static char *
find_other_exec_or_die(const char *argv0, const char *target, const char *versionstr)
{
	int			ret;
	char	   *found_path;

	found_path = pg_malloc(MAXPGPATH);

	if ((ret = find_other_exec(argv0, target, versionstr, found_path)) < 0)
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

/*
 * get the lines from a text file - return NULL if file can't be opened
 */
static char **
readfile(const char *path)
{
	int			fd;
	int			nlines;
	char	  **result;
	char	   *buffer;
	char	   *linebegin;
	int			i;
	int			n;
	int			len;
	struct stat statbuf;

	/*
	 * Slurp the file into memory.
	 *
	 * The file can change concurrently, so we read the whole file into memory
	 * with a single read() call. That's not guaranteed to get an atomic
	 * snapshot, but in practice, for a small file, it's close enough for the
	 * current use.
	 */
	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return NULL;
	if (fstat(fd, &statbuf) < 0)
	{
		close(fd);
		return NULL;
	}
	if (statbuf.st_size == 0)
	{
		/* empty file */
		close(fd);
		result = (char **) pg_malloc(sizeof(char *));
		*result = NULL;
		return result;
	}
	buffer = pg_malloc(statbuf.st_size + 1);

	len = read(fd, buffer, statbuf.st_size + 1);
	close(fd);
	if (len != statbuf.st_size)
	{
		/* oops, the file size changed between fstat and read */
		free(buffer);
		return NULL;
	}

	/*
	 * Count newlines. We expect there to be a newline after each full line,
	 * including one at the end of file. If there isn't a newline at the end,
	 * any characters after the last newline will be ignored.
	 */
	nlines = 0;
	for (i = 0; i < len; i++)
	{
		if (buffer[i] == '\n')
			nlines++;
	}

	/* set up the result buffer */
	result = (char **) pg_malloc((nlines + 1) * sizeof(char *));

	/* now split the buffer into lines */
	linebegin = buffer;
	n = 0;
	for (i = 0; i < len; i++)
	{
		if (buffer[i] == '\n')
		{
			int			slen = &buffer[i] - linebegin + 1;
			char	   *linebuf = pg_malloc(slen + 1);

			memcpy(linebuf, linebegin, slen);
			linebuf[slen] = '\0';
			result[n++] = linebuf;
			linebegin = &buffer[i + 1];
		}
	}
	result[n] = NULL;

	free(buffer);

	return result;
}

/*
 * Free memory allocated for optlines through readfile()
 */
void
free_readfile(char **optlines)
{
	char	   *curr_line = NULL;
	int			i = 0;

	if (!optlines)
		return;

	while ((curr_line = optlines[i++]))
		free(curr_line);

	free(optlines);

	return;
}
