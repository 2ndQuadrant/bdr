/*-------------------------------------------------------------------------
 *
 * pg_receivellog.c - receive streaming logical log data and write it
 *					  to a local file.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/pg_receivellog.c
 *-------------------------------------------------------------------------
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.	Hence this ugly hack.
 */
#define FRONTEND 1
#include "postgres.h"

#include "common/fe_memutils.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "access/xlog_internal.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

#include "receivelog.h"
#include "streamutil.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "getopt_long.h"

/* Time to sleep between reconnection attempts */
#define RECONNECT_SLEEP_TIME 5

/* Global options */
static char	   *outfile = NULL;
static int	        outfd = -1;
static int			verbose = 0;
static int			noloop = 0;
static int			standby_message_timeout = 10 * 1000;		/* 10 sec = default */
static volatile bool time_to_abort = false;
static const char *plugin = "test_decoding";
static const char *slot = NULL;
static XLogRecPtr	startpos;
static bool do_init_slot = false;
static bool do_start_slot = false;
static bool do_stop_slot = false;


static void usage(void);
static void StreamLog();

static void
usage(void)
{
	printf(_("%s receives PostgreSQL logical change stream.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -f, --file=FILE        receive log into this file. - for stdout\n"));
	printf(_("  -n, --no-loop          do not loop on connection lost\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -d, --database=DBNAME  database to connect to\n"));
	printf(_("  -h, --host=HOSTNAME    database server host or socket directory\n"));
	printf(_("  -p, --port=PORT        database server port number\n"));
	printf(_("  -U, --username=NAME    connect as specified database user\n"));
	printf(_("  -w, --no-password      never prompt for password\n"));
	printf(_("  -W, --password         force password prompt (should happen automatically)\n"));
	printf(_("\nReplication options:\n"));
	printf(_("  -P, --plugin=PLUGIN    use output plugin PLUGIN (defaults to test_decoding)\n"));
	printf(_("  -s, --status-interval=INTERVAL\n"
			 "                         time between status packets sent to server (in seconds)\n"));
	printf(_("  -S, --slot=SLOT        use existing replication slot SLOT instead of starting a new one\n"));
	printf(_("\nAction to be performed:\n"));
	printf(_("      --init             initiate a new replication slot (for the slotname see --slot)\n"));
	printf(_("      --start            start streaming in a replication slot (for the slotname see --slot)\n"));
	printf(_("      --stop             stop the replication slot (for the slotname see --slot)\n"));
	printf(_("\nReport bugs to <pgsql-bugs@postgresql.org>.\n"));
}


/*
 * Local version of GetCurrentTimestamp(), since we are not linked with
 * backend code. The protocol always uses integer timestamps, regardless of
 * server setting.
 */
static int64
localGetCurrentTimestamp(void)
{
	int64 result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (int64) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}

/*
 * Local version of TimestampDifference(), since we are not linked with
 * backend code.
 */
static void
localTimestampDifference(int64 start_time, int64 stop_time,
						 long *secs, int *microsecs)
{
	int64 diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
	}
}

/*
 * Local version of TimestampDifferenceExceeds(), since we are not
 * linked with backend code.
 */
static bool
localTimestampDifferenceExceeds(int64 start_time,
								int64 stop_time,
								int msec)
{
	int64 diff = stop_time - start_time;

	return (diff >= msec * INT64CONST(1000));
}

/*
 * Converts an int64 to network byte order.
 */
static void
sendint64(int64 i, char *buf)
{
	uint32		n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32) (i >> 32);
	n32 = htonl(n32);
	memcpy(&buf[0], &n32, 4);

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = htonl(n32);
	memcpy(&buf[4], &n32, 4);
}

/*
 * Converts an int64 from network byte order to native format.
 */
static int64
recvint64(char *buf)
{
	int64		result;
	uint32		h32;
	uint32		l32;

	memcpy(&h32, buf, 4);
	memcpy(&l32, buf + 4, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, XLogRecPtr blockpos, int64 now, bool replyRequested)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	if (blockpos == startpos)
		return true;

	if (verbose)
		fprintf(stderr,
				_("%s: confirming flush up to %X/%X (slot %s)\n"),
				progname, (uint32) (blockpos >> 32), (uint32) blockpos,
				slot);

	replybuf[len] = 'r';
	len += 1;
	sendint64(blockpos, &replybuf[len]);			/* write */
	len += 8;
	sendint64(blockpos, &replybuf[len]);	/* flush */
	len += 8;
	sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	sendint64(now, &replybuf[len]);					/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0;			/* replyRequested */
	len += 1;

	startpos = blockpos;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		fprintf(stderr, _("%s: could not send feedback packet: %s"),
				progname, PQerrorMessage(conn));
		return false;
	}

	return true;
}

/*
 * Start the log streaming
 */
static void
StreamLog(void)
{
	PGresult   *res;
	char		query[256];
	char	   *copybuf = NULL;
	int64		last_status = -1;
	XLogRecPtr	logoff = InvalidXLogRecPtr;

	/*
	 * Connect in replication mode to the server
	 */
	if (!conn)
		conn = GetConnection();
	if (!conn)
		/* Error message already written in GetConnection() */
		return;

	/*
	 * Start the replication
	 */
	if (verbose)
		fprintf(stderr,
				_("%s: starting log streaming at %X/%X (slot %s)\n"),
				progname, (uint32) (startpos >> 32), (uint32) startpos,
				slot);

	/* Initiate the replication stream at specified location */
	snprintf(query, sizeof(query), "START_LOGICAL_REPLICATION \"%s\" %X/%X",
			 slot, (uint32) (startpos >> 32), (uint32) startpos);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		fprintf(stderr, _("%s: could not send replication command \"%s\": %s\n"),
				progname, query, PQresultErrorMessage(res));
		PQclear(res);
		goto error;
	}
	PQclear(res);

	if (verbose)
		fprintf(stderr,
				_("%s: initiated streaming\n"),
				progname);

	while (!time_to_abort)
	{
		int			r;
		int			bytes_left;
		int			bytes_written;
		int64		now;
		int         hdr_len;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		/*
		 * Potentially send a status message to the master
		 */
		now = localGetCurrentTimestamp();
		if (standby_message_timeout > 0 &&
			localTimestampDifferenceExceeds(last_status, now,
											standby_message_timeout))
		{
			/* Time to send feedback! */
			if (!sendFeedback(conn, logoff, now, false))
				goto error;

			last_status = now;
		}

		r = PQgetCopyData(conn, &copybuf, 1);
		if (r == 0)
		{
			/*
			 * In async mode, and no data available. We block on reading but
			 * not more than the specified timeout, so that we can send a
			 * response back to the client.
			 */
			fd_set		input_mask;
			struct timeval timeout;
			struct timeval *timeoutptr;

			FD_ZERO(&input_mask);
			FD_SET(PQsocket(conn), &input_mask);
			if (standby_message_timeout)
			{
				int64       targettime;
				long		secs;
				int			usecs;

				targettime = last_status + (standby_message_timeout - 1) *
					((int64) 1000);
				localTimestampDifference(now,
										 targettime,
										 &secs,
										 &usecs);
				if (secs <= 0)
					timeout.tv_sec = 1; /* Always sleep at least 1 sec */
				else
					timeout.tv_sec = secs;
				timeout.tv_usec = usecs;
				timeoutptr = &timeout;
			}
			else
				timeoutptr = NULL;

			r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
			if (r == 0 || (r < 0 && errno == EINTR))
			{
				/*
				 * Got a timeout or signal. Continue the loop and either
				 * deliver a status packet to the server or just go back into
				 * blocking.
				 */
				continue;
			}
			else if (r < 0)
			{
				fprintf(stderr, _("%s: select() failed: %s\n"),
						progname, strerror(errno));
				goto error;
			}
			/* Else there is actually data on the socket */
			if (PQconsumeInput(conn) == 0)
			{
				fprintf(stderr,
						_("%s: could not receive data from WAL stream: %s"),
						progname, PQerrorMessage(conn));
				goto error;
			}
			continue;
		}
		if (r == -1)
			/* End of copy stream */
			break;
		if (r == -2)
		{
			fprintf(stderr, _("%s: could not read COPY data: %s"),
					progname, PQerrorMessage(conn));
			goto error;
		}

		/* Check the message type. */
		if (copybuf[0] == 'k')
		{
			int		pos;
			bool	replyRequested;

			/*
			 * Parse the keepalive message, enclosed in the CopyData message.
			 * We just check if the server requested a reply, and ignore the
			 * rest.
			 */
			pos = 1;	/* skip msgtype 'k' */
			pos += 8;	/* skip walEnd */
			pos += 8;	/* skip sendTime */

			if (r < pos + 1)
			{
				fprintf(stderr, _("%s: streaming header too small: %d\n"),
						progname, r);
				goto error;
			}
			replyRequested = copybuf[pos];

			/* If the server requested an immediate reply, send one. */
			if (replyRequested)
			{
				now = localGetCurrentTimestamp();
				if (!sendFeedback(conn, logoff, now, false))
					goto error;
				last_status = now;
			}
			continue;
		}
		else if (copybuf[0] != 'w')
		{
			fprintf(stderr, _("%s: unrecognized streaming header: \"%c\"\n"),
					progname, copybuf[0]);
			goto error;
		}


		/*
		 * Read the header of the XLogData message, enclosed in the CopyData
		 * message. We only need the WAL location field (dataStart), the rest
		 * of the header is ignored.
		 */
		hdr_len = 1;	/* msgtype 'w' */
		hdr_len += 8;	/* dataStart */
		hdr_len += 8;	/* walEnd */
		hdr_len += 8;	/* sendTime */
		if (r < hdr_len + 1)
		{
			fprintf(stderr, _("%s: streaming header too small: %d\n"),
					progname, r);
			goto error;
		}

		/* Extract WAL location for this block */
		{
			XLogRecPtr temp = recvint64(&copybuf[1]);
			logoff = Max(temp, logoff);
		}

		if (outfd == -1 && strcmp(outfile, "-") == 0)
		{
			outfd = 1;
		}
		else if (outfd == -1)
		{
			outfd = open(outfile, O_CREAT|O_APPEND|O_WRONLY|PG_BINARY,
						 S_IRUSR | S_IWUSR);
			if (outfd == -1)
			{
				fprintf(stderr,
						_("%s: could not open log file \"%s\": %s\n"),
						progname, outfile, strerror(errno));
				goto error;
			}
		}

		bytes_left = r - hdr_len;
		bytes_written = 0;


		while (bytes_left)
		{
			int ret;

			ret = write(outfd,
						copybuf + hdr_len + bytes_written,
						bytes_left);

			if (ret < 0)
			{
				fprintf(stderr,
						_("%s: could not write %u bytes to log file \"%s\": %s\n"),
						progname, bytes_left, outfile,
						strerror(errno));
				goto error;
			}

			/* Write was successful, advance our position */
			bytes_written += ret;
			bytes_left -= ret;
		}

		if (write(outfd, "\n", 1) != 1)
		{
			fprintf(stderr,
					_("%s: could not write %u bytes to log file \"%s\": %s\n"),
					progname, 1, outfile,
					strerror(errno));
			goto error;
		}
	}

	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr,
				_("%s: unexpected termination of replication stream: %s"),
				progname, PQresultErrorMessage(res));
		goto error;
	}
	PQclear(res);

	if (copybuf != NULL)
		PQfreemem(copybuf);

	if (outfd != -1 && close(outfd) != 0)
		fprintf(stderr, _("%s: could not close file \"%s\": %s\n"),
				progname, outfile, strerror(errno));
	outfd = -1;
error:
	PQfinish(conn);
	conn = NULL;
}

/*
 * When sigint is called, just tell the system to exit at the next possible
 * moment.
 */
#ifndef WIN32

static void
sigint_handler(int signum)
{
	time_to_abort = true;
}
#endif

int
main(int argc, char **argv)
{
	PGresult   *res;
	static struct option long_options[] = {
/* general options */
		{"file", required_argument, NULL, 'f'},
		{"no-loop", no_argument, NULL, 'n'},
		{"verbose", no_argument, NULL, 'v'},
		{"version", no_argument, NULL, 'V'},
		{"help", no_argument, NULL, '?'},
/* connnection options */
		{"database", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
/* replication options */
		{"plugin", required_argument, NULL, 'P'},
		{"status-interval", required_argument, NULL, 's'},
		{"slot", required_argument, NULL, 'S'},
		{"startpos", required_argument, NULL, 'I'},
/* action */
		{"init", no_argument, NULL, 1},
		{"start", no_argument, NULL, 2},
		{"stop", no_argument, NULL, 3},
		{NULL, 0, NULL, 0}
	};
	int			c;
	int			option_index;
	uint32		hi, lo;

	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_receivellog"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		else if (strcmp(argv[1], "-V") == 0 ||
				 strcmp(argv[1], "--version") == 0)
		{
			puts("pg_receivellog (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "f:nvd:h:p:U:wWP:s:S:",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
/* general options */
			case 'f':
				outfile = pg_strdup(optarg);
				break;
			case 'n':
				noloop = 1;
				break;
			case 'v':
				verbose++;
				break;
/* connnection options */
			case 'd':
				dbname = pg_strdup(optarg);
				break;
			case 'h':
				dbhost = pg_strdup(optarg);
				break;
			case 'p':
				if (atoi(optarg) <= 0)
				{
					fprintf(stderr, _("%s: invalid port number \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				dbport = pg_strdup(optarg);
				break;
			case 'U':
				dbuser = pg_strdup(optarg);
				break;
			case 'w':
				dbgetpassword = -1;
				break;
			case 'W':
				dbgetpassword = 1;
				break;
/* replication options */
			case 'P':
				plugin = pg_strdup(optarg);
				break;
			case 's':
				standby_message_timeout = atoi(optarg) * 1000;
				if (standby_message_timeout < 0)
				{
					fprintf(stderr, _("%s: invalid status interval \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				break;
			case 'S':
				slot = pg_strdup(optarg);
				break;
			case 'I':
				if (sscanf(optarg, "%X/%X", &hi, &lo) != 2)
				{
					fprintf(stderr,
							_("%s: could not parse start position \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				startpos = ((uint64) hi) << 32 | lo;
				break;
			case 1:
				do_init_slot = true;
				break;
			case 2:
				do_start_slot = true;
				break;
			case 3:
				do_stop_slot = true;
				break;
/* action */

			default:
				/*
				 * getopt_long already emitted a complaint
				 */
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	/*
	 * Any non-option arguments?
	 */
	if (optind < argc)
	{
		fprintf(stderr,
				_("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Required arguments
	 */
	if (slot == NULL)
	{
		fprintf(stderr, _("%s: no slot specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (!do_stop_slot && outfile == NULL)
	{
		fprintf(stderr, _("%s: no target file specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (!do_stop_slot && dbname == NULL)
	{
		fprintf(stderr, _("%s: no database specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (!do_stop_slot && !do_init_slot && !do_start_slot)
	{
		fprintf(stderr, _("%s: at least one action needs to be specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);

	}

	if (do_stop_slot && (do_init_slot || do_start_slot))
	{
		fprintf(stderr, _("%s: --stop cannot be combined with --init or --start\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);

	}
#ifndef WIN32
	pqsignal(SIGINT, sigint_handler);
#endif


	/*
	 * don't really need this but it actually helps to get more precise error
	 * messages about authentication and such.
	 */
	{
		conn = GetConnection();
		if (!conn)
			/* Error message already written in GetConnection() */
			exit(1);

		/*
		 * Run IDENTIFY_SYSTEM so we can get the timeline and current xlog
		 * position.
		 */
		res = PQexec(conn, "IDENTIFY_SYSTEM");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, _("%s: could not send replication command \"%s\": %s"),
					progname, "IDENTIFY_SYSTEM", PQerrorMessage(conn));
			disconnect_and_exit(1);
		}

		if (PQntuples(res) != 1 || PQnfields(res) != 4)
		{
			fprintf(stderr,
					_("%s: could not identify system: got %d rows and %d fields, expected %d rows and %d fields\n"),
					progname, PQntuples(res), PQnfields(res), 1, 4);
			disconnect_and_exit(1);
		}
		PQclear(res);
	}


	/*
	 * stop a replication slot
	 */
	if (do_stop_slot)
	{
		char		query[256];

		snprintf(query, sizeof(query), "FREE_LOGICAL_REPLICATION \"%s\"",
				 slot);
		res = PQexec(conn, query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			fprintf(stderr, _("%s: could not send replication command \"%s\": %s"),
				progname, query, PQerrorMessage(conn));
			disconnect_and_exit(1);
		}

		if (PQntuples(res) != 0 || PQnfields(res) != 0)
		{
			fprintf(stderr,
					_("%s: could not stop logical rep: got %d rows and %d fields, expected %d rows and %d fields\n"),
					progname, PQntuples(res), PQnfields(res), 0, 0);
			disconnect_and_exit(1);
		}

		PQclear(res);
		disconnect_and_exit(0);
	}

	/*
	 * init a replication slot
	 */
	if (do_init_slot)
	{
		char		query[256];

		if (verbose)
			fprintf(stderr,
					_("%s: init replication slot\n"),
					progname);

		snprintf(query, sizeof(query), "INIT_LOGICAL_REPLICATION \"%s\" \"%s\"",
				 slot, plugin);

		res = PQexec(conn, query);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, _("%s: could not send replication command \"%s\": %s"),
				progname, query, PQerrorMessage(conn));
			disconnect_and_exit(1);
		}

		if (PQntuples(res) != 1 || PQnfields(res) != 4)
		{
			fprintf(stderr,
					_("%s: could not init logical rep: got %d rows and %d fields, expected %d rows and %d fields\n"),
					progname, PQntuples(res), PQnfields(res), 1, 4);
			disconnect_and_exit(1);
		}

		if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &hi, &lo) != 2)
		{
			fprintf(stderr,
					_("%s: could not parse log location \"%s\"\n"),
					progname, PQgetvalue(res, 0, 1));
			disconnect_and_exit(1);
		}
		startpos = ((uint64) hi) << 32 | lo;

		slot = strdup(PQgetvalue(res, 0, 0));
		PQclear(res);
	}


	if (!do_start_slot)
		disconnect_and_exit(0);

	while (true)
	{
		StreamLog();
		if (time_to_abort)
		{
			/*
			 * We've been Ctrl-C'ed. That's not an error, so exit without an
			 * errorcode.
			 */
			exit(0);
		}
		else if (noloop)
		{
			fprintf(stderr, _("%s: disconnected.\n"), progname);
			exit(1);
		}
		else
		{
			fprintf(stderr,
					/* translator: check source for value for %d */
					_("%s: disconnected. Waiting %d seconds to try again.\n"),
					progname, RECONNECT_SLEEP_TIME);
			pg_usleep(RECONNECT_SLEEP_TIME * 1000000);
		}
	}
}
