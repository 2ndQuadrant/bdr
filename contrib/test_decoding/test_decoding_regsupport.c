/*-------------------------------------------------------------------------
 *
 * test_decoding_regsupport.c
 *		  regression test functions
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/test_decoding/test_decoding_regsupport.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


PG_MODULE_MAGIC;

Datum		pg_td_mkdir(PG_FUNCTION_ARGS);
Datum		pg_td_rmdir(PG_FUNCTION_ARGS);
Datum		pg_td_unlink(PG_FUNCTION_ARGS);
Datum		pg_td_chmod(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_td_mkdir);
PG_FUNCTION_INFO_V1(pg_td_rmdir);
PG_FUNCTION_INFO_V1(pg_td_unlink);
PG_FUNCTION_INFO_V1(pg_td_chmod);


Datum
pg_td_mkdir(PG_FUNCTION_ARGS)
{
	text	   *dirname_t = PG_GETARG_TEXT_P(0);
	const char *dirname;

	dirname = text_to_cstring(dirname_t);

	if (mkdir(dirname, S_IRWXU) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m",
						dirname)));

	PG_RETURN_VOID();
}

Datum
pg_td_rmdir(PG_FUNCTION_ARGS)
{
	text	   *dirname_t = PG_GETARG_TEXT_P(0);
	const char *dirname;

	dirname = text_to_cstring(dirname_t);

	if (rmdir(dirname) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\": %m",
						dirname)));

	PG_RETURN_VOID();
}

Datum
pg_td_unlink(PG_FUNCTION_ARGS)
{
	text	   *filename_t = PG_GETARG_TEXT_P(0);
	const char *filename;

	filename = text_to_cstring(filename_t);

	if (unlink(filename) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not unlink \"%s\": %m",
						filename)));

	PG_RETURN_VOID();
}


Datum
pg_td_chmod(PG_FUNCTION_ARGS)
{
	text	   *fname_t = PG_GETARG_TEXT_P(0);
	text	   *modes_t = PG_GETARG_TEXT_P(1);
	const char *fname;
	const char *modes;
	mode_t mode;

	fname = text_to_cstring(fname_t);
	modes = text_to_cstring(modes_t);

	errno = 0;
	mode = (mode_t) strtoul(modes, NULL, 8);

	if (errno != 0)
		ereport(ERROR,
				(errmsg("could not parse mode \"%s\": %m",
						modes)));

	if (chmod(fname, mode) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not chmod(\"%s\", %x): %m",
						fname, mode)));

	PG_RETURN_VOID();
}
