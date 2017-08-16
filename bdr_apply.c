/*-------------------------------------------------------------------------
 *
 * bdr_apply.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_apply.c
 *
 * BDR support integration for the pglogical apply process
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/int8.h"

#include "bdr_catcache.h"
#include "bdr_version.h"
#include "bdr_worker.h"
#include "bdr_apply.h"

void
bdr_apply_worker_start(void)
{
	bdr_required_this_conn = bdr_is_active_db();
}

/*
 * Add BDR-specific messages to the parameters sent to output plugins
 * by pglogical's apply worker and sync worker for processing in
 * bdr_process_output_params(...)
 */
void
bdr_start_replication_params(StringInfo s)
{
	if (bdr_required_this_conn)
	{
		appendStringInfo(s, ", bdr_version_num '%06d'", BDR_VERSION_NUM);
		appendStringInfo(s, ", bdr_version_str '%s'", BDR_VERSION);
	}
}

/*
 * Process parameters from the output plugin startup message to determine
 * connected remote BDR version, etc. Handles parameters emitted by
 * bdr_prepare_startup_params(...).
 *
 * (When support for in-core replication is added, this will probably have
 * to be done via direct libpq queries instead, but we'll worry about that
 * when the time comes.)
 */
bool
bdr_handle_startup_param(const char *key, const char *value)
{
	/* If this is a plain pglogical link, butt out */
	if (!bdr_required_this_conn)
		return false;

	if (key == NULL)
	{
		/* All params processed */
		if (PEER_HAS_BDR())
		{
			elog(DEBUG2, "connection is to upstream peer bdr %s (%06d)",
				 peer_bdr_version_str, peer_bdr_version_num);
		}
		else
		{
			/* TODO: info about peer, connection */
			elog(ERROR, "peer does not have BDR plugin enabled but this is a BDR connection");
		}
	}
	else if (strcmp(key, "bdr_version_num") == 0)
	{
		int64 val;
		(void) scanint8(value, false, &val);
		peer_bdr_version_num = (uint32)val;
	}
	else if (strcmp(key, "bdr_version_str") == 0)
	{
		strncpy(peer_bdr_version_str, value, BDR_VERSION_STR_SIZE);
		peer_bdr_version_str[BDR_VERSION_STR_SIZE-1]='\0';
	}

	return false;
}
