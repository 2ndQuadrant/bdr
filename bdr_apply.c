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
bdr_receiver_writer_start(void)
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
