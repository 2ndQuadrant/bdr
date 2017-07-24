/*-------------------------------------------------------------------------
 *
 * bdr_output.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_output.c
 *
 * Integration into the pglogical output plugin for BDR support
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/parsenodes.h"

#include "replication/logical.h"

#include "pglogical_output_config.h"

#include "bdr_output.h"
#include "bdr_version.h"
#include "bdr_worker.h"

void
bdr_output_start(struct LogicalDecodingContext * ctx, struct OutputPluginOptions *opt)
{
	/*
	 * Output plugin determines if it's being activated for BDR based on
	 * plugin arguments, so we'll clear this later if the downstream
	 * isn't running BDR.
	 */
	bdr_required_this_conn = true;
}

/*
 * Process arguments to the output plugin, including those added by
 * bdr_start_replication_params(...)
 */
bool
bdr_process_output_params(struct DefElem *elem)
{
	if (elem == NULL)
	{
		if (PEER_HAS_BDR())
		{
			elog(DEBUG2, "received connection from bdr node version %s (%06d)",
				peer_bdr_version_str, peer_bdr_version_num);
		}
		else
		{
			elog(DEBUG2, "peer is not a BDR node, BDR inactive on this connection");
			bdr_required_this_conn = false;
		}
	}
	else if (strcmp(elem->defname, "bdr_version_num") == 0)
	{
		peer_bdr_version_num = PGL_parse_param_uint32(elem);
	}
	else if (strcmp(elem->defname, "bdr_version_num") == 0)
	{
		strncpy(peer_bdr_version_str, strVal(elem), BDR_VERSION_STR_SIZE);
		peer_bdr_version_str[BDR_VERSION_STR_SIZE-1] = '\0';
	}

	return false;
}

/*
 * Add extra startup-message parameters to be sent back to the connected worker,
 * for processing in bdr_handle_startup_param(...)
 */
List*
bdr_prepare_startup_params(List *params)
{
	if (bdr_required_this_conn)
	{
		params = add_startup_msg_i(params, "bdr_version_num", BDR_VERSION_NUM);
		params = add_startup_msg_s(params, "bdr_version_str", BDR_VERSION);
	}
	return params;
}
