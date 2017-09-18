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

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_output.h"
#include "bdr_state.h"
#include "bdr_version.h"
#include "bdr_worker.h"

static BdrSubscriptionMode sub_mode;

void
bdr_output_start(struct LogicalDecodingContext * ctx, struct OutputPluginOptions *opt)
{
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
		/* end of parameters */
		if (peer_bdr_version_num != -1)
		{
			elog(bdr_debug_level,
				"received connection from bdr node %u nodegroup %u, bdr version %s (%06d)",
				peer_bdr_node_id, peer_bdr_node_group_id,
				peer_bdr_version_str, peer_bdr_version_num);
		}
		else
			elog(DEBUG2, "peer is not a BDR node, BDR inactive on this connection");
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
	else if (strcmp(elem->defname, "bdr_node_id") == 0)
	{
		peer_bdr_node_id = PGL_parse_param_uint32(elem);
	}
	else if (strcmp(elem->defname, "bdr_node_group_id") == 0)
	{
		peer_bdr_node_group_id = PGL_parse_param_uint32(elem);
	}
	else if (strcmp(elem->defname, "bdr_subscription_mode") == 0)
	{
		if (strlen(strVal(elem->arg)) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("bdr_subscription_mode must be a single character, not '%s'",
					 	strVal(elem->arg))));

		sub_mode = (BdrSubscriptionMode)strVal(elem->arg)[0];
	}

	return false;
}
