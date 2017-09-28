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

#include "access/xact.h"

#include "utils/int8.h"
#include "utils/memutils.h"

#include "pglogical_worker.h"

#include "bdr_catalogs.h"
#include "bdr_catcache.h"
#include "bdr_state.h"
#include "bdr_version.h"
#include "bdr_worker.h"
#include "bdr_apply.h"

/*
 * Remember we also have MyPGLSubscription for the PGL sub
 */
static BdrSubscription *bdr_sub = NULL;

void
bdr_receiver_writer_start(void)
{
	MemoryContext old_ctx;

	if (!bdr_is_active_db())
		return;

	Assert(!IsTransactionState());
	StartTransactionCommand();
	bdr_cache_local_nodeinfo();
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	bdr_sub = bdr_get_subscription(MyPGLogicalWorker->subid, true);
	(void) MemoryContextSwitchTo(old_ctx);
	CommitTransactionCommand();
}

/*
 * Add BDR-specific messages to the parameters sent to output plugins
 * by pglogical's apply worker and sync worker for processing in
 * bdr_process_output_params(...)
 */
void
bdr_start_replication_params(StringInfo s)
{
	BdrNodeInfo *local;

	local = bdr_get_cached_local_node_info();

	if (bdr_sub == NULL)
		return;

	Assert(bdr_sub->pglogical_subscription_id == MyPGLogicalWorker->subid);

	appendStringInfo(s, ", bdr_version_num '%06d'", BDR_VERSION_NUM);
	appendStringInfo(s, ", bdr_version_str '%s'", BDR_VERSION);
	appendStringInfo(s, ", bdr_node_id '%u'", local->bdr_node->node_id);
	appendStringInfo(s, ", bdr_node_group_id '%u'",
		local->bdr_node_group->id);

	/*
	 * If we're replaying in catchup mode from this peer, we need to tell the
	 * output plugin so it filters in transtions for the whole nodegroup.
	 */
	appendStringInfo(s, ", bdr_subscription_mode '%c'", (char)bdr_sub->mode);
}
