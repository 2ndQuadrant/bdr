/*-------------------------------------------------------------------------
 *
 * bdr_shmem.c
 * 		BDR message handling using consensus manager and message broker
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_shmem.c
 *
 * This file contains functionality related to BDR shared memory management.
 *
 * BDR has its own shared memory, separate to pglogical's, that is used for
 * inter-backend message queues, etc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "miscadmin.h"

#include "pgstat.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/shmem.h"

#include "bdr_shmem.h"

BdrShmemContext *bdr_ctx = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Max local node count used only during shmem setup
 * (after which use the copy in BdrShmemContext)
 */
static int max_local_nodes = -1;

static Size
bdr_calc_shmem_size(void)
{
	Assert(max_local_nodes > 0);
	return offsetof(BdrShmemContext, lock)
		   + (sizeof(BdrManagerShmem) * max_local_nodes);
}

BdrManagerShmem*
bdr_shmem_lookup_manager_segment(uint32 node_id, bool missing_ok)
{
	int i;
	BdrManagerShmem *seg = NULL;
	LWLockAcquire(bdr_ctx->lock, LW_SHARED);
	for (i = 0; i < bdr_ctx->max_local_nodes; i++)
	{
		if (bdr_ctx->managers[i].node_id == node_id)
		{
			seg = &bdr_ctx->managers[i];
			break;
		}
	}
	LWLockRelease(bdr_ctx->lock);

	if (seg == NULL && !missing_ok)
		elog(ERROR, "could not find bdr manager of node %u in shared memory", node_id);

	return seg;
}

BdrManagerShmem*
bdr_shmem_allocate_manager_segment(uint32 node_id)
{
	int i;
	int first_free_idx = -1, found_existing = -1;
	BdrManagerShmem *seg = NULL;
	LWLockAcquire(bdr_ctx->lock, LW_EXCLUSIVE);
	for (i = 0; i < bdr_ctx->max_local_nodes; i++)
	{
		if (first_free_idx == -1 && bdr_ctx->managers[i].node_id == 0)
			first_free_idx = i;
		/* existing entry */
		if (bdr_ctx->managers[i].node_id == node_id)
		{
			found_existing = i;
			break;
		}
	}
	if (found_existing == -1 && first_free_idx != -1)
	{
		/* claim while still locked */
		seg = &bdr_ctx->managers[first_free_idx];
		seg->node_id = node_id;
		seg->manager = MyProc;
	}
	LWLockRelease(bdr_ctx->lock);

	if (found_existing != -1)
		elog(ERROR, "bdr manager for %u already registered", node_id);

	if (first_free_idx == -1)
		elog(ERROR, "no free shmem slots for bdr manager %u", node_id);

	/* We're the only proc who should be releasing the seg, so */
	Assert(seg == &bdr_ctx->managers[first_free_idx] && seg->node_id == node_id && seg->manager == MyProc);
	return seg;
}

void
bdr_shmem_release_manager_segment(BdrManagerShmem *seg)
{
	int i;
	bool found = false;

	if (seg == NULL)
		return;

	if (seg->manager != NULL && seg->manager->pid != MyProcPid)
	{
		/* shouldn't happen */
		elog(WARNING, "releasing manager for %u from out-of-process; seg manager is %d but I am %d",
			 seg->node_id, seg->manager->pid, MyProcPid);
	}

	/*
	 * Even though we own this segment and should be able to release
	 * it without fuss, we need strict memory ordering so someone
	 * else doesn't start acquiring it while we're still releasing it.
	 *
	 * We could clobber their writes.
	 */
	LWLockAcquire(bdr_ctx->lock, LW_EXCLUSIVE);
	for (i = 0; i < bdr_ctx->max_local_nodes; i++)
	{
		if (bdr_ctx->managers[i].node_id == seg->node_id)
		{
			Assert (seg == &bdr_ctx->managers[i]);
			seg->node_id = 0;
			seg->manager = NULL;
			found = true;
			break;
		}
	}
	LWLockRelease(bdr_ctx->lock);

	Assert(found);
}

static void
bdr_shmem_startup(void)
{
	bool        found;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	bdr_ctx = ShmemInitStruct("bdr",
							   bdr_calc_shmem_size(), &found);

	if (!found)
	{
		memset(bdr_ctx, 0, bdr_calc_shmem_size());
		bdr_ctx->lock = &(GetNamedLWLockTranche("bdr"))->lock;
		bdr_ctx->max_local_nodes = max_local_nodes;
	}
}

void
bdr_shmem_init(int maxnodes)
{
	Assert(process_shared_preload_libraries_in_progress);

	max_local_nodes = maxnodes;

	/* Allocate enough shmem for the worker limit ... */
	RequestAddinShmemSpace(bdr_calc_shmem_size());

	/*
	 * We'll need to be able to take exclusive locks so only one per-db backend
	 * tries to allocate or free message queue dsm handle entries.
	 */
	RequestNamedLWLockTranche("bdr", 1);

	bdr_ctx = NULL;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = bdr_shmem_startup;
}

/*
 * From a non-manager process, poll shmem until we see the manager process
 * start up.
 *
 * We could avoid polling by having a wait-list in shmem, but it's hardly
 * worth the trouble for something that won't happen much or for long.
 */
void
wait_for_manager_shmem_attach(uint32 myid)
{
	while (bdr_shmem_lookup_manager_segment(myid, true) == NULL)
	{
		int rc;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L, PG_WAIT_EXTENSION);

        ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();
	}
}
