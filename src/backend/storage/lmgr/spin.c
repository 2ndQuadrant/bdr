/*-------------------------------------------------------------------------
 *
 * spin.c
 *	   Hardware-independent implementation of spinlocks.
 *
 *
 * For machines that have test-and-set (TAS) instructions, s_lock.h/.c
 * define the spinlock implementation.  This file contains only a stub
 * implementation for spinlocks using PGSemaphores.  Unless semaphores
 * are implemented in a way that doesn't involve a kernel call, this
 * is too slow to be very useful :-(
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/spin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "replication/walsender.h"
#include "storage/lwlock.h"
#include "storage/pg_sema.h"
#include "storage/spin.h"


#ifndef HAVE_SPINLOCKS
PGSemaphore SpinlockSemaArray;
#endif

/*
 * Report the amount of shared memory needed to store semaphores for spinlock
 * support.
 */
Size
SpinlockSemaSize(void)
{
	return SpinlockSemas() * sizeof(PGSemaphoreData);
}

#ifdef HAVE_SPINLOCKS

/*
 * Report number of semaphores needed to support spinlocks.
 */
int
SpinlockSemas(void)
{
	return 0;
}
#else							/* !HAVE_SPINLOCKS */

/*
 * No TAS, so spinlocks are implemented as PGSemaphores.
 */


/*
 * Report number of semaphores needed to support spinlocks.
 */
int
SpinlockSemas(void)
{
	return NUM_SPINLOCK_SEMAPHORES + NUM_ATOMICS_SEMAPHORES;
}

/*
 * Initialize semaphores.
 */
extern void
SpinlockSemaInit(PGSemaphore spinsemas)
{
	int			i;
	int			nsemas = SpinlockSemas();

	for (i = 0; i < nsemas; ++i)
		PGSemaphoreCreate(&spinsemas[i]);
	SpinlockSemaArray = spinsemas;
}

/*
 * s_lock.h hardware-spinlock emulation
 */

void
s_init_lock_sema(volatile slock_t *lock, bool nested)
{
	static int	counter = 0;

	*lock = (++counter) % NUM_SPINLOCK_SEMAPHORES;
}

void
s_unlock_sema(volatile slock_t *lock)
{
	PGSemaphoreUnlock(&SpinlockSemaArray[*lock]);
}

bool
s_lock_free_sema(volatile slock_t *lock)
{
	/* We don't currently use S_LOCK_FREE anyway */
	elog(ERROR, "spin.c does not support S_LOCK_FREE()");
	return false;
}

int
tas_sema(volatile slock_t *lock)
{
	/* Note that TAS macros return 0 if *success* */
	return !PGSemaphoreTryLock(&SpinlockSemaArray[*lock]);
}

#endif   /* !HAVE_SPINLOCKS */
