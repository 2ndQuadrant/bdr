#ifndef BDR_STORAGE_LWLOCK_H
#define BDR_STORAGE_LWLOCK_H

#include_next "storage/lwlock.h"

/*
 * The LWLock API for extensions was changed in c1772ad92 (9.5). We need
 * adapters to allow the new API to be used in code built against 9.4.
 *
 * This is a simplistic adapter that expects one LWLock per tranche and
 * makes no attempt to prevent re-use of tranche names, assignment of
 * locks from unallocated tranches, etc. If the code is right on 9.6
 * it'll work on 9.4 though.
 */
static inline LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name)
{
    LWLock     *lock = LWLockAssign();

    return (LWLockPadded *)lock;
}

static inline void
RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks)
{
    Assert(num_lwlocks == 1);

    RequestAddinLWLocks(num_lwlocks);
}

#endif
