#ifndef BDR_COMPAT_ACCESS_COMMITTS_H
#define BDR_COMPAT_ACCESS_COMMITTS_H

#include "access/committs.h"

/*
 * Compatibility wrapper to adapt commit timestamps as implemented
 * in 9.4bdr to what got committed in 9.5. New type names, etc.
 */

/*
 * Internal var for GUC renamed to match external GUC name in 9.5
 * commit ts.
 */
#define track_commit_timestamp commit_ts_enabled 

/*
 * 9.4bdr's CommitExtraData, a uint32, got
 * replaced with explicit use of RepOriginId, a uint16.
 *
 * They're not interchangeable; we can't safely pass
 * a uint16* to something expecting uint32*. So adapters
 * are needed.
 */
#define TransactionIdGetCommitTsData(xid, ts, nodeid) \
({ \
	CommitExtraData data; \
	TransactionIdGetCommitTsData(xid, ts, &data); \
    *nodeid = (RepOriginId)data; \
})

#endif
