#ifndef BDR_REPLICATION_MESSAGE_H
#define BDR_REPLICATION_MESSAGE_H

/*
 * 9.6 got logical WAL messages in commit but of course they have a totally
 * different API to what we had in Postgres-BDR 9.4. BDR now uses the 9.6
 * API and needs an adapter for the 9.4 API.
 *
 * See commits 3fe3511d05 and be65eddd
 *
 * See also replication/message.h and replication/output_plugin.h
 */

/* Logical message writing in 9.4 was in storage/standby.h */
#include "storage/standby.h"

inline static XLogRecPtr
LogLogicalMessage(const char *prefix, const char *message,
				  size_t size, bool transactional)
{
	Assert(strcmp(prefix, "bdr") == 0);
	return LogStandbyMessage(message, size, transactional);
}

#endif
