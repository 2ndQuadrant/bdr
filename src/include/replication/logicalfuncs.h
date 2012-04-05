/*-------------------------------------------------------------------------
 * logicalfuncs.h
 *     PostgreSQL WAL to logical transformation support functions
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALFUNCS_H
#define LOGICALFUNCS_H

extern Datum pg_stat_get_logical_replication_slots(PG_FUNCTION_ARGS);

#endif
