/*-------------------------------------------------------------------------
 * logicalfuncs.h
 *	   PostgreSQL WAL to logical transformation support functions
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALFUNCS_H
#define LOGICALFUNCS_H

extern int logical_read_local_xlog_page(XLogReaderState * state,
							 XLogRecPtr targetPagePtr,
							 int reqLen, XLogRecPtr targetRecPtr,
							 char *cur_page, TimeLineID *pageTLI);

extern Datum pg_stat_get_logical_decoding_slots(PG_FUNCTION_ARGS);

#endif
