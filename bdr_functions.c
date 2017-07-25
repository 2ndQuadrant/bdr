/*-------------------------------------------------------------------------
 *
 * bdr_functions.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_functions.c
 *
 * SQL-callable function interface for BDR
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

#include "bdr_functions.h"

PG_FUNCTION_INFO_V1(bdr_decode_message_payload);

Datum
bdr_decode_message_payload(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unimplemented")));
}


PG_FUNCTION_INFO_V1(bdr_decode_state);

Datum
bdr_decode_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unimplemented")));
}
