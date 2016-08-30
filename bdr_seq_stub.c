/*
 * Compatibility stubs for when global sequences are disabled,
 * so we can still call the same C functions.
 */

#include "postgres.h"
#include "fmgr.h"

#include "bdr_seq.h"

PGDLLEXPORT Datum bdr_internal_sequence_reset_cache(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bdr_internal_sequence_reset_cache);

Datum
bdr_internal_sequence_reset_cache(PG_FUNCTION_ARGS)
{
	elog(ERROR, "BDR global sequences not available");
}
