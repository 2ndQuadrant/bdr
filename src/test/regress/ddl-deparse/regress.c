#include "postgres.h"

#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


/*
 * c_function_test()
 *
 * Simple C-language function
 */

PG_FUNCTION_INFO_V1(c_function_test);
Datum
c_function_test(PG_FUNCTION_ARGS)
{
	int32   arg = PG_GETARG_INT32(0);

	PG_RETURN_INT32(arg + 1);
}
