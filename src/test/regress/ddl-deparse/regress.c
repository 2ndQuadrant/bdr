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


/*
 * deparse_test_ddl_command_end()
 *
 * Event trigger function to assist with DDL deparse regression testing.
 *
 * At the end of each DDL command, this function retrieves the JSON
 * representation of the command, converts it back to a query, and
 * stores it in a table for later use.
 */

PG_FUNCTION_INFO_V1(deparse_test_ddl_command_end);
Datum
deparse_test_ddl_command_end(PG_FUNCTION_ARGS)
{
	int				  ret, row;
	TupleDesc		  spi_tupdesc;
	const char		 *get_creation_commands;
	const char		 *save_command_text;

	MemoryContext tmpcontext;
	MemoryContext oldcontext;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	get_creation_commands =
		"SELECT command FROM pg_event_trigger_get_creation_commands()";

	save_command_text =
		"INSERT INTO deparse.deparse_test_commands (command) VALUES ($1)";

	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "deparse_test_ddl_command_end temporary context",
									   ALLOCSET_DEFAULT_MINSIZE,
									   ALLOCSET_DEFAULT_INITSIZE,
									   ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	ret = SPI_connect();
	if (ret < 0)
		elog(ERROR, "deparse_test_ddl_command_end: SPI_connect returned %d", ret);

	ret = SPI_execute(get_creation_commands, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "deparse_test_ddl_command_end: SPI_execute returned %d", ret);

	spi_tupdesc = SPI_tuptable->tupdesc;

	for (row = 0; row < SPI_processed; row++)
	{
		HeapTuple  spi_tuple;
		Datum	   json;
		Datum	   command;
		bool	   isnull;
		Oid		   argtypes[1];
		Datum	   values[1];

		spi_tuple = SPI_tuptable->vals[row];

		json = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull);
		command = DirectFunctionCall1(pg_event_trigger_expand_command, json);

		argtypes[0] = TEXTOID;
		values[0] = command;

		ret = SPI_execute_with_args(save_command_text, 1, argtypes,
									values, NULL, false, 0);
	}

	SPI_finish();
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpcontext);

	PG_RETURN_NULL();
}

