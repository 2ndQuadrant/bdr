--
-- DEPARSE_INIT
--
CREATE SCHEMA deparse;
CREATE TABLE deparse.deparse_test_commands (
  id SERIAL PRIMARY KEY,
  command TEXT
);
CREATE FUNCTION deparse.deparse_test_ddl_command_end()
  RETURNS event_trigger
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $fn$
BEGIN
	BEGIN
		INSERT INTO deparse.deparse_test_commands (command)
		SELECT pg_event_trigger_expand_command(command)
		FROM pg_event_trigger_get_creation_commands();
	EXCEPTION WHEN OTHERS THEN 
			RAISE WARNING 'state: % errm: %', sqlstate, sqlerrm;
	END;
END;
$fn$;

CREATE EVENT TRIGGER deparse_test_trg_ddl_command_end
  ON ddl_command_end
  EXECUTE PROCEDURE deparse.deparse_test_ddl_command_end();
