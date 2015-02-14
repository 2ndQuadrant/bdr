--
-- DEPARSE_INIT
--
CREATE SCHEMA deparse;
CREATE TABLE deparse.deparse_test_commands (
  lsn pg_lsn,
  ord integer,
  command TEXT
);
CREATE OR REPLACE FUNCTION deparse.deparse_test_ddl_command_end()
  RETURNS event_trigger
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $fn$
BEGIN
	BEGIN
		INSERT INTO deparse.deparse_test_commands (command, ord, lsn)
		SELECT pg_event_trigger_expand_command(command), ordinality, lsn
		FROM pg_event_trigger_get_creation_commands() WITH ORDINALITY,
		pg_current_xlog_insert_location() lsn;
	EXCEPTION WHEN OTHERS THEN 
			RAISE WARNING 'state: % errm: %', sqlstate, sqlerrm;
	END;
END;
$fn$;

CREATE OR REPLACE FUNCTION deparse.deparse_test_sql_drop()
  RETURNS event_trigger
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $fn$
DECLARE
fmt	TEXT;
obj RECORD;
i	integer = 1;
BEGIN
	FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
		IF NOT obj.original THEN
			CONTINUE;
		END IF;

		IF obj.object_type = 'table column' OR obj.object_type = 'foreign table column' THEN
			fmt = format('ALTER TABLE %I.%I DROP COLUMN %I CASCADE',
				obj.address_names[1],
				obj.address_names[2],
				obj.address_names[3]);
		ELSIF obj.object_type = 'composite type column' THEN
			fmt = format('ALTER TYPE %I.%I DROP ATTRIBUTE %I CASCADE',
				obj.address_names[1],
				obj.address_names[2],
				obj.address_names[3]);
		ELSIF obj.object_type = 'table constraint' THEN
			fmt = format('ALTER TABLE %I.%I DROP CONSTRAINT %I CASCADE',
				obj.address_names[1],
				obj.address_names[2],
				obj.address_names[3]);
		ELSIF obj.object_type = 'domain constraint' THEN
			fmt = format('ALTER DOMAIN %s DROP CONSTRAINT %I CASCADE',
				obj.address_names[1],
				obj.address_args[1]);
		ELSIF obj.object_type = 'default value' THEN
			fmt = format('ALTER TABLE %I.%I ALTER COLUMN %I DROP DEFAULT',
				obj.address_names[1],
				obj.address_names[2],
				obj.address_names[3]);
		ELSIF obj.object_type = 'foreign-data wrapper' THEN
			fmt = format('DROP FOREIGN DATA WRAPPER %s CASCADE',
				obj.object_identity);
		ELSE
			fmt = format('DROP %s %s CASCADE',
				obj.object_type, obj.object_identity);
		END IF;

		INSERT INTO deparse.deparse_test_commands (lsn, ord, command)
		     VALUES (pg_current_xlog_insert_location(), i, fmt);
		i := i + 1;
	END LOOP;
END;
$fn$;

CREATE EVENT TRIGGER deparse_test_trg_sql_drop
  ON sql_drop
  EXECUTE PROCEDURE deparse.deparse_test_sql_drop();

CREATE EVENT TRIGGER deparse_test_trg_ddl_command_end
  ON ddl_command_end
  EXECUTE PROCEDURE deparse.deparse_test_ddl_command_end();
