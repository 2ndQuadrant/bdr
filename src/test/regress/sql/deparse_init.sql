--
-- DEPARSE_INIT
--
CREATE SCHEMA deparse;
UPDATE pg_namespace SET nspname = 'pg_deparse' WHERE nspname = 'deparse';
CREATE UNLOGGED TABLE pg_deparse.deparse_test_commands (
  backend_id int,
  backend_start timestamptz,
  lsn pg_lsn,
  ord integer,
  command TEXT
);
CREATE OR REPLACE FUNCTION pg_deparse.deparse_test_ddl_command_end()
  RETURNS event_trigger
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $fn$
BEGIN
	BEGIN
		INSERT INTO pg_deparse.deparse_test_commands
		            (backend_id, backend_start, command, ord, lsn)
		SELECT id, pg_stat_get_backend_start(id),
		     pg_event_trigger_expand_command(command), ordinality, lsn
		FROM pg_event_trigger_get_creation_commands() WITH ORDINALITY,
		pg_current_xlog_insert_location() lsn,
		pg_stat_get_backend_idset() id
		 WHERE pg_stat_get_backend_pid(id) = pg_backend_pid();
	EXCEPTION WHEN OTHERS THEN 
			RAISE WARNING 'state: % errm: %', sqlstate, sqlerrm;
	END;
END;
$fn$;

CREATE OR REPLACE FUNCTION pg_deparse.deparse_test_sql_drop()
  RETURNS event_trigger
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $fn$
DECLARE
fmt	TEXT;
obj RECORD;
i	integer = 1;
BEGIN

	/* This function runs in the sql_drop event trigger.
     *
	 * When it runs, we know that all objects reported by the
	 * pg_event_trigger_dropped_objects() function marked as "original" have
	 * been mentioned in the DROP command, either directly by name or
	 * indirectly by owner (DROP OWNED BY).  Since no objects that depend on
	 * them can persist after that, we can replicate the effect of that by
	 * executing an equivalent "DROP IF EXISTS object ... CASCADE".  CASCADE
	 * lets the deletion work even in presence of objects that appear further
	 * down in the return set of pg_event_trigger_dropped_objects, while IF
	 * EXISTS let the deletion silently do nothing if the object was already
	 * dropped because it was dependent on another object before it in the same
	 * result set.
     *
     * (In general, it is impossible to reorder the result set in a way that
     * would be completely free of dependency issues.)
     */

	FOR obj IN
	SELECT object_type, address_names, address_args, object_identity
	  FROM pg_event_trigger_dropped_objects()
	 WHERE original
	  LOOP

		-- special case for default acls: ignore them.
		IF obj.object_type = 'default acl' THEN
			CONTINUE;
		END IF;

		/*
		 * special cases for objects that are part of other objects: drop
		 * each in a separate command.  Since we only deal with "original"
		 * objects, these would not be reported in the complex case of
		 * DROP OWNED.
		 */
		IF obj.object_type = 'table column' OR obj.object_type = 'foreign table column' THEN
			fmt = format('ALTER TABLE %I.%I DROP COLUMN %I CASCADE',
				obj.address_names[1],
				obj.address_names[2],
				obj.address_names[3]);
			-- ignore these; they are output by ALTER TABLE itself
			fmt := NULL;
		ELSIF obj.object_type = 'composite type column' THEN
			fmt = format('ALTER TYPE %I.%I DROP ATTRIBUTE %I CASCADE',
				obj.address_names[1],
				obj.address_names[2],
				obj.address_names[3]);
			-- ignore these; they are output by ALTER TYPE itself
			fmt := NULL;
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
			fmt = format('DROP FOREIGN DATA WRAPPER IF EXISTS %s CASCADE',
				obj.object_identity);
		ELSIF obj.object_type = 'user mapping' THEN
			fmt = format('DROP USER MAPPING FOR %I SERVER %I',
				obj.address_names[1], obj.address_args[1]);
		ELSIF obj.object_type = 'operator of access method' THEN
			fmt = format('ALTER OPERATOR FAMILY %I.%I USING %I DROP OPERATOR %s (%s, %s)',
				obj.address_names[2], obj.address_names[3], obj.address_names[1], obj.address_names[4],
				obj.address_args[1], obj.address_args[2]);
			-- ignore these; they are output by ALTER OPERATOR FAMILY itself
			fmt := NULL;
		ELSIF obj.object_type = 'function of access method' THEN
			fmt = format('ALTER OPERATOR FAMILY %I.%I USING %I DROP FUNCTION %s (%s, %s)',
				obj.address_names[2], obj.address_names[3], obj.address_names[1], obj.address_names[4],
				obj.address_args[1], obj.address_args[2]);
			-- ignore these; they are output by ALTER OPERATOR FAMILY itself
			fmt := NULL;
		ELSE
			-- all other cases
			fmt := format('DROP %s IF EXISTS %s CASCADE',
				obj.object_type, obj.object_identity);
		END IF;

		IF fmt IS NULL THEN
			CONTINUE;
		END IF;

		fmt := fmt || ' /* DROP support */';

		INSERT INTO pg_deparse.deparse_test_commands
		            (backend_id, backend_start, lsn, ord, command)
			 SELECT id, pg_stat_get_backend_start(id),
			        pg_current_xlog_insert_location(), i, fmt
			   FROM pg_stat_get_backend_idset() id
			  WHERE pg_stat_get_backend_pid(id) = pg_backend_pid();
		i := i + 1;
	END LOOP;
END;
$fn$;

CREATE OR REPLACE FUNCTION pg_deparse.output_commands() RETURNS SETOF text LANGUAGE PLPGSQL AS $$
DECLARE
        cmd text;
        prev_id int = -1;
        prev_start timestamptz = '-infinity';
        sess_id int;
        sess_start timestamptz;
BEGIN
   FOR cmd, sess_id, sess_start IN
			   SELECT command, backend_id, backend_start
                 FROM pg_deparse.deparse_test_commands
			 ORDER BY lsn, ord
   LOOP
          IF (sess_id, sess_start) <> (prev_id, prev_start) THEN
                prev_id := sess_id;
                prev_start := sess_start;
                RETURN NEXT '\c';
          END IF;
      RETURN NEXT cmd || ';' ;
   END LOOP;
END;
$$;

CREATE EVENT TRIGGER deparse_test_trg_sql_drop
  ON sql_drop
  EXECUTE PROCEDURE pg_deparse.deparse_test_sql_drop();

CREATE EVENT TRIGGER deparse_test_trg_ddl_command_end
  ON ddl_command_end
  EXECUTE PROCEDURE pg_deparse.deparse_test_ddl_command_end();
