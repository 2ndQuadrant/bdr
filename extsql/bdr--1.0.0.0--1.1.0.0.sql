--\echo Use "CREATE EXTENSION bdr" to load this file. \quit
--
-- This extension script adds compatibility for 9.6 DDL replication
--

SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SET LOCAL bdr.skip_ddl_replication = true;
SET LOCAL search_path = bdr;

ALTER TABLE bdr_queued_commands
  ADD COLUMN search_path TEXT;

UPDATE bdr_queued_commands
SET search_path = '';

ALTER TABLE bdr_queued_commands
  ALTER COLUMN search_path SET DEFAULT '';

ALTER TABLE bdr_queued_commands
  ALTER COLUMN search_path SET NOT NULL;

--
-- DDL replication limitations in 9.6bdr mean that we can't directly
-- EXECUTE ddl in function bodies, and must use bdr.bdr_replicate_ddl_command
-- with a fully qualified relation name.
--
CREATE OR REPLACE FUNCTION bdr.table_set_replication_sets(p_relation regclass, p_sets text[])
  RETURNS void
  VOLATILE
  LANGUAGE 'plpgsql'
  SET bdr.permit_unsafe_ddl_commands = true
  SET search_path = ''
  AS $$
DECLARE
    v_label json;
BEGIN
    -- emulate STRICT for p_relation parameter
    IF p_relation IS NULL THEN
        RETURN;
    END IF;

    -- query current label
    SELECT label::json INTO v_label
    FROM pg_catalog.pg_seclabel
    WHERE provider = 'bdr'
        AND classoid = 'pg_class'::regclass
        AND objoid = p_relation;

    -- replace old 'sets' parameter with new value
    SELECT json_object_agg(key, value) INTO v_label
    FROM (
        SELECT key, value
        FROM json_each(v_label)
        WHERE key <> 'sets'
      UNION ALL
        SELECT
            'sets', to_json(p_sets)
        WHERE p_sets IS NOT NULL
    ) d;

    -- and now set the appropriate label
    PERFORM bdr.bdr_replicate_ddl_command(format('SECURITY LABEL FOR bdr ON TABLE %s IS %L', p_relation, v_label)) ;
END;
$$;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
