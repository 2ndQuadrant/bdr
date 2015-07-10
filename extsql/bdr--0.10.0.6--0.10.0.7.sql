SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

--
-- When updating BDR existing truncate triggers will not be marked as tgisinternal.
--
-- We now permit updates on the catalogs, so simply update them to be
-- tgisinternal.
--
-- There's no need to replicate this change, since the extension update
-- script runs on each node and the trigger definition tuples are different
-- on each node anyway.
--

DO
$$
DECLARE
    rc integer;
BEGIN
    UPDATE pg_trigger
      SET tgisinternal = 't', tgname = tgname || '_' || oid
    FROM pg_proc p
      INNER JOIN pg_namespace n ON (p.pronamespace = n.oid)
    WHERE pg_trigger.tgfoid = p.oid
      AND tgname = 'truncate_trigger'
      AND p.proname = 'queue_truncate'
      AND n.nspname = 'bdr';

    GET DIAGNOSTICS rc = ROW_COUNT;

    IF rc > 0 THEN
        RAISE LOG 'BDR update: Set % existing truncate_trigger entries to tgisinternal', rc;
    END IF;

END;
$$
LANGUAGE plpgsql;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
