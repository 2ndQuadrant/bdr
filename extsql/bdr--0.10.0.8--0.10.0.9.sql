SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

-- Define an alias of pg_catalog.pg_get_replication_slots to expose the extra
-- pid and confirmed_flush_lsn columns from 9.5 and 9.6 that have been
-- backported to 94bdr. The new columns MUST be last.
--
CREATE OR REPLACE FUNCTION
bdr.pg_get_replication_slots(OUT slot_name name, OUT plugin name, OUT slot_type text, OUT datoid oid, OUT active boolean, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn pg_lsn, OUT pid integer, OUT confirmed_flush_lsn pg_lsn)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE ROWS 10
AS $function$pg_get_replication_slots$function$;


-- And a replacement pg_replication_slots view, so if you set your search_path
-- appropriately it'll "just work". The view fixes the column ordering to be
-- the same as 9.6.
CREATE VIEW bdr.pg_replication_slots AS
    SELECT
            L.slot_name,
            L.plugin,
            L.slot_type,
            L.datoid,
            D.datname AS database,
            L.active,
            L.pid,
            L.xmin,
            L.catalog_xmin,
            L.restart_lsn,
            L.confirmed_flush_lsn
    FROM bdr.pg_get_replication_slots() AS L
            LEFT JOIN pg_catalog.pg_database D ON (L.datoid = D.oid);

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
