DO $$BEGIN
	CREATE FUNCTION pg_catalog.pg_replication_identifier_drop(text) RETURNS void LANGUAGE internal AS 'pg_replication_identifier_drop';
	RETURN;
EXCEPTION WHEN duplicate_function THEN
	RETURN;
END$$;

BEGIN;
SET LOCAL bdr.skip_ddl_replication = true;
SET LOCAL bdr.permit_unsafe_ddl_commands = true;
ALTER SEQUENCE bdr.bdr_conflict_history_id_seq USING local;
SELECT setval('bdr.bdr_conflict_history_id_seq', (SELECT coalesce(max(conflict_id)+1,1) FROM bdr.bdr_conflict_history));
COMMIT;
