SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

DO $$
BEGIN
	PERFORM 1
	FROM pg_catalog.pg_attribute
	WHERE attrelid = 'bdr.bdr_conflict_handlers'::pg_catalog.regclass
	  AND NOT attisdropped
	  AND atttypid = 'pg_catalog.regprocedure'::pg_catalog.regtype;

	IF FOUND THEN
		DROP VIEW bdr.bdr_list_conflict_handlers;
		ALTER TABLE bdr.bdr_conflict_handlers ALTER COLUMN ch_fun TYPE text USING (ch_fun::text);
		CREATE VIEW bdr.bdr_list_conflict_handlers(ch_name, ch_type, ch_reloid, ch_fun) AS
		    SELECT ch_name, ch_type, ch_reloid, ch_fun, ch_timeframe
		    FROM bdr.bdr_conflict_handlers;
	END IF;
END; $$;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
