DO $$BEGIN
	CREATE FUNCTION pg_catalog.pg_replication_identifier_drop(text) RETURNS void LANGUAGE internal AS 'pg_replication_identifier_drop';
	RETURN;
EXCEPTION WHEN duplicate_function THEN
	RETURN;
END$$;
