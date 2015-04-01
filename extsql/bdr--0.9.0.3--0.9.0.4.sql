-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

---
--- Replication identifier emulation
---
DO $DO$BEGIN
IF bdr.bdr_variant() = 'UDR' THEN

	ALTER TABLE bdr.bdr_replication_identifier RENAME TO bdr_replication_identifier_old;
	CREATE TABLE bdr.bdr_replication_identifier (
		riident oid NOT NULL,
		riname text
	);
	INSERT INTO bdr.bdr_replication_identifier SELECT riident, riname FROM bdr.bdr_replication_identifier_old;

	DROP TABLE bdr.bdr_replication_identifier_old;

	PERFORM pg_catalog.pg_extension_config_dump('bdr_replication_identifier', '');
	CREATE UNIQUE INDEX bdr_replication_identifier_riiident_index ON bdr.bdr_replication_identifier(riident);
	CREATE UNIQUE INDEX bdr_replication_identifier_riname_index ON bdr.bdr_replication_identifier(riname varchar_pattern_ops);

END IF;
END;$DO$;


RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
