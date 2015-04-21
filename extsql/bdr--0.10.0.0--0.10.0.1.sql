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


DO $$
BEGIN
	IF NOT EXISTS (
	    SELECT 1
	      FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
	     WHERE c.relname = 'bdr_nodes_node_name'
	       AND n.nspname = 'bdr'
	       AND c.relkind = 'i'
	) THEN
		-- make sure node names are unique, renaming as few nodes as possible
		WITH nodes_to_rename AS (
			SELECT node_sysid, node_timeline, node_dboid FROM (
				SELECT node_sysid, node_timeline, node_dboid,
				ROW_NUMBER() OVER(PARTITION BY node_name ORDER BY node_sysid, node_timeline, node_dboid) rownum
				FROM bdr.bdr_nodes
				WHERE node_name IS NOT NULL
			) dups
			WHERE
			dups.rownum > 1
			UNION
			SELECT node_sysid, node_timeline, node_dboid
			FROM bdr.bdr_nodes
			WHERE node_name IS NULL
		)
		UPDATE bdr.bdr_nodes SET node_name = r.node_sysid || '_' || r.node_timeline || '_' || r.node_dboid
		FROM nodes_to_rename r
		WHERE bdr_nodes.node_sysid = r.node_sysid
		  AND bdr_nodes.node_timeline = r.node_timeline
		  AND bdr_nodes.node_dboid = r.node_dboid;

		-- add constrains ensuring node_names are unique and not null
		ALTER TABLE bdr.bdr_nodes ALTER COLUMN node_name SET NOT NULL;

		CREATE UNIQUE INDEX bdr_nodes_node_name
		ON bdr.bdr_nodes(node_name);
	END IF;
END;$$;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
