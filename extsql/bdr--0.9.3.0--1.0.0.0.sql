-- This file exists only to tell the extension infrastructure
-- that no changes are required.
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

DO
LANGUAGE plpgsql
$$
BEGIN
    IF (current_setting('server_version_num')::int / 100) <> 904 THEN
        RAISE EXCEPTION 'This extension script version only supports postgres-bdr 9.4';
    END IF;
END;
$$;

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
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

--
-- set_replication_sets knows what it's doing and needs to be able to
-- replicate SECURITY LABEL commands
--
ALTER FUNCTION bdr.table_set_replication_sets(p_relation regclass, p_sets text[])
SET bdr.permit_unsafe_ddl_commands = true;

CREATE OR REPLACE FUNCTION bdr.bdr_apply_is_paused()
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

-- Read-only node support
--
-- Existing entries are left null.
ALTER TABLE bdr.bdr_nodes ADD COLUMN node_read_only boolean;
ALTER TABLE bdr.bdr_nodes ALTER COLUMN node_read_only SET DEFAULT false;

CREATE FUNCTION bdr.bdr_node_set_read_only(
    node_name text,
    read_only boolean
) RETURNS void LANGUAGE C VOLATILE
AS 'MODULE_PATHNAME';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE OR REPLACE FUNCTION bdr.bdr_drop_remote_slot(sysid text, timeline oid, dboid oid)
RETURNS boolean LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.bdr_get_apply_pid(sysid text, timeline oid, dboid oid)
RETURNS integer LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.bdr_unsubscribe(node_name text, drop_slot boolean DEFAULT true)
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    localid record;
    remoteid record;
    v_node_name alias for node_name;
	v_pid integer;
BEGIN
    -- Concurrency
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    -- Check the node exists
    SELECT node_sysid AS sysid, node_timeline AS timeline,
           node_dboid AS dboid INTO remoteid
    FROM bdr.bdr_nodes
    WHERE bdr_nodes.node_name = v_node_name;

    IF NOT FOUND THEN
        RAISE NOTICE 'Node % not found, nothing done', v_node_name;
        RETURN;
    END IF;

    -- Check the connection is unidirectional
    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    PERFORM 1 FROM bdr_connections
    WHERE conn_sysid = remoteid.sysid
      AND conn_timeline = remoteid.timeline
      AND conn_dboid = remoteid.dboid
      AND conn_origin_sysid = localid.sysid
      AND conn_origin_timeline = localid.timeline
      AND conn_origin_dboid = localid.dboid
      AND conn_is_unidirectional = 't';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unidirectional connection to node % not found',
            v_node_name;
        RETURN;
    END IF;

    -- Mark the provider node as killed
    UPDATE bdr.bdr_nodes
    SET node_status = 'k'
    WHERE bdr_nodes.node_name = v_node_name;

    --
    -- Check what to do with local node based on active connections
    --
    -- Note the logic here is:
    --  - if this is UDR node and there are still live provider nodes, we keep local node active
    --  - if this is UDR node and all provider nodes were killed, local node will be killed as well
    --  - if this is UDR + BDR node there will be connection pointing towards local node so we keep it active
    --
    PERFORM 1
    FROM bdr.bdr_connections c
        JOIN bdr.bdr_nodes n ON (
            conn_sysid = node_sysid AND
            conn_timeline = node_timeline AND
            conn_dboid = node_dboid
        )
    WHERE node_status <> 'k';

    IF NOT FOUND THEN
        UPDATE bdr.bdr_nodes
        SET node_status = 'k'
        FROM bdr.bdr_get_local_nodeid()
        WHERE node_sysid = sysid
            AND node_timeline = timeline
            AND node_dboid = dboid;
    END IF;

	IF drop_slot THEN
		-- Stop the local apply so that slot on remote site can be dropped
		-- The apply won't be able to restart because we have bdr_connections
		-- and bdr_nodes locked exclusively.
		LOOP
			v_pid := bdr.bdr_get_apply_pid(remoteid.sysid, remoteid.timeline, remoteid.dboid);
			IF v_pid IS NULL THEN
				EXIT;
			END IF;

			PERFORM pg_terminate_backend(v_pid);

			PERFORM pg_sleep(0.5);
		END LOOP;

		-- Drop the remote slot
		PERFORM bdr.bdr_drop_remote_slot(remoteid.sysid, remoteid.timeline, remoteid.dboid);
	END IF;

    -- Notify local perdb worker to kill nodes.
    PERFORM bdr.bdr_connections_changed();
END;
$body$;

CREATE OR REPLACE FUNCTION bdr.bdr_part_by_node_names(p_nodes text[])
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    unknown_node_names text := NULL;
    r record;
BEGIN
    -- concurrency
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    -- Ensure we're not running on the node being parted.
    -- We can't safely ensure that the change gets replicated
    -- to peer nodes before we cut off our local connections
    -- if running on the node being parted.
    --
    -- (This restriction can be lifted later if we add
    --  multi-phase negotiated part).
    --
    IF bdr.bdr_get_local_node_name() = ANY(p_nodes) THEN
        RAISE USING
            MESSAGE = 'Cannot part a node from its self',
            DETAIL = 'Attempted to bdr_part_by_node_names(...) on node '||bdr.bdr_get_local_node_name()||' which is one of the nodes being parted',
            HINT = 'You must call call bdr_part_by_node_names on a node that is not being removed',
            ERRCODE = 'object_in_use';
    END IF;

    SELECT
        string_agg(to_remove.remove_node_name, ', ')
    FROM
        bdr.bdr_nodes
        RIGHT JOIN unnest(p_nodes) AS to_remove(remove_node_name)
        ON (bdr_nodes.node_name = to_remove.remove_node_name)
    WHERE bdr_nodes.node_name IS NULL
    INTO unknown_node_names;

    IF unknown_node_names IS NOT NULL THEN
        RAISE USING
            MESSAGE = format('No node(s) named %s found', unknown_node_names),
            ERRCODE = 'no_data_found';
    END IF;

    FOR r IN
        SELECT
            node_name, node_status
        FROM
            bdr.bdr_nodes
            INNER JOIN unnest(p_nodes) AS to_remove(remove_node_name)
            ON (bdr_nodes.node_name = to_remove.remove_node_name)
        WHERE bdr_nodes.node_status <> 'r'
    LOOP
        RAISE WARNING 'Node % is in state % not expected ''r''. Attempting to remove anyway.',
            r.node_name, r.node_status;
    END LOOP;

    UPDATE bdr.bdr_nodes
    SET node_status = 'k'
    WHERE node_name = ANY(p_nodes);

    -- Notify local perdb worker to kill nodes.
    PERFORM bdr.bdr_connections_changed();

    UPDATE bdr.bdr_nodes
    SET node_status = 'k'
    WHERE node_name = ANY(p_nodes);

    -- Notify local perdb worker to kill nodes.
    PERFORM bdr.bdr_connections_changed();

END;
$body$;


RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE FUNCTION bdr.bdr_internal_create_truncate_trigger(regclass)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.bdr_group_create(
    local_node_name text,
    node_external_dsn text,
    node_local_dsn text DEFAULT NULL,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default']
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
	t record;
BEGIN

    -- Prohibit enabling BDR where exclusion constraints exist
    FOR t IN
        SELECT n.nspname, r.relname, c.conname, c.contype
        FROM pg_constraint c
          INNER JOIN pg_namespace n ON c.connamespace = n.oid
          INNER JOIN pg_class r ON c.conrelid = r.oid
          INNER JOIN LATERAL unnest(bdr.table_get_replication_sets(c.conrelid)) rs(rsname) ON (rs.rsname = ANY(replication_sets))
        WHERE c.contype = 'x'
          AND r.relpersistence = 'p'
          AND r.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
    LOOP
        RAISE USING
            MESSAGE = 'BDR can''t be enabled because exclusion constraints exist on persistent tables that are not excluded from replication',
            ERRCODE = 'object_not_in_prerequisite_state',
            DETAIL = format('Table %I.%I has exclusion constraint %I', t.nspname, t.relname, t.conname),
            HINT = 'Drop the exclusion constraint(s), change the table(s) to UNLOGGED if they don''t need to be replicated, or exclude the table(s) from the active replication set(s).';
    END LOOP;

    -- Warn users about secondary unique indexes
    FOR t IN
        SELECT n.nspname, r.relname, c.conname, c.contype
        FROM pg_constraint c
          INNER JOIN pg_namespace n ON c.connamespace = n.oid
          INNER JOIN pg_class r ON c.conrelid = r.oid
          INNER JOIN LATERAL unnest(bdr.table_get_replication_sets(c.conrelid)) rs(rsname) ON (rs.rsname = ANY(replication_sets))
        WHERE c.contype = 'u'
          AND r.relpersistence = 'p'
          AND r.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
    LOOP
        RAISE WARNING USING
            MESSAGE = 'Secondary unique constraint(s) exist on replicated table(s)',
            DETAIL = format('Table %I.%I has secondary unique constraint %I. This may cause unhandled replication conflicts.', t.nspname, t.relname, t.conname),
            HINT = 'Drop the secondary unique constraint(s), change the table(s) to UNLOGGED if they don''t need to be replicated, or exclude the table(s) from the active replication set(s).';
    END LOOP;

    -- Warn users about missing primary keys
    FOR t IN
        SELECT n.nspname, r.relname, c.conname
        FROM pg_class r INNER JOIN pg_namespace n ON r.relnamespace = n.oid
          LEFT OUTER JOIN pg_constraint c ON (c.conrelid = r.oid AND c.contype = 'p')
        WHERE n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
          AND relkind = 'r'
          AND relpersistence = 'p'
          AND c.oid IS NULL
    LOOP
        RAISE WARNING USING
            MESSAGE = format('Table %I.%I has no PRIMARY KEY', t.nspname, t.relname),
            HINT = 'Tables without a PRIMARY KEY cannot be UPDATEd or DELETEd from, only INSERTed into. Add a PRIMARY KEY.';
    END LOOP;

    -- Create ON TRUNCATE triggers for BDR on existing tables
    -- See bdr_truncate_trigger_add for the matching event trigger for tables
    -- created after join.
    --
    -- The triggers may be created already because the bdr event trigger
    -- runs when the bdr extension is created, even if there's no active
    -- bdr connections yet, so tables created after the extension is created
    -- will get the trigger already. So skip tables that have a tg named
    -- 'truncate_trigger' calling proc 'bdr.queue_truncate'.
    FOR t IN
        SELECT r.oid AS relid
        FROM pg_class r
          INNER JOIN pg_namespace n ON (r.relnamespace = n.oid)
          LEFT JOIN pg_trigger tg ON (r.oid = tg.tgrelid AND tgname = 'truncate_trigger')
          LEFT JOIN pg_proc p ON (p.oid = tg.tgfoid AND p.proname = 'queue_truncate')
          LEFT JOIN pg_namespace pn ON (pn.oid = p.pronamespace AND pn.nspname = 'bdr')
        WHERE r.relpersistence = 'p'
          AND r.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
          AND tg.oid IS NULL AND p.oid IS NULL and pn.oid IS NULL
    LOOP
        -- We use a C function here because in addition to trigger creation
        -- we must also mark it tgisinternal.
        PERFORM bdr.bdr_internal_create_truncate_trigger(t.relid);
    END LOOP;

    PERFORM bdr.bdr_group_join(
        local_node_name := local_node_name,
        node_external_dsn := node_external_dsn,
        join_using_dsn := null,
        node_local_dsn := node_local_dsn,
        apply_delay := apply_delay,
        replication_sets := replication_sets);
END;
$body$;

-- Setup that's common to BDR and UDR joins
-- Add a check for bdr_test_replication_connection to the upstream, fixing #81
CREATE OR REPLACE FUNCTION bdr.internal_begin_join(
    caller text, local_node_name text, node_local_dsn text, remote_dsn text,
    remote_sysid OUT text, remote_timeline OUT oid, remote_dboid OUT oid
)
RETURNS record LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    localid RECORD;
    localid_from_dsn RECORD;
    remote_nodeinfo RECORD;
    remote_nodeinfo_r RECORD;
BEGIN
    -- Only one tx can be adding connections
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    -- If there's already an entry for ourselves in bdr.bdr_connections
    -- then we know this node is part of an active BDR group and cannot
    -- be joined to another group. Unidirectional connections are ignored.
    PERFORM 1 FROM bdr_connections
    WHERE conn_sysid = localid.sysid
      AND conn_timeline = localid.timeline
      AND conn_dboid = localid.dboid
      AND (conn_origin_sysid = '0'
           AND conn_origin_timeline = 0
           AND conn_origin_dboid = 0)
      AND conn_is_unidirectional = 'f';

    IF FOUND THEN
        RAISE USING
            MESSAGE = 'This node is already a member of a BDR group',
            HINT = 'Connect to the node you wish to add and run '||caller||' from it instead',
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Validate that the local connection is usable and matches
    -- the node identity of the node we're running on.
    --
    -- For BDR this will NOT check the 'dsn' if 'node_local_dsn'
    -- gets supplied. We don't know if 'dsn' is even valid
    -- for loopback connections and can't assume it is. That'll
    -- get checked later by BDR specific code.
    SELECT * INTO localid_from_dsn
    FROM bdr_get_remote_nodeinfo(node_local_dsn);

    IF localid_from_dsn.sysid <> localid.sysid
        OR localid_from_dsn.timeline <> localid.timeline
        OR localid_from_dsn.dboid <> localid.dboid
    THEN
        RAISE USING
            MESSAGE = 'node identity for local dsn does not match current node',
            DETAIL = format($$The dsn '%s' connects to a node with identity (%s,%s,%s) but the local node is (%s,%s,%s)$$,
                node_local_dsn, localid_from_dsn.sysid, localid_from_dsn.timeline,
                localid_from_dsn.dboid, localid.sysid, localid.timeline, localid.dboid),
            HINT = 'The node_local_dsn (or, for bdr, dsn if node_local_dsn is null) parameter must refer to the node you''re running this function from',
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF NOT localid_from_dsn.is_superuser THEN
        RAISE USING
            MESSAGE = 'local dsn does not have superuser rights',
            DETAIL = format($$The dsn '%s' connects successfully but does not grant superuser rights$$, node_local_dsn),
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Now interrogate the remote node, if specified, and sanity
    -- check its connection too. The discovered node identity is
    -- returned if found.
    --
    -- This will error out if there are issues with the remote
    -- node.
    IF remote_dsn IS NOT NULL THEN
        SELECT * INTO remote_nodeinfo
        FROM bdr_get_remote_nodeinfo(remote_dsn);

        remote_sysid := remote_nodeinfo.sysid;
        remote_timeline := remote_nodeinfo.timeline;
        remote_dboid := remote_nodeinfo.dboid;

        IF NOT remote_nodeinfo.is_superuser THEN
            RAISE USING
                MESSAGE = 'connection to remote node does not have superuser rights',
                DETAIL = format($$The dsn '%s' connects successfully but does not grant superuser rights$$, remote_dsn),
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;

        IF remote_nodeinfo.version_num < bdr_min_remote_version_num() THEN
            RAISE USING
                MESSAGE = 'remote node''s BDR version is too old',
                DETAIL = format($$The dsn '%s' connects successfully but the remote node version %s is less than the required version %s$$,
                    remote_dsn, remote_nodeinfo.version_num, bdr_min_remote_version_num()),
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;

        IF remote_nodeinfo.min_remote_version_num > bdr_version_num() THEN
            RAISE USING
                MESSAGE = 'remote node''s BDR version is too new or this node''s version is too old',
                DETAIL = format($$The dsn '%s' connects successfully but the remote node version %s requires this node to run at least bdr %s, not the current %s$$,
                    remote_dsn, remote_nodeinfo.version_num, remote_nodeinfo.min_remote_version_num,
                    bdr_min_remote_version_num()),
                ERRCODE = 'object_not_in_prerequisite_state';

        END IF;

    END IF;

    -- Verify that we can make a replication connection to the remote node
    -- so that pg_hba.conf issues get caught early.
    IF remote_dsn IS NOT NULL THEN
        -- Returns (sysid, timeline, dboid) on success, else ERRORs
        SELECT * FROM bdr_test_replication_connection(remote_dsn)
        INTO remote_nodeinfo_r;

        IF (remote_nodeinfo_r.sysid, remote_nodeinfo_r.timeline, remote_nodeinfo_r.dboid)
            IS DISTINCT FROM
           (remote_sysid, remote_timeline, remote_dboid)
            AND
           (remote_sysid, remote_timeline, remote_dboid)
            IS DISTINCT FROM
           (NULL, NULL, NULL)
        THEN
            -- This just shouldn't happen, so no fancy error.
            -- The all-NULLs case only arises when we're connecting to a 0.7.x
            -- peer, where we can't get the sysid etc from SQL.
            RAISE USING
                MESSAGE = 'Replication and non-replication connections to remote node reported different node id';
        END IF;

        -- In case they're NULL because of bdr_get_remote_nodeinfo
        -- due to an old upstream
        remote_sysid := remote_nodeinfo_r.sysid;
        remote_timeline := remote_nodeinfo_r.timeline;
        remote_dboid := remote_nodeinfo_r.dboid;

    END IF;

    -- Create local node record if needed
    PERFORM 1 FROM bdr_nodes
    WHERE node_sysid = localid.sysid
      AND node_timeline = localid.timeline
      AND node_dboid = localid.dboid;

    IF NOT FOUND THEN
        INSERT INTO bdr_nodes (
            node_name,
            node_sysid, node_timeline, node_dboid,
            node_status, node_local_dsn, node_init_from_dsn
        ) VALUES (
            local_node_name,
            localid.sysid, localid.timeline, localid.dboid,
            'b', node_local_dsn, remote_dsn
        );
    END IF;

    PERFORM bdr.internal_update_seclabel();
END;
$body$;



RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

DO $$
BEGIN
  IF bdr.bdr_variant() = 'BDR' THEN
    CREATE OR REPLACE FUNCTION bdr.bdr_internal_sequence_reset_cache(seq regclass)
    RETURNS void LANGUAGE c AS 'MODULE_PATHNAME' STRICT;
  END IF;
END$$;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
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
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE OR REPLACE FUNCTION bdr.queue_truncate()
    RETURNS TRIGGER
	LANGUAGE C
	AS 'MODULE_PATHNAME','bdr_queue_truncate';
;

ALTER EVENT TRIGGER bdr_truncate_trigger_add ENABLE ALWAYS;


RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
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
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE OR REPLACE FUNCTION terminate_apply_workers(node_name text)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_apply_workers_byname';

CREATE OR REPLACE FUNCTION terminate_apply_workers(sysid text, timeline oid, dboid oid)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_apply_workers';

CREATE OR REPLACE FUNCTION terminate_walsender_workers(node_name text)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_walsender_workers_byname';

CREATE OR REPLACE FUNCTION terminate_walsender_workers(sysid text, timeline oid, dboid oid)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_walsender_workers';

COMMENT ON FUNCTION terminate_walsender_workers(node_name text)
IS 'terminate walsender workers connected to the named node';

COMMENT ON FUNCTION terminate_apply_workers(node_name text)
IS 'terminate apply workers connected to the named node';

COMMENT ON FUNCTION terminate_walsender_workers(sysid text, timeline oid, dboid oid)
IS 'terminate walsender connected to the node with the given identity';

COMMENT ON FUNCTION terminate_apply_workers(sysid text, timeline oid, dboid oid)
IS 'terminate apply workers connected to the node with the given identity';

CREATE OR REPLACE FUNCTION bdr.skip_changes_upto(from_sysid text,
    from_timeline oid, from_dboid oid, upto_lsn pg_lsn)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME','bdr_skip_changes_upto';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE OR REPLACE FUNCTION bdr.connection_get_replication_sets(
    sysid text, timeline oid, dboid oid,
    origin_sysid text default '0',
    origin_timeline oid default 0,
    origin_dboid oid default 0
)
RETURNS text[]
LANGUAGE plpgsql
AS $$
DECLARE
  found_sets text[];
BEGIN
  SELECT conn_replication_sets
  FROM bdr.bdr_connections
  WHERE conn_sysid = sysid
    AND conn_timeline = timeline
    AND conn_dboid = dboid
    AND conn_origin_sysid = origin_sysid
    AND conn_origin_timeline = origin_timeline
    AND conn_origin_dboid = origin_dboid
  INTO found_sets;

  IF NOT FOUND THEN
    IF origin_timeline <> '0' OR origin_timeline <> 0 OR origin_dboid <> 0 THEN
      RAISE EXCEPTION 'No bdr.bdr_connections entry found from origin (%,%,%) to (%,%,%)',
      	origin_sysid, origin_timeline, origin_dboid, sysid, timeline, dboid;
    ELSE
      RAISE EXCEPTION 'No bdr.bdr_connections entry found for (%,%,%) with default origin (0,0,0)',
      	sysid, timeline, dboid;
    END IF;
  END IF;

  RETURN found_sets;
END;
$$;

CREATE OR REPLACE FUNCTION bdr.connection_set_replication_sets(
    new_replication_sets text[],
    sysid text, timeline oid, dboid oid,
    origin_sysid text default '0',
    origin_timeline oid default 0,
    origin_dboid oid default 0
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE bdr.bdr_connections
  SET conn_replication_sets = new_replication_sets
  WHERE conn_sysid = sysid
    AND conn_timeline = timeline
    AND conn_dboid = dboid
    AND conn_origin_sysid = origin_sysid
    AND conn_origin_timeline = origin_timeline
    AND conn_origin_dboid = origin_dboid;

  IF NOT FOUND THEN
    IF origin_timeline <> '0' OR origin_timeline <> 0 OR origin_dboid <> 0 THEN
      RAISE EXCEPTION 'No bdr.bdr_connections entry found from origin (%,%,%) to (%,%,%)',
      	origin_sysid, origin_timeline, origin_dboid, sysid, timeline, dboid;
    ELSE
      RAISE EXCEPTION 'No bdr.bdr_connections entry found for (%,%,%) with default origin (0,0,0)',
      	sysid, timeline, dboid;
    END IF;
  END IF;

  -- The other nodes will notice the change when they replay the new tuple; we
  -- only have to explicitly notify the local node.
  PERFORM bdr.bdr_connections_changed();
END;
$$;

CREATE OR REPLACE FUNCTION bdr.connection_set_replication_sets(replication_sets text[], target_node_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  sysid text;
  timeline oid;
  dboid oid;
BEGIN
  SELECT node_sysid, node_timeline, node_dboid
  FROM bdr.bdr_nodes
  WHERE node_name = target_node_name
  INTO sysid, timeline, dboid;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No node with name % found in bdr.bdr_nodes',target_node_name;
  END IF;

  IF (
    SELECT count(1)
    FROM bdr.bdr_connections
    WHERE conn_sysid = sysid
      AND conn_timeline = timeline
      AND conn_dboid = dboid
    ) > 1
  THEN
    RAISE WARNING 'There are node-specific override entries for node % in bdr.bdr_connections. Only the default connection''s replication sets will be changed. Use the 6-argument form of this function to change others.';
  END IF;

  PERFORM bdr.connection_set_replication_sets(replication_sets, sysid, timeline, dboid);
END;
$$;

CREATE OR REPLACE FUNCTION bdr.connection_get_replication_sets(target_node_name text)
RETURNS text[]
LANGUAGE plpgsql
AS $$
DECLARE
  sysid text;
  timeline oid;
  dboid oid;
  replication_sets text[];
BEGIN
  SELECT node_sysid, node_timeline, node_dboid
  FROM bdr.bdr_nodes
  WHERE node_name = target_node_name
  INTO sysid, timeline, dboid;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No node with name % found in bdr.bdr_nodes',target_node_name;
  END IF;

  IF (
    SELECT count(1)
    FROM bdr.bdr_connections
    WHERE conn_sysid = sysid
      AND conn_timeline = timeline
      AND conn_dboid = dboid
    ) > 1
  THEN
    RAISE WARNING 'There are node-specific override entries for node % in bdr.bdr_connections. Only the default connection''s replication sets will be returned.';
  END IF;

  SELECT bdr.connection_get_replication_sets(sysid, timeline, dboid) INTO replication_sets;
  RETURN replication_sets;
END;
$$;

CREATE FUNCTION bdr._test_pause_worker_management(boolean)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME','bdr_pause_worker_management';

COMMENT ON FUNCTION bdr._test_pause_worker_management(boolean)
IS 'BDR-internal function for test use only';

CREATE FUNCTION bdr.bdr_parse_replident_name(
    replident text,
    remote_sysid OUT text,
    remote_timeline OUT oid,
    remote_dboid OUT oid,
    local_dboid OUT oid,
    replication_name OUT name
)
RETURNS record
LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_parse_replident_name_sql';

COMMENT ON FUNCTION bdr.bdr_parse_replident_name(text)
IS 'Parse a replication identifier name from the bdr plugin and report the embedded field values';

CREATE FUNCTION bdr.bdr_format_replident_name(
    remote_sysid text,
    remote_timeline oid,
    remote_dboid oid,
    local_dboid oid,
    replication_name name DEFAULT ''
)
RETURNS text
LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_format_replident_name_sql';

COMMENT ON FUNCTION bdr.bdr_format_replident_name(text, oid, oid, oid, name)
IS 'Format a BDR replication identifier name from node identity parameters';

--
-- Completely de-BDR-ize a node
--
CREATE OR REPLACE FUNCTION bdr.remove_bdr_from_local_node(force boolean DEFAULT false, convert_global_sequences boolean DEFAULT true)
RETURNS void
LANGUAGE plpgsql
SET bdr.skip_ddl_locking = on
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET search_path = 'bdr,pg_catalog'
AS $$
DECLARE
  local_node_status "char";
  _seqschema name;
  _seqname name;
  _seqmax bigint;
  _tableoid oid;
  _truncate_tg record;
BEGIN

  SELECT node_status FROM bdr.bdr_nodes WHERE (node_sysid, node_timeline, node_dboid) = bdr.bdr_get_local_nodeid()
  INTO local_node_status;

  IF NOT (local_node_status = 'k' OR local_node_status IS NULL) THEN
    IF force THEN
      RAISE WARNING 'forcing deletion of possibly active BDR node';

      UPDATE bdr.bdr_nodes
      SET node_status = 'k'
      WHERE (node_sysid, node_timeline, node_dboid) = bdr.bdr_get_local_nodeid();

      PERFORM bdr._test_pause_worker_management(false);

      PERFORM pg_sleep(5);

      RAISE NOTICE 'node forced to parted state, now removing';
    ELSE
      RAISE EXCEPTION 'this BDR node might still be active, not removing';
    END IF;
  END IF;

  RAISE NOTICE 'removing BDR from node';

  -- Alter all global sequences to become local sequences.  That alone won't
  -- they're in the right position, since another node might've had numerically
  -- higher global sequence values. So we need to then move it up to the
  -- highest allocated chunk for any node and setval to it.
  IF convert_global_sequences THEN 
    FOR _seqschema, _seqname, _seqmax IN
      SELECT
        n.nspname,
        c.relname,
        (
          SELECT max(upper(seqrange))
          FROM bdr.bdr_sequence_values
          WHERE seqschema = n.nspname
            AND seqname = c.relname
            AND in_use
        ) AS seqmax
      FROM pg_class c
      INNER JOIN pg_namespace n ON (c.relnamespace = n.oid)
      WHERE c.relkind = 'S'
        AND c.relam = (SELECT s.oid FROM pg_seqam s WHERE s.seqamname = 'bdr')
    LOOP
      EXECUTE format('ALTER SEQUENCE %I.%I USING local;', _seqschema, _seqname);
      -- This shouldn't be necessary, see bug #215
      IF _seqmax IS NOT NULL THEN
        EXECUTE format('SELECT setval(%L, $1)', quote_ident(_seqschema)||'.'||quote_ident(_seqname)) USING (_seqmax);
      END IF;
    END LOOP;
  ELSE
    RAISE NOTICE 'global sequences not converted to local; they will not work until a new nodegroup is created';
  END IF;

  -- Strip the database security label
  EXECUTE format('SECURITY LABEL FOR bdr ON DATABASE %I IS NULL', current_database());

  -- Suspend worker management, so when we terminate apply workers and
  -- walsenders they won't get relaunched.
  PERFORM bdr._test_pause_worker_management(true);

  -- Terminate every worker associated with this DB
  PERFORM bdr.terminate_walsender_workers(node_sysid, node_timeline, node_dboid)
  FROM bdr.bdr_nodes
  WHERE (node_sysid, node_timeline, node_dboid) <> bdr.bdr_get_local_nodeid();

  PERFORM bdr.terminate_apply_workers(node_sysid, node_timeline, node_dboid)
  FROM bdr.bdr_nodes
  WHERE (node_sysid, node_timeline, node_dboid) <> bdr.bdr_get_local_nodeid();

  -- Delete all connections and all nodes except the current one
  DELETE FROM bdr.bdr_connections
  WHERE (conn_sysid, conn_timeline, conn_dboid) <> bdr.bdr_get_local_nodeid();

  DELETE FROM bdr.bdr_nodes
  WHERE (node_sysid, node_timeline, node_dboid) <> bdr.bdr_get_local_nodeid();

  -- Let the perdb worker resume work and figure out everything's
  -- going away.
  PERFORM bdr._test_pause_worker_management(false);
  PERFORM bdr.bdr_connections_changed();

  -- Give it a few seconds
  PERFORM pg_sleep(2);

  -- Shut down the perdb worker
  PERFORM pg_terminate_backend(pid)
  FROM pg_stat_activity, bdr.bdr_get_local_nodeid() ni
  WHERE datname = current_database()
    AND application_name = format('bdr: (%s,%s,%s,): perdb', ni.sysid, ni.timeline, ni.dboid);

  -- Clear out the rest of bdr_nodes and bdr_connections
  DELETE FROM bdr.bdr_nodes;
  DELETE FROM bdr.bdr_connections;

  -- Drop peer replication slots for this DB
  PERFORM pg_drop_replication_slot(slot_name)
  FROM pg_catalog.pg_replication_slots,
       bdr.bdr_parse_slot_name(slot_name) ps
  WHERE ps.local_dboid = (select oid from pg_database where datname = current_database());

  -- and replication identifiers
  PERFORM pg_replication_identifier_drop(riname)
  FROM pg_catalog.pg_replication_identifier,
       bdr.bdr_parse_replident_name(riname) pi
  WHERE pi.local_dboid = (select oid from pg_database where datname = current_database());

  -- Strip the security labels we use for replication sets from all the tables
  FOR _tableoid IN
    SELECT objoid
    FROM pg_catalog.pg_seclabel
    INNER JOIN pg_catalog.pg_class ON (pg_seclabel.objoid = pg_class.oid)
    WHERE provider = 'bdr'
      AND classoid = 'pg_catalog.pg_class'::regclass
      AND pg_class.relkind = 'r'
  LOOP
    -- regclass's text out adds quoting and schema qualification if needed
    EXECUTE format('SECURITY LABEL FOR bdr ON TABLE %s IS NULL', _tableoid::regclass);
  END LOOP;

  -- Drop the on-truncate triggers. They'd otherwise get cascade-dropped
  -- when the BDR extension was dropped, but this way the system is clean. We
  -- can't drop ones under the 'bdr' schema.
  FOR _truncate_tg IN
    SELECT
      n.nspname AS tgrelnsp,
      c.relname AS tgrelname,
      t.tgname AS tgname,
      d.objid AS tgobjid,
      d.refobjid AS tgrelid
    FROM pg_depend d
    INNER JOIN pg_class c ON (d.refclassid = 'pg_class'::regclass AND d.refobjid = c.oid)
    INNER JOIN pg_namespace n ON (c.relnamespace = n.oid)
    INNER JOIN pg_trigger t ON (d.classid = 'pg_trigger'::regclass and d.objid = t.oid)
    INNER JOIN pg_depend d2 ON (d.classid = d2.classid AND d.objid = d2.objid)
    WHERE tgname LIKE 'truncate_trigger_%'
      AND d2.refclassid = 'pg_proc'::regclass
      AND d2.refobjid = 'bdr.queue_truncate'::regproc
      AND n.nspname <> 'bdr'
  LOOP
    EXECUTE format('DROP TRIGGER %I ON %I.%I',
         _truncate_tg.tgname, _truncate_tg.tgrelnsp, _truncate_tg.tgrelname);

    -- The trigger' dependency entry will be dangling because of how we
    -- dropped it
    DELETE FROM pg_depend
    WHERE classid = 'pg_trigger'::regclass
      AND objid = _truncate_tg.tgobjid
      AND (refclassid = 'pg_proc'::regclass AND refobjid = 'bdr.queue_truncate'::regproc)
          OR
          (refclassid = 'pg_class'::regclass AND refobjid = _truncate_tg.tgrelid);

  END LOOP;

  -- Delete the other detritus from the extension. The user should really drop it,
  -- but we should try to restore a clean state anyway.
  DELETE FROM bdr.bdr_queued_commands;
  DELETE FROM bdr.bdr_queued_drops;
  DELETE FROM bdr.bdr_global_locks;
  DELETE FROM bdr.bdr_conflict_handlers;
  DELETE FROM bdr.bdr_conflict_history;
  DELETE FROM bdr.bdr_replication_set_config;
  DELETE FROM bdr.bdr_sequence_elections;
  DELETE FROM bdr.bdr_sequence_values;
  DELETE FROM bdr.bdr_votes;

  -- We can't drop the BDR extension, we just need to tell the
  -- user to do that.
  RAISE NOTICE 'BDR removed from this node. You can now DROP EXTENSION bdr and, if this is the last BDR node on this PostgreSQL instance, remove bdr from shared_preload_libraries.';
END;
$$;

REVOKE ALL ON FUNCTION bdr.remove_bdr_from_local_node(boolean, boolean) FROM public;

COMMENT ON FUNCTION bdr.remove_bdr_from_local_node(boolean, boolean)
IS 'Remove all BDR security labels, slots, replication origins, replication sets, etc from the local node, and turn all global sequences into local sequences';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
CREATE FUNCTION bdr.bdr_is_active_in_db()
RETURNS boolean LANGUAGE c
AS 'MODULE_PATHNAME','bdr_is_active_in_db';

DROP VIEW bdr.bdr_node_slots;

CREATE VIEW bdr.bdr_node_slots AS
SELECT n.node_name,
 s.slot_name, s.restart_lsn AS slot_restart_lsn, s.confirmed_flush_lsn AS slot_confirmed_lsn,
 s.active AS walsender_active,
 s.pid AS walsender_pid,
 r.sent_location, r.write_location, r.flush_location, r.replay_location
FROM
 bdr.pg_replication_slots s
 CROSS JOIN LATERAL bdr.bdr_parse_slot_name(s.slot_name) ps(remote_sysid, remote_timeline, remote_dboid, local_dboid, replication_name)
 INNER JOIN bdr.bdr_nodes n ON ((n.node_sysid = ps.remote_sysid) AND (n.node_timeline = ps.remote_timeline) AND (n.node_dboid = ps.remote_dboid))
 LEFT JOIN pg_catalog.pg_stat_replication r ON (r.pid = s.pid)
WHERE ps.local_dboid = (select oid from pg_database where datname = current_database())
  AND s.plugin = 'bdr';

