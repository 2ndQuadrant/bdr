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
