-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

ALTER TABLE bdr.bdr_nodes
  ADD COLUMN node_name text,
  ADD COLUMN node_local_dsn text,
  ADD COLUMN node_init_from_dsn text;

ALTER TABLE bdr.bdr_nodes
  DROP CONSTRAINT bdr_nodes_node_status_check;

ALTER TABLE bdr.bdr_nodes
  ADD CONSTRAINT bdr_nodes_node_status_check
    CHECK (node_status in ('b', 'i', 'c', 'o', 'r', 'k'));

CREATE TABLE bdr_connections (
    conn_sysid text not null,
    conn_timeline oid not null,
    conn_dboid oid not null,  -- This is an oid local to the node_sysid cluster

    -- Wondering why there's no FOREIGN KEY to bdr.bdr_nodes?
    -- bdr.bdr_nodes won't be populated when the bdr.bdr_connections
    -- row gets created on the local node.

    -- These fields may later be used by BDR to override connection
    -- settings from one node to a particular other node. At the
    -- moment their main use is for UDR connections, where we must
    -- ensure that the connection is only made from one particular
    -- node.
    conn_origin_sysid text,
    conn_origin_timeline oid,
    conn_origin_dboid oid,

    PRIMARY KEY(conn_sysid, conn_timeline, conn_dboid,
                conn_origin_sysid, conn_origin_timeline, conn_origin_dboid),

    -- Either a whole origin ID (for an override or UDR entry) or no
    -- origin ID may be provided.
    CONSTRAINT origin_all_or_none_null
        CHECK ((conn_origin_sysid = '0') = (conn_origin_timeline = 0)
           AND (conn_origin_sysid = '0') = (conn_origin_dboid = 0)),

    -- Indicates that this connection is unidirectional; there won't be
    -- a corresponding inbound connection from the peer node. Only permitted
    -- where the conn_origin fields are set.
    conn_is_unidirectional boolean not null default false,

    CONSTRAINT unidirectional_conn_must_have_origin
        CHECK ((NOT conn_is_unidirectional) OR (conn_origin_sysid <> '0')),

    conn_dsn text not null,

    conn_apply_delay integer
        CHECK (conn_apply_delay >= 0),

    conn_replication_sets text[]
);

REVOKE ALL ON TABLE bdr_connections FROM public;

COMMENT ON TABLE bdr_connections IS 'Connection information for nodes in the group. Don''t modify this directly, use the provided functions. One entry should exist per node in the group.';

COMMENT ON COLUMN bdr_connections.conn_sysid IS 'System identifer for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_timeline IS 'System timeline ID for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_dboid IS 'System database OID for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_origin_sysid IS 'If set, ignore this entry unless the local sysid is this';
COMMENT ON COLUMN bdr_connections.conn_origin_timeline IS 'If set, ignore this entry unless the local timeline is this';
COMMENT ON COLUMN bdr_connections.conn_origin_dboid IS 'If set, ignore this entry unless the local dboid is this';
COMMENT ON COLUMN bdr_connections.conn_dsn IS 'A libpq-style connection string specifying how to make a connection to this node from other nodes.';
COMMENT ON COLUMN bdr_connections.conn_apply_delay IS 'If set, milliseconds to wait before applying each transaction from the remote node. Mainly for debugging. If null, the global default applies.';
COMMENT ON COLUMN bdr_connections.conn_replication_sets IS 'Replication sets this connection should participate in, if non-default.';

SELECT pg_catalog.pg_extension_config_dump('bdr_connections', '');

CREATE FUNCTION bdr_connections_changed()
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_connections_changed() FROM public;

COMMENT ON FUNCTION bdr_connections_changed() IS 'Internal BDR function, do not call directly.';


--
-- This is a helper for node_join, for internal use only. It's called
-- on the remote end by the init code when joining an existing group,
-- to do the remote-side setup.
--
CREATE FUNCTION bdr.internal_node_join(
    sysid text, timeline oid, dboid oid,
    node_external_dsn text,
    apply_delay integer,
    replication_sets text[]
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
AS
$body$
DECLARE
    status "char";
BEGIN
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    IF bdr_variant() <> 'BDR' THEN
        RAISE USING
            MESSAGE = 'Full BDR required but this module is built for '||bdr_variant(),
            DETAIL = 'The target node is running something other than full BDR so you cannot join a BDR node to it',
            HINT = 'Install full BDR if possible or use the UDR functions.',
            ERRCODE = 'feature_not_supported';
    END IF;

    -- Assert that we have a bdr_nodes entry with state = i on this node
    SELECT INTO status
    FROM bdr.bdr_nodes
    WHERE node_sysid = sysid
      AND node_timeline = timeline
      AND node_dboid = dboid;

    IF NOT FOUND THEN
        RAISE object_not_in_prerequisite_state
              USING MESSAGE = format('bdr.bdr_nodes entry for (%s,%s,%s) not found',
                                     sysid, timeline, dboid);
    END IF;

    IF status <> 'i' THEN
        RAISE object_not_in_prerequisite_state
              USING MESSAGE = format('bdr.bdr_nodes entry for (%s,%s,%s) has unexpected status %L (expected ''i'')',
                                     sysid, timeline, dboid, status);
    END IF;

    -- Insert or Update the connection info on this node, which we must be
    -- initing from.
    -- No need to care about concurrency here as we hold EXCLUSIVE LOCK.
    BEGIN
        INSERT INTO bdr.bdr_connections
        (conn_sysid, conn_timeline, conn_dboid,
         conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,
         conn_dsn,
         conn_apply_delay, conn_replication_sets,
         conn_is_unidirectional)
        VALUES
        (sysid, timeline, dboid,
         '0', 0, 0,
         node_external_dsn,
         CASE WHEN apply_delay = -1 THEN NULL ELSE apply_delay END,
         replication_sets, false);
    EXCEPTION WHEN unique_violation THEN
        UPDATE bdr.bdr_connections
        SET conn_dsn = node_external_dsn,
            conn_apply_delay = CASE WHEN apply_delay = -1 THEN NULL ELSE apply_delay END,
            conn_replication_sets = replication_sets,
            conn_is_unidirectional = false
        WHERE conn_sysid = sysid
          AND conn_timeline = timeline
          AND conn_dboid = dboid
          AND conn_origin_sysid = '0'
          AND conn_origin_timeline = 0
          AND conn_origin_dboid = 0;
    END;

    -- Schedule the apply worker launch for commit time
    PERFORM bdr.bdr_connections_changed();

    -- and ensure the apply worker is launched on other nodes
    -- when this transaction replicates there, too.
    INSERT INTO bdr.bdr_queued_commands
    (lsn, queued_at, perpetrator, command_tag, command)
    VALUES
    (pg_current_xlog_insert_location(), current_timestamp, current_user,
    'SELECT', 'SELECT bdr.bdr_connections_changed()');
END;
$body$;


CREATE FUNCTION bdr.internal_update_seclabel()
RETURNS void LANGUAGE plpgsql
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    v_label json;
BEGIN
    -- Update 'bdr' parameter in the current label if there's one.
    -- (Right now there's not much point to this but later we'll be
    -- possibly have more information in there.)

    -- first select existing label
    SELECT label::json INTO v_label
    FROM pg_catalog.pg_shseclabel
    WHERE provider = 'bdr'
      AND classoid = 'pg_database'::regclass
      AND objoid = (SELECT oid FROM pg_database WHERE datname = current_database());

    -- then replace 'bdr' with 'bdr'::true
    SELECT json_object_agg(key, value) INTO v_label
    FROM (
        SELECT key, value
        FROM json_each(v_label)
        WHERE key <> 'bdr'
      UNION ALL
        SELECT 'bdr', to_json(true)
    ) d;

    -- and set the newly computed label
    -- (It's safe to do this early, it won't take effect
    -- until commit)
    EXECUTE format('SECURITY LABEL FOR bdr ON DATABASE %I IS %L',
                   current_database(), v_label);
END;
$body$;

-- Setup that's common to BDR and UDR joins
CREATE FUNCTION bdr.internal_begin_join(
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

--
-- The public interface for node join/addition, to be run to join a currently
-- unconnected node with a blank database to a BDR group.
--
CREATE FUNCTION bdr.bdr_group_join(
    local_node_name text,
    node_external_dsn text,
    join_using_dsn text,
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
    localid record;
    connectback_nodeinfo record;
    remoteinfo record;
BEGIN
    IF node_external_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'dsn may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    IF bdr_variant() <> 'BDR' THEN
        RAISE USING
            MESSAGE = 'Full BDR required but this module is built for '||bdr_variant(),
            DETAIL = 'The local node is not running full BDR, which is required to use bdr_join',
            HINT = 'Install full BDR if possible or use the UDR functions.',
            ERRCODE = 'feature_not_supported';
    END IF;

    PERFORM bdr.internal_begin_join(
        'bdr_group_join',
        local_node_name,
        CASE WHEN node_local_dsn IS NULL THEN node_external_dsn ELSE node_local_dsn END,
        join_using_dsn);

    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    -- Request additional connection tests to determine that the remote is
    -- reachable for replication and non-replication mode and that the remote
    -- can connect back to us via 'dsn' on non-replication and replication
    -- modes.
    --
    -- This cannot be checked for the first node since there's no peer
    -- to ask for help.
    IF join_using_dsn IS NOT NULL THEN

        SELECT * INTO connectback_nodeinfo
        FROM bdr.bdr_test_remote_connectback(join_using_dsn, node_external_dsn);

        -- The connectback must actually match our local node identity
        -- and must provide a superuser connection.
        IF NOT connectback_nodeinfo.is_superuser THEN
            RAISE USING
                MESSAGE = 'node_external_dsn does not have superuser rights when connecting via remote node',
                DETAIL = format($$The dsn '%s' connects successfully but does not grant superuser rights$$, dsn),
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;

        IF (connectback_nodeinfo.sysid, connectback_nodeinfo.timeline, connectback_nodeinfo.dboid)
          IS DISTINCT FROM
           (localid.sysid, localid.timeline, localid.dboid)
          AND
           (connectback_nodeinfo.sysid, connectback_nodeinfo.timeline, connectback_nodeinfo.dboid)
          IS DISTINCT FROM
           (NULL, NULL, NULL) -- Returned by old versions' dummy functions
        THEN
            RAISE USING
                MESSAGE = 'node identity for node_external_dsn does not match current node when connecting back via remote',
                DETAIL = format($$The dsn '%s' connects to a node with identity (%s,%s,%s) but the local node is (%s,%s,%s)$$,
                    node_local_dsn, connectback_nodeinfo.sysid, connectback_nodeinfo.timeline,
                    connectback_nodeinfo.dboid, localid.sysid, localid.timeline, localid.dboid),
                HINT = 'The ''node_external_dsn'' parameter must refer to the node you''re running this function from, from the perspective of the node pointed to by join_using_dsn',
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
    END IF;

    -- Null/empty checks are skipped, the underlying constraints on the table
    -- will catch that for us.
    INSERT INTO bdr.bdr_connections (
        conn_sysid, conn_timeline, conn_dboid,
        conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,
        conn_dsn, conn_apply_delay, conn_replication_sets,
        conn_is_unidirectional
    ) VALUES (
        localid.sysid, localid.timeline, localid.dboid,
        '0', 0, 0,
        node_external_dsn, apply_delay, replication_sets, false
    );

    -- Now ensure the per-db worker is started if it's not already running.
    -- This won't actually take effect until commit time, it just adds a commit
    -- hook to start the worker when we commit.
    PERFORM bdr.bdr_connections_changed();
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_group_join(text, text, text, text, integer, text[])
IS 'Join an existing BDR group by connecting to a member node and copying its contents';

CREATE FUNCTION bdr.bdr_group_create(
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
BEGIN
    PERFORM bdr.bdr_group_join(
        local_node_name := local_node_name,
        node_external_dsn := node_external_dsn,
        join_using_dsn := null,
        node_local_dsn := node_local_dsn,
        apply_delay := apply_delay,
        replication_sets := replication_sets);
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_group_create(text, text, text, integer, text[])
IS 'Create a BDR group, turning a stand-alone database into the first node in a BDR group';

--
-- The public interface for unidirectional replication setup.
--
CREATE FUNCTION bdr.bdr_subscribe(
    local_node_name text,
    subscribe_to_dsn text,
    node_local_dsn text,
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
    localid record;
    remoteid record;
BEGIN
    IF node_local_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'node_local_dsn may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    IF subscribe_to_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'remote may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT remote_sysid AS sysid, remote_timeline AS timeline,
           remote_dboid AS dboid INTO remoteid
    FROM bdr.internal_begin_join('bdr_subscribe',
         local_node_name || '-subscriber',
         node_local_dsn, subscribe_to_dsn);

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

    IF FOUND THEN
        RAISE USING
            MESSAGE = 'This node is already connected to given remote node',
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Insert node entry representing remote node
    PERFORM 1 FROM bdr_nodes
    WHERE node_sysid = remoteid.sysid
      AND node_timeline = remoteid.timeline
      AND node_dboid = remoteid.dboid;

    IF NOT FOUND THEN
        INSERT INTO bdr_nodes (
            node_name,
            node_sysid, node_timeline, node_dboid,
            node_status
        ) VALUES (
            local_node_name,
            remoteid.sysid, remoteid.timeline, remoteid.dboid,
            'r'
        );
    END IF;

    -- Null/empty checks are skipped, the underlying constraints on the table
    -- will catch that for us.
    INSERT INTO bdr.bdr_connections (
        conn_sysid, conn_timeline, conn_dboid,
        conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,
        conn_dsn, conn_apply_delay, conn_replication_sets,
        conn_is_unidirectional
    ) VALUES (
        remoteid.sysid, remoteid.timeline, remoteid.dboid,
        localid.sysid, localid.timeline, localid.dboid,
        subscribe_to_dsn, apply_delay, replication_sets, true
    );

    -- Now ensure the per-db worker is started if it's not already running.
    -- This won't actually take effect until commit time, it just adds a commit
    -- hook to start the worker when we commit.
    PERFORM bdr.bdr_connections_changed();
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_subscribe(text, text, text, integer, text[])
IS 'Subscribe to remote logical changes';

CREATE OR REPLACE FUNCTION bdr.bdr_part_by_node_names(p_nodes text[])
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
BEGIN
    -- concurrency
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    UPDATE bdr.bdr_nodes
    SET node_status = 'k'
    WHERE node_name = ANY(p_nodes);

    -- Notify local perdb worker to kill nodes.
    PERFORM bdr.bdr_connections_changed();

END;
$body$;

CREATE FUNCTION bdr.bdr_node_join_wait_for_ready()
RETURNS void LANGUAGE plpgsql VOLATILE AS $body$
DECLARE
    _node_status "char";
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'Can only wait for node join in an ISOLATION LEVEL READ COMMITTED transaction, not %',
                        current_setting('transaction_isolation');
    END IF;

    LOOP
        SELECT INTO _node_status
          node_status
        FROM bdr.bdr_nodes
        WHERE (node_sysid, node_timeline, node_dboid)
              = bdr.bdr_get_local_nodeid();

    PERFORM pg_sleep(0.5);

        EXIT WHEN _node_status = 'r';
    END LOOP;
END;
$body$;

CREATE FUNCTION bdr_upgrade_to_090(my_conninfo cstring, local_conninfo cstring, remote_conninfo cstring)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_upgrade_to_090(cstring,cstring,cstring) FROM public;

COMMENT ON FUNCTION bdr_upgrade_to_090(cstring,cstring,cstring)
IS 'Upgrade a BDR 0.7.x or 0.8.x node to BDR 0.9.0 dynamic configuration. remote_conninfo is the node to connect to to perform the upgrade, my_conninfo is the dsn for other nodes to connect to this node with, local_conninfo is used to connect locally back to the node. Use null remote conninfo on the first node.';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
