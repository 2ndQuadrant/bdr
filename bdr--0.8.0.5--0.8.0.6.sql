-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE TABLE bdr_connections (
    conn_sysid text not null,
    conn_timeline oid not null, -- Ditto
    conn_dboid oid not null,  -- This is an oid local to the node_sysid cluster

    PRIMARY KEY(conn_sysid, conn_timeline, conn_dboid),
    -- Wondering why there's no FOREIGN KEY to bdr.bdr_nodes?
    -- bdr.bdr_nodes won't be populated when the bdr.bdr_connections
    -- row gets created on the local node.

    conn_dsn text not null,
    conn_local_dsn text,

    conn_init_from_dsn text,

    conn_apply_delay integer
        CHECK (conn_apply_delay >= 0),

    conn_replication_sets text[]
);

REVOKE ALL ON TABLE bdr_connections FROM public;

COMMENT ON TABLE bdr_connections IS 'Connection information for nodes in the group. Don''t modify this directly, use the provided functions. One entry should exist per node in the group.';

COMMENT ON COLUMN bdr_connections.conn_sysid IS 'System identifer for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_timeline IS 'System timeline ID for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_dboid IS 'System database OID for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_dsn IS 'A libpq-style connection string specifying how to make a connection to this node from other nodes.';
COMMENT ON COLUMN bdr_connections.conn_local_dsn IS 'When copying local state from a remote node during setup, libpq connection string to use to connect to this DB from the local host. If unspecified, uses ''dsn''.';
COMMENT ON COLUMN bdr_connections.conn_init_from_dsn IS 'When copying local state from a remote node during setup, libpq connection string to connect to the remote node to initialize from. Not used after init.';
COMMENT ON COLUMN bdr_connections.conn_apply_delay IS 'If set, milliseconds to wait before applying each transaction from the remote node. Mainly for debugging. If null, the global default applies.';
COMMENT ON COLUMN bdr_connections.conn_replication_sets IS 'Replication sets this connection should participate in, if non-default.';

-- XXX DYNCONF If we provide a string WHERE clause here we can possibly exclude
-- the connection that already exists on the target node, maybe using bdr_nodes.
SELECT pg_catalog.pg_extension_config_dump('bdr_connections', '');


CREATE FUNCTION bdr_get_local_nodeid( sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

-- bdr_get_local_nodeid is intentionally not revoked from all, it's read-only



CREATE FUNCTION bdr_connections_changed()
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_connections_changed() FROM public;

COMMENT ON FUNCTION bdr_connections_changed() IS 'Internal BDR function, do not call directly.';


--
-- This is a helper for node_join, for internal use only. It's called
-- on the remote end by the init code when joining an existing group,
-- to do the remote-side setup.
--
CREATE FUNCTION node_join_internal(
	sysid text, timeline oid, dboid oid,
	dsn text,
	init_from_dsn text,
	local_dsn text,
	apply_delay integer,
	replication_sets text[]
	)
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
AS
$$
DECLARE
    status "char";
BEGIN
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

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

    -- Insert the connection info on this node, which we must be initing from
    INSERT INTO bdr.bdr_connections
    (conn_sysid, conn_timeline, conn_dboid,
     conn_dsn, conn_local_dsn, conn_init_from_dsn,
     conn_apply_delay, conn_replication_sets)
    VALUES
    (sysid, timeline, dboid,
     dsn, local_dsn, init_from_dsn,
     CASE WHEN apply_delay = -1 THEN NULL ELSE apply_delay END,
     replication_sets);

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
$$;


--
-- The public interface for node join/addition, to be run to join a currently
-- unconnected node with a blank database to a BDR group.
--
CREATE FUNCTION node_join(
    dsn text,
    init_from_dsn text,
    local_dsn text DEFAULT NULL,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default']
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $$
DECLARE
    v_label json;
    nodeid record;
BEGIN
    -- Only one tx can be adding connections
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;

    -- We're doing a read-modify-write on our seclabel entry, so try to avoid
    -- races. This won't lock out writers from other DBs, but we only ever
    -- write to our own.
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    SELECT sysid, timeline, dboid INTO nodeid
    FROM bdr.bdr_get_local_nodeid();

    -- Null/empty checks are skipped, the underlying constraints on the table
    -- will catch that for us.
    INSERT INTO bdr.bdr_connections(
        conn_sysid, conn_timeline, conn_dboid,
        conn_dsn, conn_local_dsn, conn_init_from_dsn,
        conn_apply_delay, conn_replication_sets
    )
    VALUES (
        nodeid.sysid, nodeid.timeline, nodeid.dboid,
        dsn, local_dsn, init_from_dsn,
        apply_delay, replication_sets
    );

    -- Is there a current label?
    -- (Right now there's not much point to this check but later
    -- we'll be possibly updating it.)
    SELECT label::json INTO v_label
    FROM pg_catalog.pg_shseclabel
    WHERE provider = 'bdr'
      AND classoid = 'pg_database'::regclass
      AND objoid = (SELECT oid FROM pg_database WHERE datname = current_database());

    -- Inserted without error? Ensure there's a security label on the
    -- database.
    -- (Later we'll probably use something other than a dummy value for the seclabel,
    --  in which case we'll need to watch out for it being overwritten by a remote
    --  dump's apply on the local node.)
    EXECUTE format('SECURITY LABEL FOR bdr ON DATABASE %I IS %L',
                   current_database(), '{"bdr": true}'::json::text);

    -- Now ensure the per-db worker is started if it's not already running.
    -- This won't actually take effect until commit time, it just adds a commit
    -- hook to start the worker when we commit.
    PERFORM bdr.bdr_connections_changed();
END;
$$;

ALTER TABLE bdr.bdr_nodes
  DROP CONSTRAINT bdr_nodes_node_status_check;

ALTER TABLE bdr.bdr_nodes
  ADD CONSTRAINT bdr_nodes_node_status_check
    CHECK (node_status in ('i', 'c', 'o', 'r'));

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
