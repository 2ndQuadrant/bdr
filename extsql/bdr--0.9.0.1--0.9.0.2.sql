-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE TYPE bdr.bdr_sync_type AS ENUM
(
	'none',
	'full'
);

--
-- The public interface for unidirectional replication setup.
--
DROP FUNCTION bdr.bdr_subscribe(text, text, text, integer, text[]);

CREATE FUNCTION bdr.bdr_subscribe(
    local_node_name text,
    subscribe_to_dsn text,
    node_local_dsn text,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default'],
    synchronize bdr.bdr_sync_type DEFAULT 'full'
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

	-- If user does not want synchronization, update the local node status
	-- to catched up so that the initial dump/restore does not happen
	IF synchronize = 'none' THEN
		UPDATE bdr_nodes SET node_status = 'o'
	     WHERE node_sysid = localid.sysid
		   AND node_timeline = localid.timeline
		   AND node_dboid = localid.dboid;
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

COMMENT ON FUNCTION bdr.bdr_subscribe(text, text, text, integer, text[], bdr.bdr_sync_type)
IS 'Subscribe to remote logical changes';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
