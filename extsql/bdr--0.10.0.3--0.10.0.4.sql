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
