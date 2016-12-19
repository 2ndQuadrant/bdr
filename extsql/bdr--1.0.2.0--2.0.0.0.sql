--\echo Use "CREATE EXTENSION bdr" to load this file. \quit
--
-- This extension script adds compatibility for 9.6 DDL replication
--

SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SET LOCAL bdr.skip_ddl_replication = true;
SET LOCAL search_path = bdr;

ALTER TABLE bdr_queued_commands
  ADD COLUMN search_path TEXT;

UPDATE bdr_queued_commands
SET search_path = '';

ALTER TABLE bdr_queued_commands
  ALTER COLUMN search_path SET DEFAULT '';

ALTER TABLE bdr_queued_commands
  ALTER COLUMN search_path SET NOT NULL;

-- Marking this immutable is technically a bit cheeky as we could add
-- new statuses. But for index use we need it, and it's safe since
-- any unrecognised entries will result in ERRORs and can thus never
-- exist in an index.
CREATE FUNCTION bdr.node_status_from_char("char")
RETURNS text LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_node_status_from_char';

CREATE FUNCTION bdr.node_status_to_char(text)
RETURNS "char" LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_node_status_to_char';

--
-- DDL replication limitations in 9.6bdr mean that we can't directly
-- EXECUTE ddl in function bodies, and must use bdr.bdr_replicate_ddl_command
-- with a fully qualified relation name.
--
CREATE OR REPLACE FUNCTION bdr.table_set_replication_sets(p_relation regclass, p_sets text[])
  RETURNS void
  VOLATILE
  LANGUAGE 'plpgsql'
  SET bdr.permit_unsafe_ddl_commands = true
  SET search_path = ''
  AS $$
DECLARE
    v_label json;
BEGIN
    -- emulate STRICT for p_relation parameter
    IF p_relation IS NULL THEN
        RETURN;
    END IF;

    -- query current label
    SELECT label::json INTO v_label
    FROM pg_catalog.pg_seclabel
    WHERE provider = 'bdr'
        AND classoid = 'pg_class'::regclass
        AND objoid = p_relation;

    -- replace old 'sets' parameter with new value
    SELECT json_object_agg(key, value) INTO v_label
    FROM (
        SELECT key, value
        FROM json_each(v_label)
        WHERE key <> 'sets'
      UNION ALL
        SELECT
            'sets', to_json(p_sets)
        WHERE p_sets IS NOT NULL
    ) d;

    -- and now set the appropriate label
    PERFORM bdr.bdr_replicate_ddl_command(format('SECURITY LABEL FOR bdr ON TABLE %s IS %L', p_relation, v_label)) ;
END;
$$;

ALTER TABLE bdr.bdr_nodes
  ADD COLUMN node_seq_id smallint;

-- BDR doesn't like partial unique indexes, but we don't do
-- conflict resolution on bdr_nodes so it's safe here.
CREATE UNIQUE INDEX ON bdr.bdr_nodes(node_seq_id) WHERE (node_status IN (bdr.node_status_to_char('BDR_NODE_STATUS_READY')));

CREATE FUNCTION bdr.global_seq_nextval(regclass)
RETURNS bigint
LANGUAGE c STRICT VOLATILE AS 'MODULE_PATHNAME','global_seq_nextval_oid';

COMMENT ON FUNCTION bdr.global_seq_nextval(regclass)
IS 'generate sequence values unique to this node using a local sequence as a seed';

-- For testing purposes we sometimes want to be able to override the
-- timestamp etc.
CREATE FUNCTION bdr.global_seq_nextval_test(regclass, bigint)
RETURNS bigint
LANGUAGE c STRICT VOLATILE AS 'MODULE_PATHNAME','global_seq_nextval_oid';

COMMENT ON FUNCTION bdr.global_seq_nextval_test(regclass, bigint)
IS 'function for BDR testing only, do not use in application code';

-- Add "node_status" to remote_nodeinfo result
DROP FUNCTION bdr.bdr_get_remote_nodeinfo(
	dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid,
	variant OUT text, version OUT text, version_num OUT integer,
	min_remote_version_num OUT integer, is_superuser OUT boolean);

CREATE FUNCTION bdr.bdr_get_remote_nodeinfo(
	dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid,
	variant OUT text, version OUT text, version_num OUT integer,
	min_remote_version_num OUT integer, is_superuser OUT boolean,
	node_status OUT "char")
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

-- Update join to check node status of remote during join
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
	cur_node RECORD;
BEGIN
    -- Only one tx can be adding connections
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    RAISE LOG USING MESSAGE = format('node identity of node being created is (%s,%s,%s)', localid.sysid, localid.timeline, localid.dboid);

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
    --
    -- We'll get a null node name back at this point since we haven't
    -- inserted our nodes record (and it wouldn't have committed yet
    -- if we had).
    ---
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

        IF remote_nodeinfo.node_status IS DISTINCT FROM bdr.node_status_to_char('BDR_NODE_STATUS_READY') THEN
            RAISE USING
                MESSAGE = 'remote node does not appear to be a fully running BDR node',
                DETAIL = format($$The dsn '%s' connects successfully but the target node has bdr.bdr_nodes node_status=%s instead of expected 'r'$$, remote_dsn, remote_nodeinfo.node_status),
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

    -- Create local node record so the apply worker knows
    -- to start initializing this node with bdr_init_replica
    -- when it's started.
    --
    -- bdr_init_copy might've created a node entry in catchup
    -- mode already, in which case we can skip this.
    SELECT node_status FROM bdr_nodes
    WHERE node_sysid = localid.sysid
      AND node_timeline = localid.timeline
      AND node_dboid = localid.dboid
    INTO cur_node;

    IF NOT FOUND THEN
        INSERT INTO bdr_nodes (
            node_name,
            node_sysid, node_timeline, node_dboid,
            node_status, node_local_dsn, node_init_from_dsn
        ) VALUES (
            local_node_name,
            localid.sysid, localid.timeline, localid.dboid,
            bdr.node_status_to_char('BDR_NODE_STATUS_BEGINNING_INIT'),
            node_local_dsn, remote_dsn
        );
    ELSIF bdr.node_status_from_char(cur_node.node_status) = 'BDR_NODE_STATUS_CATCHUP' THEN
        RAISE DEBUG 'starting node join in BDR_NODE_STATUS_CATCHUP';
    ELSE
        RAISE USING
            MESSAGE = 'a bdr_nodes entry for this node already exists',
            DETAIL = format('bdr.bdr_nodes entry for (%s,%s,%s) named ''%s'' with status %s exists',
                            cur_node.node_sysid, cur_node.node_timeline, cur_node.node_dboid,
                            cur_node.node_name, bdr.node_status_from_char(cur_node.node_status)),
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    PERFORM bdr.internal_update_seclabel();
END;
$body$;

CREATE FUNCTION bdr.acquire_global_lock(lockmode text)
RETURNS void LANGUAGE c VOLATILE STRICT
AS 'MODULE_PATHNAME','bdr_acquire_global_lock_sql';

REVOKE ALL ON FUNCTION bdr.acquire_global_lock(text) FROM public;

COMMENT ON FUNCTION bdr.acquire_global_lock(text) IS
'Acquire bdr global lock ("ddl lock") in specified mode';

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
        -- One exception is if we're the only live node
        IF (SELECT count(node_status) FROM bdr.bdr_nodes WHERE node_status IN (bdr.node_status_to_char('BDR_NODE_STATUS_READY'))) > 1 THEN
            RAISE USING
                MESSAGE = 'cannot part a node from its self',
                DETAIL = 'Attempted to bdr_part_by_node_names(...) on node '||bdr.bdr_get_local_node_name()||' which is one of the nodes being parted',
                HINT = 'You must call call bdr_part_by_node_names on a node that is not being removed',
                ERRCODE = 'object_in_use';
        ELSE
            RAISE WARNING USING
                MESSAGE = 'parting last node',
                HINT = 'Marking last node as parted. To remove BDR completely use bdr.remove_bdr_from_local_node(...)';
        END IF;
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
        WHERE bdr_nodes.node_status <> bdr.node_status_to_char('BDR_NODE_STATUS_READY')
    LOOP
        IF r.node_status = bdr.node_status_to_char('BDR_NODE_STATUS_KILLED') THEN
            RAISE INFO 'Node %i is already parted, ignoring', r.node_name;
        ELSE
            RAISE WARNING 'Node % is in state % not expected ''r'' (BDR_NODE_STATUS_READY). Attempting to remove anyway.',
                r.node_name, r.node_status;
        END IF;
    END LOOP;

    UPDATE bdr.bdr_nodes
    SET node_status = bdr.node_status_to_char('BDR_NODE_STATUS_KILLED')
    WHERE node_name = ANY(p_nodes);

    -- Notify local perdb worker to kill nodes.
    PERFORM bdr.bdr_connections_changed();
END;
$body$;

-- Conflict history table didn't have full BDR node IDs before
-- Unfortunately we cannot fix the display attribute ordering.
ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN remote_node_timeline oid;
ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN remote_node_dboid oid;
ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN local_tuple_origin_timeline oid;
ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN local_tuple_origin_dboid oid;

-- Conflict history should never be in dumps
SELECT pg_catalog.pg_extension_config_dump('bdr_conflict_history', 'WHERE false');

-- bdr.bdr_nodes gets synced by bdr_sync_nodes(), it shouldn't be
-- dumped and applied.
SELECT pg_catalog.pg_extension_config_dump('bdr_nodes', 'WHERE false');

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
