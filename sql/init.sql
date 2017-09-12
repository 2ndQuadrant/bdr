SELECT * FROM bdr_regress_variables()
\gset

\set VERBOSITY verbose

\c :node1_dsn
CREATE EXTENSION bdr CASCADE;

SELECT E'\'' || current_database() || E'\'' AS node1_db
\gset

\c :node2_dsn
CREATE EXTENSION bdr CASCADE;

SELECT E'\'' || current_database() || E'\'' AS node2_db
\gset

\c :node1_dsn

-- Only used in tests
CREATE FUNCTION bdr_submit_comment(message text)
RETURNS text LANGUAGE c STRICT AS 'bdr','bdr_submit_comment';

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info(NULL);

SELECT 1
FROM bdr.create_node(node_name := 'node1', local_dsn := :'node1_dsn' || ' user=super');

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info(NULL);

SELECT 1
FROM bdr.create_node_group('bdrgroup');

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info(NULL);
SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info((SELECT node_group_id FROM bdr.node_group));

-- We must create a slot before creating the subscription to work
-- around the deadlock in 2ndQuadrant/pglogical_internal#152
-- TODO: BDR should do this automatically
SELECT slot_name FROM pg_create_logical_replication_slot(pglogical.pglogical_gen_slot_name(:node2_db, 'node1', 'bdrgroup_node1'), 'pglogical');

\c :node2_dsn

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info(NULL);

SELECT 1
FROM bdr.create_node(node_name := 'node2', local_dsn := :'node2_dsn' || ' user=super');

SELECT 1
FROM bdr.join_node_group(:'node1_dsn', 'nosuch-nodegroup');

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info((SELECT node_group_id FROM bdr.node_group));

SELECT 1
FROM bdr.join_node_group(:'node1_dsn', 'bdrgroup');

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info((SELECT node_group_id FROM bdr.node_group));

SELECT subscription_name, status, provider_node, slot_name, replication_sets
FROM pglogical.show_subscription_status();

-- See above...
-- TODO: BDR should do this automatically
SELECT slot_name FROM pg_create_logical_replication_slot(pglogical.pglogical_gen_slot_name(:node1_db, 'node2', 'bdrgroup_node2'), 'pglogical');

\c :node1_dsn

DO LANGUAGE plpgsql $$
BEGIN
  WHILE NOT EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status = 'replicating')
  LOOP
    PERFORM pg_sleep(0.5);
  END LOOP;
END;
$$;

SELECT subscription_name, status, provider_node, slot_name, replication_sets
FROM pglogical.show_subscription_status();

SET client_min_messages = notice;

-- Wait for it to start up

-- We already created the slots so all we have to do here is wait for
-- the slots to be in sync once we do some WAL-logged work on the
-- upstreams.

CREATE TABLE throwaway AS SELECT 1;

\c :node2_dsn

CREATE TABLE throwaway AS SELECT 1;

\c :node1_dsn

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

SELECT application_name, sync_state
FROM pg_stat_replication
ORDER BY application_name;

SELECT slot_name, plugin, slot_type, database, temporary, active
FROM pg_replication_slots ORDER BY slot_name;

SELECT
    backend_type,
    regexp_replace(application_name, '[[:digit:]]', 'n', 'g') AS appname
FROM pg_stat_activity
WHERE application_name LIKE 'pglogical%'
ORDER BY appname;
