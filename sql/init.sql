SELECT * FROM bdr_regress_variables()
\gset

\set VERBOSITY terse

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

-- Wait for the creating node to go fully active
DO LANGUAGE plpgsql $$
BEGIN
  WHILE NOT EXISTS (SELECT 1 FROM bdr.state_journal_details WHERE state_name = 'ACTIVE')
  LOOP
    PERFORM pg_sleep(0.5);
  END LOOP;
END;
$$;

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info(NULL);
SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info((SELECT node_group_id FROM bdr.node_group));

\c :node2_dsn

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info(NULL);

SELECT 1
FROM bdr.create_node(node_name := 'node2', local_dsn := :'node2_dsn' || ' user=super');

-- TODO: https://redmine.2ndquadrant.com/issues/1057
SELECT pg_sleep(0.5);

SELECT 1
FROM bdr.join_node_group(:'node1_dsn', 'nosuch-nodegroup');

-- TODO: https://redmine.2ndquadrant.com/issues/1057
SELECT pg_sleep(0.5);

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info((SELECT node_group_id FROM bdr.node_group));

SELECT 1 FROM bdr.join_node_group(:'node1_dsn', pause_in_standby := true);

SELECT node_name, node_local_state, nodegroup_name, pgl_interface_name FROM bdr.node_group_member_info((SELECT node_group_id FROM bdr.node_group));

\c :node1_dsn

-- A dummy transaction will help make sure we make prompt progress.
SELECT 1 FROM txid_current();

-- Forcing a checkpoint will force out replication origins and make us
-- advance more promptly too.
CHECKPOINT;

\c :node2_dsn

-- Wait for the joining node to reach standby state
DO LANGUAGE plpgsql $$
BEGIN
  WHILE NOT EXISTS (SELECT 1 FROM bdr.state_journal_details WHERE state_name = 'STANDBY')
  LOOP
    PERFORM pg_sleep(0.5);
  END LOOP;
END;
$$;

SELECT goal_state_name FROM bdr.state_journal_details ORDER BY state_counter DESC LIMIT 1;

-- TODO: replicate some minimal test data here
-- TODO: Make standby read-only RM#859

-- Promote to active
SELECT bdr.promote_node();

SELECT goal_state_name FROM bdr.state_journal_details ORDER BY state_counter DESC LIMIT 1;

-- Wait for the joining node to go fully active
DO LANGUAGE plpgsql $$
BEGIN
  WHILE NOT EXISTS (SELECT 1 FROM bdr.state_journal_details WHERE state_name = 'ACTIVE')
  LOOP
    PERFORM pg_sleep(0.5);
  END LOOP;
END;
$$;

\c :node1_dsn

-- Wait for the join target to start replaying from the joining node
-- at the end of BDR setup.
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

\c :node2_dsn

SELECT subscription_name, status, provider_node, slot_name, replication_sets
FROM pglogical.show_subscription_status();

\c :node1_dsn

SELECT
    state_counter, state, state_name, peer_id, peer_name,
    regexp_replace(
	regexp_replace(extra_data, '[[:xdigit:]]{1,8}/[[:xdigit:]]{8}', 'X/XXXXXXXX'),
	 '[[:digit:]]', 'n', 'g') AS extra_data_masked
FROM bdr.state_journal_details;

\c :node2_dsn

SELECT
    state_counter, state, state_name, peer_id, peer_name,
    regexp_replace(
	regexp_replace(extra_data, '[[:xdigit:]]{1,8}/[[:xdigit:]]{8}', 'X/XXXXXXXX'),
	 '[[:digit:]]', 'n', 'g') AS extra_data_masked
FROM bdr.state_journal_details;
