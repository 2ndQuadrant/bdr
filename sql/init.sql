SELECT * FROM bdr_regress_variables()
\gset

\set VERBOSITY terse
CREATE EXTENSION bdr CASCADE;

-- We should be doing a bdr node create/join here but we don't yet have the
-- interfaces for that. We're just dummied up so far. So create a pglogical
-- node, and let that bring up BDR because the BDR plugin is loaded, even
-- though no actual BDR node is created yet.

SELECT *
FROM pglogical.create_node(node_name := 'test_node', dsn := (SELECT node1_dsn FROM bdr_regress_variables()) || ' user=super');

-- Create the BDR node
--
-- Doing it in this order is racey since pglogical will start the manager before
-- we can reliably create the bdr node entries. We'll have to restart pglogical
-- with pg_terminate_backend afterwards.

INSERT INTO bdr.node_group(node_group_id, node_group_name)
VALUES (4, 'dummy_nodegroup');

INSERT INTO bdr.node(pglogical_node_id, node_group_id)
SELECT node_id, 4 FROM pglogical.local_node;

SET client_min_messages = error;

CREATE TEMPORARY TABLE throwaway AS
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid();

DROP TABLE throwaway;

SET client_min_messages = notice;

SELECT pg_sleep(10);

-- Give the master time to start up and chat to its self
-- so we can hopefully see some messaging happening
SELECT pg_sleep(30);
