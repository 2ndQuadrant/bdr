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

-- Give the master time to start up and chat to its self
SELECT pg_sleep(60);
