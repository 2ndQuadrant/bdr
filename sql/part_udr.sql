\c postgres
SELECT bdr.bdr_unsubscribe('node-pg');

-- wait till all slots are killed, we need a better way for that.
SELECT pg_sleep(1);

-- the node state for the removed node should show 'k'
SELECT node_name, node_status FROM bdr.bdr_nodes;

\c regression

-- There should now be zero slots
SELECT * FROM pg_replication_slots;
-- Zero active connections
SELECT count(*) FROM pg_stat_replication;

SELECT node_name, node_status FROM bdr.bdr_nodes;
