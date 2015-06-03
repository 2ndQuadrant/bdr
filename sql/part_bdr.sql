\c regression
SELECT bdr.bdr_unsubscribe('node-pg');
SELECT bdr.bdr_part_by_node_names(ARRAY['node-pg']);

-- wait till all slots are killed, we need a better way for that.
SELECT pg_sleep(1);

-- There should now be zero slots
SELECT * FROM pg_replication_slots;
-- Zero active connections
SELECT count(*) FROM pg_stat_replication;
-- and the node state for the removed node should show 'k'
SELECT node_name, node_status FROM bdr.bdr_nodes;

\c postgres
-- ... on both nodes.
SELECT node_name, node_status FROM bdr.bdr_nodes;
