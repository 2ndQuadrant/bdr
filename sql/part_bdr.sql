\c postgres
SELECT bdr.bdr_part_by_node_names(ARRAY['node-pg']);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c regression

-- There should now be zero slots
SELECT plugin, slot_type, database, active FROM pg_replication_slots;
-- Zero active connections
SELECT count(*) FROM pg_stat_replication;
-- and the node state for the removed node should show 'k'
SELECT node_name, node_status FROM bdr.bdr_nodes;

\c postgres
-- ... on both nodes.
SELECT node_name, node_status FROM bdr.bdr_nodes;
