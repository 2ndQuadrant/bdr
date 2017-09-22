SELECT * FROM bdr_regress_variables()
\gset
\x

\set VERBOSITY terse

\c :node1_dsn

SELECT node_name, node_group_name, repset_name, interface_name, cur_state_journal_state, node_local_dbname, set_repl_ops FROM bdr.local_node_summary;
SELECT node_group_name, node_name, set_name FROM bdr.node_group_replication_sets;
SELECT node_group_name, sub_name, origin_name, target_name, sub_enabled, sub_slot_name, sub_replication_sets, sub_forward_origins, sub_apply_delay, subscription_status, bdr_subscription_mode FROM bdr.subscription_summary;
SELECT target_dbname, node_group_name, origin_name, target_name, bdr_slot_name, slot_name, plugin, slot_type, database, temporary, active, usename, application_name, state FROM bdr.node_slots;

\c :node2_dsn

SELECT node_name, node_group_name, repset_name, interface_name, cur_state_journal_state, node_local_dbname, set_repl_ops FROM bdr.local_node_summary;
SELECT node_group_name, node_name, set_name FROM bdr.node_group_replication_sets;
SELECT node_group_name, sub_name, origin_name, target_name, sub_enabled, sub_slot_name, sub_replication_sets, sub_forward_origins, sub_apply_delay, subscription_status, bdr_subscription_mode FROM bdr.subscription_summary;
SELECT target_dbname, node_group_name, origin_name, target_name, bdr_slot_name, slot_name, plugin, slot_type, database, temporary, active, usename, application_name, state FROM bdr.node_slots;
