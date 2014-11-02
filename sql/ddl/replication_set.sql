\c postgres
CREATE TABLE switcheroo(id serial primary key, data text);

-- show initial replication sets
SELECT * FROM bdr.table_get_replication_sets('switcheroo');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
SELECT * FROM bdr.table_get_replication_sets('switcheroo');

\c postgres
-- empty replication set (just 'all')
SELECT bdr.table_set_replication_sets('switcheroo', '{}');
SELECT * FROM bdr.table_get_replication_sets('switcheroo');
-- configure a couple
SELECT bdr.table_set_replication_sets('switcheroo', '{fascinating, is-it-not}');
SELECT * FROM bdr.table_get_replication_sets('switcheroo');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
SELECT * FROM bdr.table_get_replication_sets('switcheroo');

\c postgres
-- make sure we can reset replication sets to the default again
-- configure a couple
SELECT bdr.table_set_replication_sets('switcheroo', NULL);
SELECT * FROM bdr.table_get_replication_sets('switcheroo');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
SELECT * FROM bdr.table_get_replication_sets('switcheroo');

\c postgres
-- make sure reserved names can't be set
SELECT bdr.table_set_replication_sets('switcheroo', '{default,blubber}');
SELECT bdr.table_set_replication_sets('switcheroo', '{blubber,default}');
SELECT bdr.table_set_replication_sets('switcheroo', '{frakbar,all}');
--invalid characters
SELECT bdr.table_set_replication_sets('switcheroo', '{///}');
--too short/long
SELECT bdr.table_set_replication_sets('switcheroo', '{""}');
SELECT bdr.table_set_replication_sets('switcheroo', '{12345678901234567890123456789012345678901234567890123456789012345678901234567890}');

\c postgres
DROP TABLE switcheroo;
