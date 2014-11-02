/* First test whether a table's replication set can be properly manipulated */
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
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

/*
 * Now test whether sets properly control the replication of data.
 *
 * node1: dbname regression, sets: default, important, for-node-1
 * node2: dbname postgres, sets: default, important, for-node-2
 */
\c regression

CREATE TABLE settest_1(data text primary key);

INSERT INTO settest_1(data) VALUES ('should-replicate-via-default');

SELECT bdr.table_set_replication_sets('settest_1', '{}');
INSERT INTO settest_1(data) VALUES ('should-no-replicate-no-sets');

SELECT bdr.table_set_replication_sets('settest_1', '{unknown-set}');
INSERT INTO settest_1(data) VALUES ('should-no-replicate-unknown-set');

SELECT bdr.table_set_replication_sets('settest_1', '{for-node-2}');
INSERT INTO settest_1(data) VALUES ('should-replicate-via-for-node-2');

SELECT bdr.table_set_replication_sets('settest_1', '{}');
INSERT INTO settest_1(data) VALUES ('should-not-replicate-empty-again');

SELECT bdr.table_set_replication_sets('settest_1', '{unknown-set,for-node-2}');
INSERT INTO settest_1(data) VALUES ('should-replicate-via-for-node-2-even-though-unknown');

SELECT bdr.table_set_replication_sets('settest_1', '{unknown-set,important,for-node-2}');
INSERT INTO settest_1(data) VALUES ('should-replicate-via-for-node-2-and-important');

SELECT * FROM settest_1 ORDER BY data;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
SELECT * FROM settest_1 ORDER BY data;
