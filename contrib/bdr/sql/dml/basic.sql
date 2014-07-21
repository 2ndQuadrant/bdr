CREATE TABLE basic_dml (
    id serial primary key,
    other integer
);

-- check basic insert replication
INSERT INTO basic_dml(other) VALUES (5), (4), (3), (2), (1);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, other FROM basic_dml ORDER BY id;

-- delete one row
DELETE FROM basic_dml WHERE id = 2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, other FROM basic_dml ORDER BY id;

DROP TABLE basic_dml;
