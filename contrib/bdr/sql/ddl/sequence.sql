
-- ALTER TABLE DROP COLUMN (pk column)
CREATE TABLE test (test_id SERIAL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;

\c postgres
\d+ test
SELECT relname, relkind FROM pg_class WHERE relname = 'test_test_id_seq';
\d+ test_test_id_seq

ALTER TABLE test DROP COLUMN test_id;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
\d+ test
SELECT relname, relkind FROM pg_class WHERE relname = 'test_test_id_seq';

DROP TABLE test;

-- ADD CONSTRAINT PRIMARY KEY
CREATE TABLE test (test_id SERIAL NOT NULL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
\d+ test
ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (test_id);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
\d+ test

DROP TABLE test;
