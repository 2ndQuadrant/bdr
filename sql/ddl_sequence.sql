-- ALTER TABLE DROP COLUMN (pk column)
CREATE TABLE test (test_id SERIAL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres
\d+ test
SELECT relname, relkind FROM pg_class WHERE relname = 'test_test_id_seq';
\d+ test_test_id_seq

ALTER TABLE test DROP COLUMN test_id;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test
SELECT relname, relkind FROM pg_class WHERE relname = 'test_test_id_seq';

DROP TABLE test;

-- ADD CONSTRAINT PRIMARY KEY
CREATE TABLE test (test_id SERIAL NOT NULL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test
ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (test_id);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test

DROP TABLE test;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres

CREATE SEQUENCE test_seq USING bdr;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_seq
\c regression
\d+ test_seq

ALTER SEQUENCE test_seq owned by test_tbl_serial_combined_pk.val;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_seq
\c postgres
\d+ test_seq

-- these should fail
ALTER SEQUENCE test_seq increment by 10;
ALTER SEQUENCE test_seq minvalue 0;
ALTER SEQUENCE test_seq maxvalue 1000000;
ALTER SEQUENCE test_seq restart;
ALTER SEQUENCE test_seq cache 10;
ALTER SEQUENCE test_seq cycle;

DROP SEQUENCE test_seq;

CREATE SEQUENCE test_seq start 10000 owned by test_tbl_serial_combined_pk.val1 USING bdr;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_seq
\c regression
\d+ test_seq

DROP SEQUENCE test_seq;

-- these should fail
CREATE SEQUENCE test_seq increment by 10 USING bdr;
CREATE SEQUENCE test_seq minvalue 10 USING bdr;
CREATE SEQUENCE test_seq maxvalue 10 USING bdr;
CREATE SEQUENCE test_seq cache 10 USING bdr;
CREATE SEQUENCE test_seq cycle USING bdr;

-- non-bdr sequence
CREATE SEQUENCE test_seq increment 10;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_seq
\c postgres
\d+ test_seq

ALTER SEQUENCE test_seq increment by 10;
ALTER SEQUENCE test_seq minvalue 0;
ALTER SEQUENCE test_seq maxvalue 1000000;
ALTER SEQUENCE test_seq restart;
ALTER SEQUENCE test_seq cache 10;
ALTER SEQUENCE test_seq cycle;
ALTER SEQUENCE test_seq RENAME TO renamed_test_seq;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_seq
\d+ renamed_test_seq
\c regression
\d+ test_seq
\d+ renamed_test_seq

ALTER SEQUENCE renamed_test_seq USING bdr;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ renamed_test_seq;
\c postgres
\d+ renamed_test_seq

DROP SEQUENCE renamed_test_seq;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ renamed_test_seq;
\c regression
\d+ renamed_test_seq

CREATE SEQUENCE test_seq;
CREATE TABLE test_tbl (a int DEFAULT nextval('test_seq'));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl
\c postgres
\d+ test_tbl

DROP SEQUENCE test_seq CASCADE;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl
\c regression
\d+ test_tbl

DROP TABLE test_tbl;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
