CREATE TABLE test_tbl_simple_create(val int);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_simple_create
\c postgres
\d+ test_tbl_simple_create

DROP TABLE test_tbl_simple_create;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_simple_create
\c regression
\d+ test_tbl_simple_create


CREATE TABLE test_tbl_simple_pk(val int PRIMARY KEY);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_simple_pk
\c postgres
\d+ test_tbl_simple_pk

DROP TABLE test_tbl_simple_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_simple_pk
\c regression
\d+ test_tbl_simple_pk

CREATE TABLE test_tbl_combined_pk(val int, val1 int, PRIMARY KEY (val, val1));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_combined_pk
\c postgres
\d+ test_tbl_combined_pk

DROP TABLE test_tbl_combined_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_combined_pk
\c regression
\d+ test_tbl_combined_pk

CREATE TABLE test_tbl_serial(val SERIAL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_serial
\c postgres
\d+ test_tbl_serial

DROP TABLE test_tbl_serial;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_serial
\c regression
\d+ test_tbl_serial

CREATE TABLE test_tbl_serial(val SERIAL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_serial
\c postgres
\d+ test_tbl_serial

CREATE TABLE test_tbl_serial_pk(val SERIAL PRIMARY KEY);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_serial_pk
\c postgres
\d+ test_tbl_serial_pk

DROP TABLE test_tbl_serial_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_serial_pk
\c regression
\d+ test_tbl_serial_pk

CREATE TABLE test_tbl_serial_combined_pk(val SERIAL, val1 INTEGER, PRIMARY KEY (val, val1));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_serial_combined_pk
\c postgres
\d+ test_tbl_serial_combined_pk

CREATE SEQUENCE test_seq USING bdr;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_seq
\c regression
\d+ test_seq

DROP SEQUENCE test_seq;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_seq;
\c postgres
\d+ test_seq

CREATE TABLE test_tbl_create_index (val int, val2 int);
CREATE UNIQUE INDEX test1_idx ON test_tbl_create_index(val);
CREATE INDEX test2_idx ON test_tbl_create_index (lower(val2::text));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c regression
\d+ test_tbl_create_index

DROP INDEX test1_idx;
DROP INDEX test2_idx;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c postgres
\d+ test_tbl_create_index

CREATE INDEX test1_idx ON test_tbl_create_index(val, val2);
CREATE INDEX test2_idx ON test_tbl_create_index USING gist (val, UPPER(val2::text));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c regression
\d+ test_tbl_create_index

DROP INDEX test1_idx;
DROP INDEX test2_idx;
DROP TABLE test_tbl_create_index;

CREATE TABLE test_simple_create_with_arrays_tbl(val int[], val1 text[]);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_simple_create_with_arrays_tbl
\c postgres
\d+ test_simple_create_with_arrays_tbl

DROP TABLE test_simple_create_with_arrays_tbl;

CREATE TYPE test_t AS ENUM('a','b','c');
CREATE TABLE test_simple_create_with_enums_tbl(val test_t, val1 test_t);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_simple_create_with_enums_tbl
\c regression
\d+ test_simple_create_with_enums_tbl

DROP TABLE test_simple_create_with_enums_tbl;
DROP TYPE test_t;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_simple_create_with_enums_tbl

\dT+ test_t
\c postgres
\d+ test_simple_create_with_enums_tbl

\dT+ test_t

CREATE TYPE test_t AS (f1 text, f2 float, f3 integer);
CREATE TABLE test_simple_create_with_composites_tbl(val test_t, val1 test_t);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_simple_create_with_composites_tbl
\c regression
\d+ test_simple_create_with_composites_tbl

DROP TABLE test_simple_create_with_composites_tbl;
DROP TYPE test_t;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\d+ test_simple_create_with_composites_tbl

\dT+ test_t
\c postgres
\d+ test_simple_create_with_composites_tbl

\dT+ test_t

DROP TABLE test_tbl_serial;
DROP TABLE test_tbl_serial_combined_pk;
