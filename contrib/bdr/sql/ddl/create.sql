CREATE TABLE test_tbl_simple_create(val int);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_simple_create
\c postgres
\d+ test_tbl_simple_create

DROP TABLE test_tbl_simple_create;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_simple_create
\c regression
\d+ test_tbl_simple_create

CREATE UNLOGGED TABLE test_tbl_unlogged_create(val int);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_unlogged_create
\c postgres
\d+ test_tbl_unlogged_create

DROP TABLE test_tbl_unlogged_create;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_unlogged_create
\c regression
\d+ test_tbl_unlogged_create

CREATE TABLE test_tbl_simple_pk(val int PRIMARY KEY);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_simple_pk
\c postgres
\d+ test_tbl_simple_pk

DROP TABLE test_tbl_simple_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_simple_pk
\c regression
\d+ test_tbl_simple_pk

CREATE TABLE test_tbl_combined_pk(val int, val1 int, PRIMARY KEY (val, val1));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_combined_pk
\c postgres
\d+ test_tbl_combined_pk

DROP TABLE test_tbl_combined_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_combined_pk
\c regression
\d+ test_tbl_combined_pk

CREATE TABLE test_tbl_serial(val SERIAL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_serial
\c postgres
\d+ test_tbl_serial

DROP TABLE test_tbl_serial;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_serial
\c regression
\d+ test_tbl_serial

CREATE TABLE test_tbl_serial(val SERIAL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_serial
\c postgres
\d+ test_tbl_serial

CREATE TABLE test_tbl_serial_pk(val SERIAL PRIMARY KEY);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_serial_pk
\c postgres
\d+ test_tbl_serial_pk

DROP TABLE test_tbl_serial_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_serial_pk
\c regression
\d+ test_tbl_serial_pk

CREATE TABLE test_tbl_serial_combined_pk(val SERIAL, val1 INTEGER, PRIMARY KEY (val, val1));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_serial_combined_pk
\c postgres
\d+ test_tbl_serial_combined_pk

CREATE TABLE test_tbl_create_index (val int, val2 int);
CREATE UNIQUE INDEX test1_idx ON test_tbl_create_index(val);
CREATE INDEX test2_idx ON test_tbl_create_index (lower(val2::text));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c regression
\d+ test_tbl_create_index

DROP INDEX test1_idx;
DROP INDEX test2_idx;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c postgres
\d+ test_tbl_create_index

CREATE INDEX test1_idx ON test_tbl_create_index(val, val2);
CREATE INDEX test2_idx ON test_tbl_create_index USING gist (val, UPPER(val2::text));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c regression
\d+ test_tbl_create_index

DROP INDEX test1_idx;
DROP INDEX CONCURRENTLY test2_idx;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c postgres
\d+ test_tbl_create_index

CREATE INDEX CONCURRENTLY test1_idx ON test_tbl_create_index(val, val2);
CREATE UNIQUE INDEX CONCURRENTLY test2_idx ON test_tbl_create_index (lower(val2::text));
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_create_index
\c regression
\d+ test_tbl_create_index

DROP INDEX CONCURRENTLY test1_idx;
DROP INDEX CONCURRENTLY test2_idx;
DROP TABLE test_tbl_create_index;

CREATE TABLE test_simple_create_with_arrays_tbl(val int[], val1 text[]);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_simple_create_with_arrays_tbl
\c postgres
\d+ test_simple_create_with_arrays_tbl

DROP TABLE test_simple_create_with_arrays_tbl;

CREATE TYPE test_t AS ENUM('a','b','c');
CREATE TABLE test_simple_create_with_enums_tbl(val test_t, val1 test_t);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_simple_create_with_enums_tbl
\c regression
\d+ test_simple_create_with_enums_tbl

DROP TABLE test_simple_create_with_enums_tbl;
DROP TYPE test_t;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_simple_create_with_enums_tbl

\dT+ test_t
\c postgres
\d+ test_simple_create_with_enums_tbl

\dT+ test_t

CREATE TYPE test_t AS (f1 text, f2 float, f3 integer);
CREATE TABLE test_simple_create_with_composites_tbl(val test_t, val1 test_t);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_simple_create_with_composites_tbl
\c regression
\d+ test_simple_create_with_composites_tbl

DROP TABLE test_simple_create_with_composites_tbl;
DROP TYPE test_t;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_simple_create_with_composites_tbl

\dT+ test_t
\c postgres
\d+ test_simple_create_with_composites_tbl

\dT+ test_t

DROP TABLE test_tbl_serial;
DROP TABLE test_tbl_serial_combined_pk;

CREATE TABLE test_tbl_inh_parent(f1 text, f2 date DEFAULT '2014-01-02');
CREATE TABLE test_tbl_inh_chld1(f1 text, f2 date DEFAULT '2014-01-02') INHERITS (test_tbl_inh_parent);
CREATE TABLE test_tbl_inh_chld2(f1 text, f2 date) INHERITS (test_tbl_inh_parent);
CREATE TABLE test_tbl_inh_chld3(f1 text) INHERITS (test_tbl_inh_parent, test_tbl_inh_chld1);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_inh_*
\c regression
\d+ test_tbl_inh_*

CREATE RULE test_tbl_inh_parent_rule_ins_1 AS ON INSERT TO test_tbl_inh_parent
          WHERE (f1 LIKE '%1%') DO INSTEAD
          INSERT INTO test_tbl_inh_chld1 VALUES (NEW.*);
CREATE RULE test_tbl_inh_parent_rule_ins_2 AS ON INSERT TO test_tbl_inh_parent
          WHERE (f1 LIKE '%2%') DO INSTEAD
          INSERT INTO test_tbl_inh_chld2 VALUES (NEW.*);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_inh_parent
\c postgres
\d+ test_tbl_inh_parent

DROP TABLE test_tbl_inh_chld1;
DROP TABLE test_tbl_inh_parent CASCADE;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl_inh_*
\c regression
\d+ test_tbl_inh_*
