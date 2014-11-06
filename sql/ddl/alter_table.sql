CREATE TABLE test_tbl(pk int primary key, dropping_col1 text, dropping_col2 text);

ALTER TABLE test_tbl ADD COLUMN col1 text;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl ADD COLUMN col2 text;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl ADD COLUMN col3_fail timestamptz NOT NULL DEFAULT now();

ALTER TABLE test_tbl ADD COLUMN serial_col_node1 SERIAL;

ALTER TABLE test_tbl DROP COLUMN dropping_col1;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl DROP COLUMN dropping_col2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col1 SET NOT NULL;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col2 SET NOT NULL;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col1 DROP NOT NULL;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col2 DROP NOT NULL;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col1 SET DEFAULT 'abc';
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col2 SET DEFAULT 'abc';
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col1 DROP DEFAULT;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col2 DROP DEFAULT;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl ADD CONSTRAINT test_const CHECK (true);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl ADD CONSTRAINT test_const1 CHECK (true);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl DROP CONSTRAINT test_const;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl DROP CONSTRAINT test_const1;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col1 SET NOT NULL;
CREATE UNIQUE INDEX test_idx ON test_tbl(col1);
ALTER TABLE test_tbl REPLICA IDENTITY USING INDEX test_idx;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl ALTER COLUMN col2 SET NOT NULL;
CREATE UNIQUE INDEX test_idx1 ON test_tbl(col2);
ALTER TABLE test_tbl REPLICA IDENTITY USING INDEX test_idx1;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl

ALTER TABLE test_tbl REPLICA IDENTITY DEFAULT;
DROP INDEX test_idx;
DROP INDEX test_idx1;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

CREATE UNIQUE INDEX test_idx ON test_tbl(col1);
ALTER TABLE test_tbl REPLICA IDENTITY USING INDEX test_idx;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
DROP INDEX test_idx;
\d+ test_tbl
\c regression
\d+ test_tbl

CREATE USER test_user;
ALTER TABLE test_tbl OWNER TO test_user;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl

ALTER TABLE test_tbl RENAME COLUMN col1 TO foobar;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_tbl
\c regression
\d+ test_tbl

DROP TABLE test_tbl;
