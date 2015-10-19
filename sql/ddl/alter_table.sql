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

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

-- ALTER COLUMN ... SET STATISTICS
\c postgres
CREATE TABLE test_tbl(id int);
ALTER TABLE test_tbl ALTER COLUMN id SET STATISTICS 10;
\d+ test_tbl
ALTER TABLE test_tbl ALTER COLUMN id SET STATISTICS 0;
\d+ test_tbl
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\d+ test_tbl
ALTER TABLE test_tbl ALTER COLUMN id SET STATISTICS -1;
\d+ test_tbl
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\d+ test_tbl
DROP TABLE test_tbl;

--- INHERITANCE ---
\c postgres

CREATE TABLE test_inh_root (id int primary key, val1 varchar, val2 int);
CREATE TABLE test_inh_chld1 (child1col int) INHERITS (test_inh_root);
CREATE TABLE test_inh_chld2 () INHERITS (test_inh_chld1);

INSERT INTO test_inh_root(id, val1, val2)
SELECT x, x::text, x%4 FROM generate_series(1,10) x;

INSERT INTO test_inh_chld1(id, val1, val2, child1col)
SELECT x, x::text, x%4+1, x*2 FROM generate_series(11,20) x;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_inh_root
\d+ test_inh_chld1
\d+ test_inh_chld2
\c regression
\d+ test_inh_root
\d+ test_inh_chld1
\d+ test_inh_chld2

SELECT * FROM test_inh_root;
SELECT * FROM test_inh_chld1;
SELECT * FROM test_inh_chld2;

SET bdr.permit_unsafe_ddl_commands = true;
ALTER TABLE test_inh_root ADD CONSTRAINT idchk CHECK (id > 0);
ALTER TABLE ONLY test_inh_chld1 ALTER COLUMN id SET DEFAULT 1;
ALTER TABLE ONLY test_inh_root DROP CONSTRAINT idchk;
RESET bdr.permit_unsafe_ddl_commands;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_inh_root
\d+ test_inh_chld1
\d+ test_inh_chld2
\c postgres
\d+ test_inh_root
\d+ test_inh_chld1
\d+ test_inh_chld2

\c regression

SELECT * FROM test_inh_root;
SELECT * FROM test_inh_chld1;
SELECT * FROM test_inh_chld2;

-- Should fail with an ERROR
ALTER TABLE public.test_inh_chld1 NO INHERIT public.test_inh_root;

-- Will also fail with an ERROR
SELECT bdr.bdr_replicate_ddl_command('ALTER TABLE public.test_inh_chld1 NO INHERIT public.test_inh_root;');

-- Will be permitted
BEGIN;
SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SELECT bdr.bdr_replicate_ddl_command('ALTER TABLE public.test_inh_chld1 NO INHERIT public.test_inh_root;');
COMMIT;

SELECT * FROM test_inh_root;
SELECT * FROM test_inh_chld1;
SELECT * FROM test_inh_chld2;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

SELECT * FROM test_inh_root;
SELECT * FROM test_inh_chld1;
SELECT * FROM test_inh_chld2;

DELETE FROM test_inh_root WHERE val2 = 0;
INSERT INTO test_inh_root(id, val1, val2) VALUES (200, 'root', 1);
INSERT INTO test_inh_chld1(id, val1, val2, child1col) VALUES (200, 'child', 0, 0);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c regression

SELECT * FROM test_inh_root;
SELECT * FROM test_inh_chld1;
SELECT * FROM test_inh_chld2;

DROP TABLE test_inh_chld2;
DROP TABLE test_inh_chld1;
DROP TABLE test_inh_root;
