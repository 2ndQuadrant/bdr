\c regression

CREATE TABLE rewrite_test(
    id integer primary key
);
INSERT INTO rewrite_test(id) SELECT generate_series(1,10);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0) FROM pg_stat_replication;

\c postgres

SELECT * FROM rewrite_test ORDER BY id;

\c regression
BEGIN;
SET LOCAL bdr.permit_unsafe_ddl_commands = on;
ALTER TABLE rewrite_test
  ADD COLUMN oops boolean not null default 'f';
COMMIT;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0) FROM pg_stat_replication;

\c postgres

SELECT * FROM rewrite_test ORDER BY id;
