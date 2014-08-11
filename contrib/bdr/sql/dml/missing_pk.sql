\c postgres
CREATE TABLE bdr_missing_pk(a serial);
CREATE VIEW bdr_missing_pk_view AS SELECT * FROM bdr_missing_pk;

INSERT INTO bdr_missing_pk SELECT generate_series(1, 10);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
SELECT * FROM bdr_missing_pk;

-- these should fails
UPDATE bdr_missing_pk SET a = 1;
DELETE FROM bdr_missing_pk WHERE a = 1;

WITH foo AS (
	UPDATE bdr_missing_pk SET a = 1 WHERE a > 1 RETURNING a
) SELECT * FROM foo;

WITH foo AS (
	DELETE FROM bdr_missing_pk RETURNING a
) SELECT * FROM foo;

UPDATE bdr_missing_pk_view SET a = 1;
DELETE FROM bdr_missing_pk_view WHERE a = 1;

WITH foo AS (
	UPDATE bdr_missing_pk_view SET a = 1 WHERE a > 1 RETURNING a
) SELECT * FROM foo;

WITH foo AS (
	DELETE FROM bdr_missing_pk_view RETURNING a
) SELECT * FROM foo;

-- success again
TRUNCATE bdr_missing_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
SELECT * FROM bdr_missing_pk;

DROP TABLE bdr_missing_pk CASCADE;
