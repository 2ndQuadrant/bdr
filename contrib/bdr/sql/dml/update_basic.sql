-- UPDATEs with simple types
CREATE TABLE tst (
    a INTEGER PRIMARY KEY,
    b TEXT,
    c FLOAT
    );
INSERT INTO tst (a, b, c) SELECT generate_series(1, 1000), 1, 1;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT COUNT(*) FROM tst;

UPDATE tst SET b = 'xyz';
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT b, COUNT(*) FROM tst GROUP BY b ORDER BY b;

UPDATE tst SET b = 'def' WHERE a BETWEEN 10 AND 500;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT b, COUNT(*) FROM tst GROUP BY b ORDER BY b;

-- two UPDATEs without explicit sync in between
UPDATE tst SET b = 'abc' WHERE a BETWEEN 10 AND 500;
\c regression
UPDATE tst SET b = 'abc' WHERE a = 1;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
SELECT b, count(*) FROM tst GROUP BY b ORDER BY b;
\c regression
SELECT b, count(*) FROM tst GROUP BY b ORDER BY b;

BEGIN;
UPDATE tst SET b = 'abc' WHERE a BETWEEN 10 AND 500;
UPDATE tst SET b = 'abc1' WHERE a BETWEEN 20 AND 600;
UPDATE tst SET b = 'abc2' WHERE a BETWEEN 30 AND 700;
COMMIT;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT b, count(*) FROM tst GROUP BY b ORDER BY b;

DROP TABLE tst;
