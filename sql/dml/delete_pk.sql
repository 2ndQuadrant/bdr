-- RT #37826 "issuing a DELETE broken replication"

CREATE TABLE test (
    id TEXT,
    ts TIMESTAMP DEFAULT ('now'::TEXT)::TIMESTAMP,
    PRIMARY KEY (id)
	);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;

-- INSERT data
INSERT INTO test (id, ts) VALUES ('row', '1970-07-21 12:00:00');
INSERT INTO test (id) VALUES ('broken');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id FROM test ORDER BY ts;

-- DELETE one row by PK
DELETE FROM test WHERE id = 'row';
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
SELECT id FROM test ORDER BY ts;

\c regression
SELECT id FROM test ORDER BY ts;

DELETE FROM test WHERE id = 'broken';
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
SELECT id FROM test ORDER BY ts;

\c postgres
SELECT id FROM test ORDER BY ts;
