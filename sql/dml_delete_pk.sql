-- RT #37826 "issuing a DELETE broken replication"
SELECT * FROM public.bdr_regress_variables()
\gset

\c :writedb1

BEGIN;
SET LOCAL bdr.permit_ddl_locking = true;
SELECT bdr.bdr_replicate_ddl_command($$
	CREATE TABLE public.test (
		id TEXT,
		ts TIMESTAMP DEFAULT ('now'::TEXT)::TIMESTAMP,
		PRIMARY KEY (id)
	);
$$);
COMMIT;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

-- INSERT data
INSERT INTO test (id, ts) VALUES ('row', '1970-07-21 12:00:00');
INSERT INTO test (id) VALUES ('broken');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :readdb2
SELECT id FROM test ORDER BY ts;

-- DELETE one row by PK
\c :writedb2
DELETE FROM test WHERE id = 'row';
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT id FROM test ORDER BY ts;

\c :readdb1
SELECT id FROM test ORDER BY ts;

\c :writedb1
DELETE FROM test WHERE id = 'broken';
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT id FROM test ORDER BY ts;

\c :readdb2
SELECT id FROM test ORDER BY ts;

\c :writedb1
BEGIN;
SET LOCAL bdr.permit_ddl_locking = true;
SELECT bdr.bdr_replicate_ddl_command($$DROP TABLE public.test;$$);
COMMIT;
