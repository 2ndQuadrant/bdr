-- test sanity checks for tables without pk
SELECT * FROM public.bdr_regress_variables()
\gset

\c :writedb1

BEGIN;
SET LOCAL bdr.permit_ddl_locking = true;
SELECT bdr.bdr_replicate_ddl_command($$
	CREATE TABLE public.bdr_missing_pk_parent(a serial PRIMARY KEY);
	CREATE TABLE public.bdr_missing_pk(a serial) INHERITS (public.bdr_missing_pk_parent);
	CREATE VIEW public.bdr_missing_pk_view AS SELECT * FROM public.bdr_missing_pk;
$$);
COMMIT;

INSERT INTO bdr_missing_pk SELECT generate_series(1, 10);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :readdb2
SELECT * FROM bdr_missing_pk;

-- these should fail
\c :writedb2
UPDATE bdr_missing_pk SET a = 1;
DELETE FROM bdr_missing_pk WHERE a = 1;

UPDATE bdr_missing_pk_parent SET a = 1;
DELETE FROM bdr_missing_pk_parent WHERE a = 1;

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

WITH foo AS (
	UPDATE bdr_missing_pk SET a = 1 RETURNING *
) INSERT INTO bdr_missing_pk SELECT * FROM foo;

WITH foo AS (
	DELETE FROM bdr_missing_pk_view RETURNING a
) INSERT INTO bdr_missing_pk SELECT * FROM foo;


-- success again
TRUNCATE bdr_missing_pk;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
\c :readdb1
SELECT * FROM bdr_missing_pk;

\c :writedb1
-- Direct updates to the catalogs should be permitted, though
-- not necessarily smart.
UPDATE pg_class
SET relname = 'bdr_missing_pk_renamed'
WHERE relname = 'bdr_missing_pk';

\dt bdr_missing_pk*
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c :readdb2
-- The catalog change should not replicate
\dt bdr_missing_pk*

\c :writedb1
UPDATE pg_class
SET relname = 'bdr_missing_pk'
WHERE relname = 'bdr_missing_pk_renamed';

BEGIN;
SET LOCAL bdr.permit_ddl_locking = true;
SELECT bdr.bdr_replicate_ddl_command($$DROP TABLE public.bdr_missing_pk CASCADE;$$);
COMMIT;
