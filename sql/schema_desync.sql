-- RT#60660

SELECT * FROM public.bdr_regress_variables()
\gset

\c :writedb1


CREATE TABLE desync (
   id integer primary key not null,
   n1 integer not null
);

\d desync

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

-- basic builtin datatypes
\c :writedb2

\d desync

\c :writedb1

-- Add a new attribute on this node only
BEGIN;
SET LOCAL bdr.skip_ddl_replication = on;
SET LOCAL bdr.skip_ddl_locking = on;
ALTER TABLE desync ADD COLUMN dropme integer;
COMMIT;

-- Create an natts=3 tuple with null RHS. This should apply fine on the other
-- side, it'll disregard the righthand NULL.
INSERT INTO desync(id, n1, dropme) VALUES (1, 1, NULL);

SELECT * FROM desync ORDER BY id;

-- This must ROLLBACK not ERROR
BEGIN;
SET LOCAL statement_timeout = '2s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

-- Drop the attribute; we're still natts=3, but one is dropped
BEGIN;
SET LOCAL bdr.skip_ddl_replication = on;
SET LOCAL bdr.skip_ddl_locking = on;
ALTER TABLE desync DROP COLUMN dropme;
COMMIT;

-- create second natts=3 tuple on db1
--
-- This will also apply on the other side, because dropped columns are always
-- sent as nulls.
INSERT INTO desync(id, n1) VALUES (2, 2);

SELECT * FROM desync ORDER BY id;

-- This must ROLLBACK not ERROR
BEGIN;
SET LOCAL statement_timeout = '2s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

\c :writedb2

-- Both new tuples should've arrived
SELECT * FROM desync ORDER BY id;

-- create natts=2 tuple on db2
--
-- This should apply to writedb1 because we right-pad rows with low natts if
-- the other side col is dropped (or nullable)
INSERT INTO desync(id, n1) VALUES (3, 3);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

-- This must ROLLBACK not ERROR
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

SELECT * FROM desync ORDER BY id;

-- Make our side confirm to the remote schema again
BEGIN;
SET LOCAL bdr.skip_ddl_replication = on;
SET LOCAL bdr.skip_ddl_locking = on;
ALTER TABLE desync ADD COLUMN dropme integer;
ALTER TABLE desync DROP COLUMN dropme;
COMMIT;

\c :writedb1

-- So now this side should apply too
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

-- This must ROLLBACK not ERROR
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

-- Yay!
SELECT * FROM desync ORDER BY id;

\c :writedb2
-- Yay! Again!
SELECT * FROM desync ORDER BY id;


-- Cleanup
DELETE FROM desync;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);


\c :writedb1

-- Now what if we repeat the scenario, but this time with non-null data?
-- The downstream should reject the too-wide row now. It doesn't matter
-- that the local col is nullable, only that the value is not null. But we're
-- going to make it not-NULLable anyway, so we can also test rejection of
-- right-extension of missing remote values.
BEGIN;
SET LOCAL bdr.skip_ddl_replication = on;
SET LOCAL bdr.skip_ddl_locking = on;
ALTER TABLE desync ADD COLUMN dropme2 integer;
ALTER TABLE desync ALTER COLUMN dropme2 SET NOT NULL;
COMMIT;

INSERT INTO desync(id, n1, dropme2) VALUES (4, 4, 4);

SELECT * FROM desync ORDER BY id;

-- This must ERROR not ROLLBACK
BEGIN;
SET LOCAL statement_timeout = '2s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

\c :writedb2

SELECT * FROM desync ORDER BY id;

-- Similarly, if we enqueue a change on our side that lacks a value
-- for the other side's non-nullable column, it must not replicate.

INSERT INTO desync(id, n1) VALUES (5, 5);

-- This must ERROR not ROLLBACK
BEGIN;
SET LOCAL statement_timeout = '2s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

SELECT * FROM desync ORDER BY id;

-- If we add the col locally, we can then apply the pending change, but we'll
-- still be stuck in the other direction because of the pending change from our
-- side.
BEGIN;
SET LOCAL bdr.skip_ddl_replication = on;
SET LOCAL bdr.skip_ddl_locking = on;
ALTER TABLE desync ADD COLUMN dropme2 integer;
COMMIT;

\c :writedb1

-- We don't support autocompletion of DEFAULTs; this won't help
BEGIN;
SET LOCAL bdr.skip_ddl_replication = on;
SET LOCAL bdr.skip_ddl_locking = on;
ALTER TABLE desync ALTER COLUMN dropme2 SET DEFAULT 0;
COMMIT;

-- This must ERROR not ROLLBACK
BEGIN;
SET LOCAL statement_timeout = '2s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

-- but if we drop the NOT NULL constraint temporarily we can
-- apply the pending change.
BEGIN;
SET LOCAL bdr.skip_ddl_replication = on;
SET LOCAL bdr.skip_ddl_locking = on;
ALTER TABLE desync ALTER COLUMN dropme2 DROP NOT NULL;
COMMIT;

\c :writedb2

-- This must ROLLBACK not ERROR
BEGIN;
SET LOCAL statement_timeout = '2s';
CREATE TABLE dummy_tbl(id integer);
ROLLBACK;

SELECT * FROM desync ORDER BY id;

\c :writedb1

SELECT * FROM desync ORDER BY id;
