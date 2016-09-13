-- tests for functions and triggers
\c postgres super
CREATE FUNCTION test_fn(IN inpar character varying (20), INOUT inoutpar integer, OUT timestamp with time zone) RETURNS SETOF record AS
$$
BEGIN
	PERFORM E'\t\r\n\b\f';
END;
$$ LANGUAGE plpgsql IMMUTABLE  STRICT;
\df+ test_fn
\c regression
\df+ test_fn

ALTER FUNCTION test_fn(varchar, integer) SECURITY DEFINER CALLED ON NULL INPUT VOLATILE ROWS 1 COST 1;
\df+ test_fn
\c postgres
\df+ test_fn

CREATE OR REPLACE FUNCTION test_fn(IN inpar varchar, INOUT inoutpar integer, OUT timestamp with time zone) RETURNS SETOF record AS
$$
BEGIN
END;
$$ LANGUAGE plpgsql STABLE;
\df+ test_fn
\c regression
\df+ test_fn

DROP FUNCTION test_fn(varchar, integer);
\df test_fn
\c postgres
\df test_fn


CREATE FUNCTION test_trigger_fn() RETURNS trigger AS
$$
BEGIN
END;
$$ LANGUAGE plpgsql;
\df+ test_trigger_fn

CREATE FUNCTION showtrigstate(rel regclass)
RETURNS TABLE (
	tgname name,
	tgenabled "char",
	tgisinternal boolean)
LANGUAGE sql AS
$$
SELECT
  CASE WHEN t.tgname LIKE 'truncate_trigger%' THEN 'truncate_trigger' ELSE t.tgname END,
  t.tgenabled, t.tgisinternal
FROM pg_catalog.pg_trigger t
WHERE t.tgrelid = $1
ORDER BY t.tgname;
$$;

CREATE TABLE test_trigger_table (f1 integer, f2 text);
CREATE TRIGGER test_trigger_fn_trg1 BEFORE INSERT OR DELETE ON test_trigger_table FOR EACH STATEMENT WHEN (True) EXECUTE PROCEDURE test_trigger_fn();
CREATE TRIGGER test_trigger_fn_trg2 AFTER UPDATE OF f1 ON test_trigger_table FOR EACH ROW EXECUTE PROCEDURE test_trigger_fn();
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
-- We can't use \d+ here because tgisinternal triggers have names with the oid
-- appended, and that varies run-to-run. Use a custom query.
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c regression
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

ALTER TRIGGER test_trigger_fn_trg1 ON test_trigger_table RENAME TO test_trigger_fn_trg;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c postgres
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

ALTER TABLE test_trigger_table DISABLE TRIGGER test_trigger_fn_trg;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c regression
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

ALTER TABLE test_trigger_table DISABLE TRIGGER ALL;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c postgres
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

ALTER TABLE test_trigger_table ENABLE TRIGGER test_trigger_fn_trg2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c regression
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

ALTER TABLE test_trigger_table ENABLE TRIGGER USER;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c postgres
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

ALTER TABLE test_trigger_table ENABLE ALWAYS TRIGGER test_trigger_fn_trg;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c regression
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

ALTER TABLE test_trigger_table ENABLE REPLICA TRIGGER test_trigger_fn_trg2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c postgres
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

DROP TRIGGER test_trigger_fn_trg2 ON test_trigger_table;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
SELECT * FROM showtrigstate('test_trigger_table'::regclass);
\c regression
SELECT * FROM showtrigstate('test_trigger_table'::regclass);

-- should fail (for test to be useful it should be called on different node than CREATE FUNCTION)
DROP FUNCTION test_trigger_fn();

DROP TABLE test_trigger_table;
DROP FUNCTION test_trigger_fn();
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_trigger_table
\c postgres
\d+ test_trigger_table
\df+ test_trigger_fn
