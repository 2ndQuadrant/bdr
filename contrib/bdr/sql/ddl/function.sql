-- tests for functions and triggers
\c postgres super
CREATE FUNCTION test_fn(IN inpar character varying (20), INOUT inoutpar integer, OUT timestamp with time zone) RETURNS SETOF record AS
$$
BEGIN
END;
$$ LANGUAGE plpgsql IMMUTABLE  STRICT;
SELECT * FROM pg_proc WHERE proname = 'test_fn';
\c regression
SELECT * FROM pg_proc WHERE proname = 'test_fn';

ALTER FUNCTION test_fn(varchar, integer) SECURITY DEFINER CALLED ON NULL INPUT VOLATILE ROWS 1 COST 1;
SELECT * FROM pg_proc WHERE proname = 'test_fn';
\c postgres
SELECT * FROM pg_proc WHERE proname = 'test_fn';

CREATE OR REPLACE FUNCTION test_fn(IN inpar varchar, INOUT inoutpar integer, OUT timestamp with time zone) RETURNS SETOF record AS
$$
BEGIN
END;
$$ LANGUAGE plpgsql STABLE;
SELECT * FROM pg_proc WHERE proname = 'test_fn';
\c regression
SELECT * FROM pg_proc WHERE proname = 'test_fn';

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

CREATE TABLE test_trigger_table (f1 integer, f2 text);
CREATE TRIGGER test_trigger_fn_trg1 BEFORE INSERT OR DELETE ON test_trigger_table FOR EACH STATEMENT WHEN (True) EXECUTE PROCEDURE test_trigger_fn();
CREATE TRIGGER test_trigger_fn_trg2 AFTER UPDATE OF f1 ON test_trigger_table FOR EACH ROW EXECUTE PROCEDURE test_trigger_fn();
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\d+ test_trigger_table
\c regression
\d+ test_trigger_table

ALTER TRIGGER test_trigger_fn_trg1 ON test_trigger_table RENAME TO test_trigger_fn_trg;
\d+ test_trigger_table
\c postgres
\d+ test_trigger_table

DROP TRIGGER test_trigger_fn_trg2 ON test_trigger_table;
\d+ test_trigger_table
\c regression
\d+ test_trigger_table

-- should fail (for test to be useful it should be called on different node than CREATE FUNCTION)
DROP FUNCTION test_trigger_fn();

DROP TABLE test_trigger_table;
DROP FUNCTION test_trigger_fn();
\d+ test_trigger_table
\c postgres
\d+ test_trigger_table
\df+ test_trigger_fn
