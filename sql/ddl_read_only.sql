\c postgres

SELECT bdr.bdr_replicate_ddl_command($$
        CREATE TABLE public.test_read_only (
                data text
        );
$$);

-- set all nodes ro
SELECT bdr.bdr_node_set_read_only(node_name, true) FROM bdr.bdr_nodes;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

-- errors
CREATE TABLE readonly_test_shoulderror(a int);

SELECT bdr.bdr_replicate_ddl_command($$
        CREATE TABLE public.readonly_test_shoulderror (
                data text
        );
$$);

INSERT INTO public.test_read_only VALUES('foo');
UPDATE public.test_read_only SET data = 'foo';
DELETE FROM public.test_read_only;

WITH cte AS (
	INSERT INTO public.test_read_only VALUES('foo') RETURNING *
)
SELECT * FROM cte;

-- success
CREATE TEMP TABLE test_read_only_temp (
        data text
);

INSERT INTO test_read_only_temp VALUES('foo');
UPDATE test_read_only_temp SET data = 'foo';
DELETE FROM test_read_only_temp;

WITH cte AS (
	INSERT INTO test_read_only_temp VALUES('foo') RETURNING *
)
SELECT * FROM cte;

\c regression
-- errors
CREATE TABLE test(a int);

SELECT bdr.bdr_replicate_ddl_command($$
        CREATE TABLE public.test (
                data text
        );
$$);

INSERT INTO public.test_read_only VALUES('foo');
UPDATE public.test_read_only SET data = 'foo';
DELETE FROM public.test_read_only;

WITH cte AS (
	INSERT INTO public.test_read_only VALUES('foo') RETURNING *
)
SELECT * FROM cte;

-- success
CREATE TEMP TABLE test_read_only_temp (
        data text
);

INSERT INTO test_read_only_temp VALUES('foo');
UPDATE test_read_only_temp SET data = 'foo';
DELETE FROM test_read_only_temp;

WITH cte AS (
	INSERT INTO test_read_only_temp VALUES('foo') RETURNING *
)
SELECT * FROM cte;

\c postgres
-- set all nodes rw
SELECT bdr.bdr_node_set_read_only(node_name, false) FROM bdr.bdr_nodes;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

-- cleanup
SELECT bdr.bdr_replicate_ddl_command($$
        DROP TABLE public.test_read_only;
$$);
