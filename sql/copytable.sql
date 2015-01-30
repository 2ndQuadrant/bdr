\c postgres

CREATE TABLE testtab (x integer, blah text);

INSERT INTO testtab(x, blah) SELECT x, repeat('blah'||x, x) FROM generate_series(1,10) x;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c regression

CREATE TABLE testtab (x integer, blah text);

CREATE FUNCTION bdr_copytable_test(cstring,cstring,cstring,cstring) RETURNS void LANGUAGE c AS 'bdr.so','bdr_copytable_test';

SELECT bdr_copytable_test(
	'dbname=postgres',
	'dbname=regression',
	'COPY public.testtab TO stdout',
	'COPY public.testtab FROM stdin');

-- Shouldn't be needed. Wha?
SELECT pg_sleep(1);

SELECT * FROM testtab ORDER BY x;

DROP FUNCTION bdr_copytable_test(cstring,cstring,cstring,cstring);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

SELECT * FROM testtab ORDER BY x;
