-- basic builtin datatypes
CREATE TABLE basic_dml (
    id serial primary key,
    other integer,
    data text,
    something interval
);

-- check basic insert replication
INSERT INTO basic_dml(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- update one row
UPDATE basic_dml SET other = '4', data = NULL, something = '3 days'::interval WHERE id = 4;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- update multiple rows
UPDATE basic_dml SET other = id, data = data || id::text;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- update non-conflicting rows on both sides without wait
UPDATE basic_dml SET other = id, something = something - '10 seconds'::interval WHERE id < 3;
\c regression
UPDATE basic_dml SET other = id, something = something + '10 seconds'::interval WHERE id > 3;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- delete one row
DELETE FROM basic_dml WHERE id = 2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- delete multiple rows
DELETE FROM basic_dml WHERE id < 4;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- truncate
TRUNCATE basic_dml;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, other, data, something FROM basic_dml ORDER BY id;

-- copy
\COPY basic_dml FROM STDIN WITH CSV
9000,1,aaa,1 hour
9001,2,bbb,2 years
9002,3,ccc,3 minutes
9003,4,ddd,4 days
\.
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, other, data, something FROM basic_dml ORDER BY id;

DROP TABLE basic_dml;
