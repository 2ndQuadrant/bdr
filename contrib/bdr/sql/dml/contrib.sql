-- contrib datatypes
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE contrib_dml (
    id serial primary key,
    fixed cube,
    variable hstore
);

-- check basic insert replication
INSERT INTO contrib_dml(fixed, variable)
VALUES ('(1,2)', 'a=>1,b=>2'),
       ('(3,4)', 'c=>3,d=>4'),
       ('(5,6)', 'e=>5,f=>6'),
       ('(7,8)', 'g=>7,h=>8'),
       ('(1,2,3)', 'a=>1,b=>2,c=>3'),
       ('(4,5,6)', 'c=>1,d=>2,e=>3'),
       (NULL, NULL);
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, fixed, variable FROM contrib_dml ORDER BY id;

-- update one row
UPDATE contrib_dml SET fixed = '(1,2,3,4)', variable = 'a=>NULL,b=>1' WHERE id = 1;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, fixed, variable FROM contrib_dml ORDER BY id;

-- update multiple rows
UPDATE contrib_dml SET fixed = cube_enlarge(fixed, 1, 1), variable = variable || 'x=>99' WHERE '(1,6)' && fixed;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, fixed, variable FROM contrib_dml ORDER BY id;

-- delete one row
DELETE FROM contrib_dml WHERE id = 2;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, fixed, variable FROM contrib_dml ORDER BY id;

-- delete multiple rows
DELETE FROM contrib_dml WHERE id < 4;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c postgres
SELECT id, fixed, variable FROM contrib_dml ORDER BY id;

-- truncate
TRUNCATE contrib_dml;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
\c regression
SELECT id, fixed, variable FROM contrib_dml ORDER BY id;

DROP TABLE contrib_dml;
