-- test for RT-#37869

CREATE TABLE add_column (
    id serial primary key,
    data text
);

INSERT INTO add_column (data) SELECT generate_series(1,100,10);

ALTER TABLE add_column ADD COLUMN other varchar(100);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
SELECT id, data, other FROM add_column ORDER BY id;

UPDATE add_column SET other = 'foobar';

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
SELECT id, data, other FROM add_column ORDER BY id;

