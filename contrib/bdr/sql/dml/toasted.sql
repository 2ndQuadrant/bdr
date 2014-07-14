CREATE TABLE toasted (
    id serial primary key,
    other text,
    data text
);

ALTER TABLE toasted ALTER COLUMN data SET STORAGE EXTERNAL;

-- check replication of toast values
INSERT INTO toasted(other, data) VALUES('foo', repeat('1234567890', 300));

-- check that unchanged toast values work correctly
UPDATE toasted SET other = 'foo2';

-- check that changed toast values are replicated
UPDATE toasted SET other = 'foo3', data = '-'||data;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;
SELECT * FROM toasted ORDER BY id;

DROP TABLE toasted;
