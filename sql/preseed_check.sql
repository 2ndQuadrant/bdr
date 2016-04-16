-- Verify data from preseed.sql has correctly been cloned
\c regression
\d some_local_tbl
INSERT INTO some_local_tbl(key, data) VALUES('key4', 'data4');
SELECT * FROM some_local_tbl ORDER BY id;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c postgres
\d some_local_tbl
SELECT * FROM some_local_tbl ORDER BY id;
