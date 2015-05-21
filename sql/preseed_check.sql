-- Verify data from preseed.sql has correctly been cloned
\c regression
\d some_local_tbl
SELECT * FROM some_local_tbl ORDER BY id;

\c postgres
\d some_local_tbl
SELECT * FROM some_local_tbl ORDER BY id;
