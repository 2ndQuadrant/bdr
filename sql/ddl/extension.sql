\c postgres
-- create nonexistant extension
CREATE EXTENSION pg_trgm;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\dx pg_trgm

-- drop and recreate using CINE
DROP EXTENSION pg_trgm;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\dx pg_trgm
CREATE EXTENSION IF NOT EXISTS pg_trgm;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c regression
\dx pg_trgm

-- CINE existing extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;
\c postgres
\dx pg_trgm

DROP EXTENSION pg_trgm;
