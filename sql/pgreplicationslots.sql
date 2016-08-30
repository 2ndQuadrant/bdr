--
-- Access the extended bdr.pg_replication_slots view
-- with the data backported from 9.5 and 9.6.
--

SELECT 1
FROM pg_stat_replication r
INNER JOIN bdr.pg_replication_slots s ON (r.pid = s.active_pid)
WHERE confirmed_flush_lsn IS NOT NULL;
