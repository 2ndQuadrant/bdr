-- We're one instance with two databases so we should
-- have two walsenders and two apply workers.

SELECT count(pid) FROM pg_stat_activity WHERE application_name LIKE 'bdr (%): apply';

-- Must report 't' for all except our own
SELECT
  n.node_name,
  bdr.terminate_apply_workers(n.node_name)
FROM bdr.bdr_nodes n
ORDER BY node_name;

-- One worker should vanish and not have restarted because of the timer
SELECT count(pid) FROM pg_stat_activity WHERE application_name LIKE 'bdr (%): apply';

-- Reconnect... (would like a faster way to do this?)
SELECT pg_sleep(12);

-- Should have two walsenders
SELECT count(pid) from pg_stat_replication WHERE application_name LIKE 'bdr (%):receive';

-- terminate walsenders, this time by ID
SELECT
  n.node_name,
  bdr.terminate_walsender_workers(node_sysid, node_timeline, node_dboid)
FROM bdr.bdr_nodes n
ORDER BY node_name;

-- One left
SELECT count(pid) from pg_stat_replication WHERE application_name LIKE 'bdr (%):receive';

-- Then allow reconnect before continuing tests
SELECT pg_sleep(12);

-- Really back?
SELECT count(pid) from pg_stat_replication WHERE application_name LIKE 'bdr (%):receive';
