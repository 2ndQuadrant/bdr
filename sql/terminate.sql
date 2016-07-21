-- We're one instance with two databases so we should
-- have two walsenders and two apply workers.

CREATE FUNCTION wait_for_nwalsenders(nsenders integer)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  WHILE (SELECT count(1) FROM pg_stat_get_wal_senders() s) != nsenders
  LOOP
    PERFORM pg_sleep(0.2);
    PERFORM pg_stat_clear_snapshot();
  END LOOP;
END;
$$;


CREATE FUNCTION wait_for_nworkers(nsenders integer)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  WHILE (SELECT count(1) FROM pg_stat_activity WHERE application_name LIKE 'bdr (%): apply') != nsenders
  LOOP
    PERFORM pg_sleep(0.2);
    PERFORM pg_stat_clear_snapshot();
  END LOOP;
END;
$$;


SELECT wait_for_nwalsenders(2);
SELECT wait_for_nworkers(2);

BEGIN; SET LOCAL bdr.permit_unsafe_ddl_commands = true; SELECT bdr._test_pause_worker_management(true); COMMIT;

-- Must report 't' for all except our own
SELECT
  n.node_name,
  bdr.terminate_apply_workers(n.node_name)
FROM bdr.bdr_nodes n
ORDER BY node_name;

-- One worker should vanish and not have restarted because of the timer
SELECT wait_for_nworkers(1);

-- Wait for reconnect. No need for bdr_connections_changed()
-- since this'll just stop the apply workers quitting as soon
-- as they launch.
BEGIN; SET LOCAL bdr.permit_unsafe_ddl_commands = true; SELECT bdr._test_pause_worker_management(false); COMMIT;

SELECT wait_for_nworkers(2);

BEGIN; SET LOCAL bdr.permit_unsafe_ddl_commands = true; SELECT bdr._test_pause_worker_management(true); COMMIT;

-- terminate walsenders, this time by ID
SELECT
  n.node_name,
  bdr.terminate_walsender_workers(node_sysid, node_timeline, node_dboid)
FROM bdr.bdr_nodes n
ORDER BY node_name;

-- One left
SELECT wait_for_nwalsenders(1);

-- OK, let them come back up
BEGIN; SET LOCAL bdr.permit_unsafe_ddl_commands = true; SELECT bdr._test_pause_worker_management(false); COMMIT;

SELECT wait_for_nwalsenders(2);
