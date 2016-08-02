CREATE FUNCTION bdr.bdr_is_active_in_db()
RETURNS boolean LANGUAGE c
AS 'MODULE_PATHNAME','bdr_is_active_in_db';

DROP VIEW bdr.bdr_node_slots;

CREATE VIEW bdr.bdr_node_slots AS
SELECT n.node_name,
 s.slot_name, s.restart_lsn AS slot_restart_lsn, s.confirmed_flush_lsn AS slot_confirmed_lsn,
 s.active AS walsender_active,
 s.pid AS walsender_pid,
 r.sent_location, r.write_location, r.flush_location, r.replay_location
FROM
 bdr.pg_replication_slots s
 CROSS JOIN LATERAL bdr.bdr_parse_slot_name(s.slot_name) ps(remote_sysid, remote_timeline, remote_dboid, local_dboid, replication_name)
 INNER JOIN bdr.bdr_nodes n ON ((n.node_sysid = ps.remote_sysid) AND (n.node_timeline = ps.remote_timeline) AND (n.node_dboid = ps.remote_dboid))
 LEFT JOIN pg_catalog.pg_stat_replication r ON (r.pid = s.pid)
WHERE ps.local_dboid = (select oid from pg_database where datname = current_database())
  AND s.plugin = 'bdr';

