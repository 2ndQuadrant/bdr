-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE FUNCTION bdr.bdr_parse_slot_name(
    slot_name name,
    remote_sysid OUT text,
    remote_timeline OUT oid,
    remote_dboid OUT oid,
    local_dboid OUT oid,
    replication_name OUT name
)
RETURNS record
LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_parse_slot_name_sql';

COMMENT ON FUNCTION bdr.bdr_parse_slot_name(name)
IS 'Parse a slot name from the bdr plugin and report the embedded field values';

CREATE FUNCTION bdr.bdr_format_slot_name(
    remote_sysid text,
    remote_timeline oid,
    remote_dboid oid,
    local_dboid oid,
    replication_name name DEFAULT ''
)
RETURNS name
LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_format_slot_name_sql';

COMMENT ON FUNCTION bdr.bdr_format_slot_name(text, oid, oid, oid, name)
IS 'Format a BDR slot name from node identity parameters';

CREATE VIEW bdr_node_slots AS
SELECT n.node_name, s.slot_name
FROM
  pg_catalog.pg_replication_slots s,
  bdr_nodes n,
  LATERAL bdr_parse_slot_name(s.slot_name) ps
WHERE (n.node_sysid, n.node_timeline, n.node_dboid)
    = (ps.remote_sysid, ps.remote_timeline, ps.remote_dboid);

CREATE FUNCTION bdr.bdr_get_local_node_name() RETURNS text
LANGUAGE sql
AS $$
SELECT node_name
FROM bdr.bdr_nodes n,
     bdr.bdr_get_local_nodeid() i
WHERE n.node_sysid = i.sysid
  AND n.node_timeline = i.timeline
  AND n.node_dboid = i.dboid;
$$;

COMMENT ON FUNCTION bdr.bdr_get_local_node_name()
IS 'Return the name from bdr.bdr_nodes for the local node, or null if no entry exists';

-- See https://github.com/2ndQuadrant/bdr/issues/5
ALTER FUNCTION bdr.queue_truncate()
SECURITY DEFINER
SET search_path = 'bdr';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
