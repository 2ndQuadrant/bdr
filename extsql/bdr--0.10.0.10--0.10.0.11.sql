SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE OR REPLACE FUNCTION bdr.connection_get_replication_sets(
    sysid text, timeline oid, dboid oid,
    origin_sysid text default '0',
    origin_timeline oid default 0,
    origin_dboid oid default 0
)
RETURNS text[]
LANGUAGE plpgsql
AS $$
DECLARE
  found_sets text[];
BEGIN
  SELECT conn_replication_sets
  FROM bdr.bdr_connections
  WHERE conn_sysid = sysid
    AND conn_timeline = timeline
    AND conn_dboid = dboid
    AND conn_origin_sysid = origin_sysid
    AND conn_origin_timeline = origin_timeline
    AND conn_origin_dboid = origin_dboid
  INTO found_sets;

  IF NOT FOUND THEN
    IF origin_timeline <> '0' OR origin_timeline <> 0 OR origin_dboid <> 0 THEN
      RAISE EXCEPTION 'No bdr.bdr_connections entry found from origin (%,%,%) to (%,%,%)',
      	origin_sysid, origin_timeline, origin_dboid, sysid, timeline, dboid;
    ELSE
      RAISE EXCEPTION 'No bdr.bdr_connections entry found for (%,%,%) with default origin (0,0,0)',
      	sysid, timeline, dboid;
    END IF;
  END IF;

  RETURN found_sets;
END;
$$;

CREATE OR REPLACE FUNCTION bdr.connection_set_replication_sets(
    new_replication_sets text[],
    sysid text, timeline oid, dboid oid,
    origin_sysid text default '0',
    origin_timeline oid default 0,
    origin_dboid oid default 0
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE bdr.bdr_connections
  SET conn_replication_sets = new_replication_sets
  WHERE conn_sysid = sysid
    AND conn_timeline = timeline
    AND conn_dboid = dboid
    AND conn_origin_sysid = origin_sysid
    AND conn_origin_timeline = origin_timeline
    AND conn_origin_dboid = origin_dboid;

  IF NOT FOUND THEN
    IF origin_timeline <> '0' OR origin_timeline <> 0 OR origin_dboid <> 0 THEN
      RAISE EXCEPTION 'No bdr.bdr_connections entry found from origin (%,%,%) to (%,%,%)',
      	origin_sysid, origin_timeline, origin_dboid, sysid, timeline, dboid;
    ELSE
      RAISE EXCEPTION 'No bdr.bdr_connections entry found for (%,%,%) with default origin (0,0,0)',
      	sysid, timeline, dboid;
    END IF;
  END IF;

  -- The other nodes will notice the change when they replay the new tuple; we
  -- only have to explicitly notify the local node.
  PERFORM bdr.bdr_connections_changed();
END;
$$;

CREATE OR REPLACE FUNCTION bdr.connection_set_replication_sets(replication_sets text[], target_node_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  sysid text;
  timeline oid;
  dboid oid;
BEGIN
  SELECT node_sysid, node_timeline, node_dboid
  FROM bdr.bdr_nodes
  WHERE node_name = target_node_name
  INTO sysid, timeline, dboid;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No node with name % found in bdr.bdr_nodes',target_node_name;
  END IF;

  IF (
    SELECT count(1)
    FROM bdr.bdr_connections
    WHERE conn_sysid = sysid
      AND conn_timeline = timeline
      AND conn_dboid = dboid
    ) > 1
  THEN
    RAISE WARNING 'There are node-specific override entries for node % in bdr.bdr_connections. Only the default connection''s replication sets will be changed. Use the 6-argument form of this function to change others.';
  END IF;

  PERFORM bdr.connection_set_replication_sets(replication_sets, sysid, timeline, dboid);
END;
$$;

CREATE OR REPLACE FUNCTION bdr.connection_get_replication_sets(target_node_name text)
RETURNS text[]
LANGUAGE plpgsql
AS $$
DECLARE
  sysid text;
  timeline oid;
  dboid oid;
  replication_sets text[];
BEGIN
  SELECT node_sysid, node_timeline, node_dboid
  FROM bdr.bdr_nodes
  WHERE node_name = target_node_name
  INTO sysid, timeline, dboid;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No node with name % found in bdr.bdr_nodes',target_node_name;
  END IF;

  IF (
    SELECT count(1)
    FROM bdr.bdr_connections
    WHERE conn_sysid = sysid
      AND conn_timeline = timeline
      AND conn_dboid = dboid
    ) > 1
  THEN
    RAISE WARNING 'There are node-specific override entries for node % in bdr.bdr_connections. Only the default connection''s replication sets will be returned.';
  END IF;

  SELECT bdr.connection_get_replication_sets(sysid, timeline, dboid) INTO replication_sets;
  RETURN replication_sets;
END;
$$;

CREATE FUNCTION bdr._test_pause_worker_management(boolean)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME','bdr_pause_worker_management';

COMMENT ON FUNCTION bdr._test_pause_worker_management(boolean)
IS 'BDR-internal function for test use only';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
