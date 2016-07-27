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

CREATE FUNCTION bdr.bdr_parse_replident_name(
    replident text,
    remote_sysid OUT text,
    remote_timeline OUT oid,
    remote_dboid OUT oid,
    local_dboid OUT oid,
    replication_name OUT name
)
RETURNS record
LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_parse_replident_name_sql';

COMMENT ON FUNCTION bdr.bdr_parse_replident_name(text)
IS 'Parse a replication identifier name from the bdr plugin and report the embedded field values';

CREATE FUNCTION bdr.bdr_format_replident_name(
    remote_sysid text,
    remote_timeline oid,
    remote_dboid oid,
    local_dboid oid,
    replication_name name DEFAULT ''
)
RETURNS text
LANGUAGE c STRICT IMMUTABLE AS 'MODULE_PATHNAME','bdr_format_replident_name_sql';

COMMENT ON FUNCTION bdr.bdr_format_replident_name(text, oid, oid, oid, name)
IS 'Format a BDR replication identifier name from node identity parameters';

--
-- Completely de-BDR-ize a node
--
CREATE OR REPLACE FUNCTION bdr.remove_bdr_from_local_node(force boolean DEFAULT false, convert_global_sequences boolean DEFAULT true)
RETURNS void
LANGUAGE plpgsql
SET bdr.skip_ddl_locking = on
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET search_path = 'bdr,pg_catalog'
AS $$
DECLARE
  local_node_status "char";
  _seqschema name;
  _seqname name;
  _seqmax bigint;
  _tableoid oid;
  _truncate_tg record;
BEGIN

  SELECT node_status FROM bdr.bdr_nodes WHERE (node_sysid, node_timeline, node_dboid) = bdr.bdr_get_local_nodeid()
  INTO local_node_status;

  IF NOT (local_node_status = 'k' OR local_node_status IS NULL) THEN
    IF force THEN
      RAISE WARNING 'forcing deletion of possibly active BDR node';

      UPDATE bdr.bdr_nodes
      SET node_status = 'k'
      WHERE (node_sysid, node_timeline, node_dboid) = bdr.bdr_get_local_nodeid();

      PERFORM bdr._test_pause_worker_management(false);

      PERFORM pg_sleep(5);

      RAISE NOTICE 'node forced to parted state, now removing';
    ELSE
      RAISE EXCEPTION 'this BDR node might still be active, not removing';
    END IF;
  END IF;

  RAISE NOTICE 'removing BDR from node';

  -- Alter all global sequences to become local sequences.  That alone won't
  -- they're in the right position, since another node might've had numerically
  -- higher global sequence values. So we need to then move it up to the
  -- highest allocated chunk for any node and setval to it.
  IF convert_global_sequences THEN 
    FOR _seqschema, _seqname, _seqmax IN
      SELECT
        n.nspname,
        c.relname,
        (
          SELECT max(upper(seqrange))
          FROM bdr.bdr_sequence_values
          WHERE seqschema = n.nspname
            AND seqname = c.relname
            AND in_use
        ) AS seqmax
      FROM pg_class c
      INNER JOIN pg_namespace n ON (c.relnamespace = n.oid)
      WHERE c.relkind = 'S'
        AND c.relam = (SELECT s.oid FROM pg_seqam s WHERE s.seqamname = 'bdr')
    LOOP
      EXECUTE format('ALTER SEQUENCE %I.%I USING local;', _seqschema, _seqname);
      -- This shouldn't be necessary, see bug #215
      IF _seqmax IS NOT NULL THEN
        EXECUTE format('SELECT setval(%L, $1)', quote_ident(_seqschema)||'.'||quote_ident(_seqname)) USING (_seqmax);
      END IF;
    END LOOP;
  ELSE
    RAISE NOTICE 'global sequences not converted to local; they will not work until a new nodegroup is created';
  END IF;

  -- Strip the database security label
  EXECUTE format('SECURITY LABEL FOR bdr ON DATABASE %I IS NULL', current_database());

  -- Suspend worker management, so when we terminate apply workers and
  -- walsenders they won't get relaunched.
  PERFORM bdr._test_pause_worker_management(true);

  -- Terminate every worker associated with this DB
  PERFORM bdr.terminate_walsender_workers(node_sysid, node_timeline, node_dboid)
  FROM bdr.bdr_nodes
  WHERE (node_sysid, node_timeline, node_dboid) <> bdr.bdr_get_local_nodeid();

  PERFORM bdr.terminate_apply_workers(node_sysid, node_timeline, node_dboid)
  FROM bdr.bdr_nodes
  WHERE (node_sysid, node_timeline, node_dboid) <> bdr.bdr_get_local_nodeid();

  -- Delete all connections and all nodes except the current one
  DELETE FROM bdr.bdr_connections
  WHERE (conn_sysid, conn_timeline, conn_dboid) <> bdr.bdr_get_local_nodeid();

  DELETE FROM bdr.bdr_nodes
  WHERE (node_sysid, node_timeline, node_dboid) <> bdr.bdr_get_local_nodeid();

  -- Let the perdb worker resume work and figure out everything's
  -- going away.
  PERFORM bdr._test_pause_worker_management(false);
  PERFORM bdr.bdr_connections_changed();

  -- Give it a few seconds
  PERFORM pg_sleep(2);

  -- Shut down the perdb worker
  PERFORM pg_terminate_backend(pid)
  FROM pg_stat_activity, bdr.bdr_get_local_nodeid() ni
  WHERE datname = current_database()
    AND application_name = format('bdr: (%s,%s,%s,): perdb', ni.sysid, ni.timeline, ni.dboid);

  -- Clear out the rest of bdr_nodes and bdr_connections
  DELETE FROM bdr.bdr_nodes;
  DELETE FROM bdr.bdr_connections;

  -- Drop peer replication slots for this DB
  PERFORM pg_drop_replication_slot(slot_name)
  FROM pg_catalog.pg_replication_slots,
       bdr.bdr_parse_slot_name(slot_name) ps
  WHERE ps.local_dboid = (select oid from pg_database where datname = current_database());

  -- and replication identifiers
  PERFORM pg_replication_identifier_drop(riname)
  FROM pg_catalog.pg_replication_identifier,
       bdr.bdr_parse_replident_name(riname) pi
  WHERE pi.local_dboid = (select oid from pg_database where datname = current_database());

  -- Strip the security labels we use for replication sets from all the tables
  FOR _tableoid IN
    SELECT objoid
    FROM pg_catalog.pg_seclabel
    INNER JOIN pg_catalog.pg_class ON (pg_seclabel.objoid = pg_class.oid)
    WHERE provider = 'bdr'
      AND classoid = 'pg_catalog.pg_class'::regclass
      AND pg_class.relkind = 'r'
  LOOP
    -- regclass's text out adds quoting and schema qualification if needed
    EXECUTE format('SECURITY LABEL FOR bdr ON TABLE %s IS NULL', _tableoid::regclass);
  END LOOP;

  -- Drop the on-truncate triggers. They'd otherwise get cascade-dropped
  -- when the BDR extension was dropped, but this way the system is clean. We
  -- can't drop ones under the 'bdr' schema.
  FOR _truncate_tg IN
    SELECT
      n.nspname AS tgrelnsp,
      c.relname AS tgrelname,
      t.tgname AS tgname,
      d.objid AS tgobjid,
      d.refobjid AS tgrelid
    FROM pg_depend d
    INNER JOIN pg_class c ON (d.refclassid = 'pg_class'::regclass AND d.refobjid = c.oid)
    INNER JOIN pg_namespace n ON (c.relnamespace = n.oid)
    INNER JOIN pg_trigger t ON (d.classid = 'pg_trigger'::regclass and d.objid = t.oid)
    INNER JOIN pg_depend d2 ON (d.classid = d2.classid AND d.objid = d2.objid)
    WHERE tgname LIKE 'truncate_trigger_%'
      AND d2.refclassid = 'pg_proc'::regclass
      AND d2.refobjid = 'bdr.queue_truncate'::regproc
      AND n.nspname <> 'bdr'
  LOOP
    EXECUTE format('DROP TRIGGER %I ON %I.%I',
         _truncate_tg.tgname, _truncate_tg.tgrelnsp, _truncate_tg.tgrelname);

    -- The trigger' dependency entry will be dangling because of how we
    -- dropped it
    DELETE FROM pg_depend
    WHERE classid = 'pg_trigger'::regclass
      AND objid = _truncate_tg.tgobjid
      AND (refclassid = 'pg_proc'::regclass AND refobjid = 'bdr.queue_truncate'::regproc)
          OR
          (refclassid = 'pg_class'::regclass AND refobjid = _truncate_tg.tgrelid);

  END LOOP;

  -- Delete the other detritus from the extension. The user should really drop it,
  -- but we should try to restore a clean state anyway.
  DELETE FROM bdr.bdr_queued_commands;
  DELETE FROM bdr.bdr_queued_drops;
  DELETE FROM bdr.bdr_global_locks;
  DELETE FROM bdr.bdr_conflict_handlers;
  DELETE FROM bdr.bdr_conflict_history;
  DELETE FROM bdr.bdr_replication_set_config;
  DELETE FROM bdr.bdr_sequence_elections;
  DELETE FROM bdr.bdr_sequence_values;
  DELETE FROM bdr.bdr_votes;

  -- We can't drop the BDR extension, we just need to tell the
  -- user to do that.
  RAISE NOTICE 'BDR removed from this node. You can now DROP EXTENSION bdr and, if this is the last BDR node on this PostgreSQL instance, remove bdr from shared_preload_libraries.';
END;
$$;

REVOKE ALL ON FUNCTION bdr.remove_bdr_from_local_node(boolean, boolean) FROM public;

COMMENT ON FUNCTION bdr.remove_bdr_from_local_node(boolean, boolean)
IS 'Remove all BDR security labels, slots, replication origins, replication sets, etc from the local node, and turn all global sequences into local sequences';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
