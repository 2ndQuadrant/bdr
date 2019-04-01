SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SET LOCAL bdr.skip_ddl_replication = true;
SET LOCAL search_path = bdr;

--
-- Completely de-BDR-ize a node
-- Updated for #281
-- Updated to backport db753837d from bdr2 to fix pg_depend issues
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
  WHERE ps.local_dboid = (select oid from pg_database where datname = current_database())
       AND plugin = 'bdr';

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
      AND ((refclassid = 'pg_proc'::regclass AND refobjid = 'bdr.queue_truncate'::regproc)
          OR
          (refclassid = 'pg_class'::regclass AND refobjid = _truncate_tg.tgrelid));

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


CREATE VIEW bdr.replication_set_tables AS
WITH
  bn(node_name) AS (SELECT bdr.bdr_get_local_node_name()),
  m(nspname, relname, memberships) AS (
        SELECT n.nspname, c.relname, b.memberships
        FROM pg_class c
        INNER JOIN pg_namespace n ON c.relnamespace = n.oid
        CROSS JOIN LATERAL bdr.table_get_replication_sets(c.oid::regclass) AS b(memberships)
        WHERE c.relkind = 'r' AND c.relpersistence = 'p'
        AND n.nspname !~ ANY (ARRAY['^pg_catalog$', '^bdr$', '^information_schema$', '^pg_temp.*'])
    ),
  rcm(nspname, relname, memberships, replicate_inserts, replicate_updates, replicate_deletes) AS (
    SELECT m.nspname, m.relname, m.memberships,
      coalesce(bool_or(rc.replicate_inserts), 't'),
      coalesce(bool_or(rc.replicate_updates), 't'),
      coalesce(bool_or(rc.replicate_deletes), 't')
    FROM m
    LEFT JOIN bdr.bdr_replication_set_config rc
      ON (rc.set_name = ANY (m.memberships))
    GROUP BY m.nspname, m.relname, m.memberships
  )
SELECT bn.node_name, rcm.*
FROM bn CROSS JOIN rcm;

REVOKE ALL ON TABLE bdr.replication_set_tables FROM public;

CREATE VIEW bdr.replication_set_nodes AS
SELECT
  n.node_name, c.conn_replication_sets, o.node_name AS origin_node_name
FROM bdr.bdr_nodes n
INNER JOIN bdr.bdr_connections c
  ON (n.node_sysid, n.node_timeline, n.node_dboid) = (c.conn_sysid, c.conn_timeline, c.conn_dboid)
LEFT JOIN bdr.bdr_nodes o
  ON (c.conn_origin_sysid, c.conn_origin_timeline, c.conn_origin_dboid) = (o.node_sysid, o.node_timeline, o.node_dboid)
WHERE n.node_status != 'k';

REVOKE ALL ON TABLE bdr.replication_set_nodes FROM public;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
