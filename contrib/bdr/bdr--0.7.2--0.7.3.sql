SET LOCAL search_path = bdr;
-- We must be able to use exclusion constraints for global sequences
SET bdr.permit_unsafe_ddl_commands=true;

--
-- To permit an upgrade to 0.9.x we must update the catalogs to be 0.9.x compatible.
--

--
-- Create bdr.bdr_connections
--
-- 0.7.x won't use it, but it must exist and match in definition so we can replicate
-- rows from 0.9.x when we receive them.
--
CREATE TABLE bdr.bdr_connections(
	conn_sysid             text    not null,
	conn_timeline          oid     not null,
	conn_dboid             oid     not null,
	conn_origin_sysid      text    not null,
	conn_origin_timeline   oid     not null,
	conn_origin_dboid      oid     not null,
	conn_is_unidirectional boolean not null default false,
	conn_dsn               text    not null,
	conn_apply_delay       integer,
	conn_replication_sets  text[],
	PRIMARY KEY (conn_sysid, conn_timeline, conn_dboid, conn_origin_sysid, conn_origin_timeline, conn_origin_dboid)
);

SELECT pg_catalog.pg_extension_config_dump('bdr_connections', '');

-- Modify bdr.bdr_nodes
--
-- 0.7.x ignores the new columns, but they must exist for 0.9.x rows to replicate
--

ALTER TABLE bdr.bdr_nodes
  ADD COLUMN node_name          text,
  ADD COLUMN node_local_dsn     text,
  ADD COLUMN node_init_from_dsn text,
  ADD COLUMN node_read_only     boolean default false;

CREATE FUNCTION node_name_gen_tg() RETURNS trigger
LANGUAGE plpgsql AS
$$
BEGIN
  NEW.node_name := format('bdr07_%s_%s_%s', NEW.node_sysid, NEW.node_timeline, NEW.node_dboid);
  RETURN NEW;
END;
$$;

CREATE TRIGGER bdr_nodes_name_gen 
BEFORE INSERT OR UPDATE ON bdr.bdr_nodes
FOR EACH ROW EXECUTE PROCEDURE node_name_gen_tg();

-- Add dummies for some functions BDR 0.9.x calls. They won't do anything and we'll
-- have to add the 0.7.x node's connections manually still, they're just there to satisfy
-- 0.9.x.
--
-- We don't have to worry about upgradeability because these don't get replicated
-- and the 0.9.x nodes have their own extension schema.

CREATE FUNCTION bdr.bdr_connections_changed()
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'bdr.bdr_connections_changed() request ignored';
END;
$$;

-- I've removed use of bdr.bdr_variant(), bdr.bdr_get_local_nodeid(),
-- bdr.bdr_min_remote_version_num(), bdr.bdr_version_num(), etc from
-- bdr_get_remote_nodeinfo_internal and we already have bdr.bdr_version()

--
-- bdr.bdr_group_join expects to be able to call bdr.bdr_test_remote_connectback
-- which calls back into us to invoke bdr.bdr_test_replication_connection (results discarded)
-- and bdr.bdr_get_remote_nodeinfo(). It also expects to return the caller's
-- (sysid,timeline,dboid) which we can't obtain without backporting a bunch of code,
-- so hack 0.10.x to accept (NULL,NULL,NULL) and provide a dummy function.
--
CREATE FUNCTION bdr.bdr_test_replication_connection(dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'bdr.bdr_test_replication_connection(dsn) call ignored';
  sysid := NULL;
  timeline := NULL;
  dboid := NULL;
  RETURN;
END;
$$;

CREATE FUNCTION bdr.bdr_get_remote_nodeinfo(
	dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid,
	variant OUT text, version OUT text, version_num OUT integer,
	min_remote_version_num OUT integer, is_superuser OUT boolean)
RETURNS record LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'bdr.bdr_get_remote_nodeinfo(dsn) call ignored';
  sysid := NULL;
  timeline := NULL;
  dboid := NULL;
  variant := 'BDR';
  version := NULL;
  version_num := NULL;
  min_remote_version_num := NULL;
  is_superuser := 't';
END;
$$;

CREATE FUNCTION bdr_get_local_nodeid( sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION bdr_variant() RETURNS text LANGUAGE sql
AS $$ SELECT TEXT 'BDR'; $$;

-- The 0.9.x internal_node_join will work verbatim. We ignore
-- the bdr.bdr_connections_changed call, but still replicate it.
CREATE FUNCTION bdr.internal_node_join(
    sysid text, timeline oid, dboid oid,
    node_external_dsn text,
    apply_delay integer,
    replication_sets text[]
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
AS
$body$
DECLARE
    status "char";
BEGIN
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    IF bdr_variant() <> 'BDR' THEN
        RAISE USING
            MESSAGE = 'Full BDR required but this module is built for '||bdr_variant(),
            DETAIL = 'The target node is running something other than full BDR so you cannot join a BDR node to it',
            HINT = 'Install full BDR if possible or use the UDR functions.',
            ERRCODE = 'feature_not_supported';
    END IF;

    -- Assert that we have a bdr_nodes entry with state = i on this node
    SELECT INTO status
    FROM bdr.bdr_nodes
    WHERE node_sysid = sysid
      AND node_timeline = timeline
      AND node_dboid = dboid;

    IF NOT FOUND THEN
        RAISE object_not_in_prerequisite_state
              USING MESSAGE = format('bdr.bdr_nodes entry for (%s,%s,%s) not found',
                                     sysid, timeline, dboid);
    END IF;

    IF status <> 'i' THEN
        RAISE object_not_in_prerequisite_state
              USING MESSAGE = format('bdr.bdr_nodes entry for (%s,%s,%s) has unexpected status %L (expected ''i'')',
                                     sysid, timeline, dboid, status);
    END IF;

    -- Insert or Update the connection info on this node, which we must be
    -- initing from.
    -- No need to care about concurrency here as we hold EXCLUSIVE LOCK.
    BEGIN
        INSERT INTO bdr.bdr_connections
        (conn_sysid, conn_timeline, conn_dboid,
         conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,
         conn_dsn,
         conn_apply_delay, conn_replication_sets,
         conn_is_unidirectional)
        VALUES
        (sysid, timeline, dboid,
         '0', 0, 0,
         node_external_dsn,
         CASE WHEN apply_delay = -1 THEN NULL ELSE apply_delay END,
         replication_sets, false);
    EXCEPTION WHEN unique_violation THEN
        UPDATE bdr.bdr_connections
        SET conn_dsn = node_external_dsn,
            conn_apply_delay = CASE WHEN apply_delay = -1 THEN NULL ELSE apply_delay END,
            conn_replication_sets = replication_sets,
            conn_is_unidirectional = false
        WHERE conn_sysid = sysid
          AND conn_timeline = timeline
          AND conn_dboid = dboid
          AND conn_origin_sysid = '0'
          AND conn_origin_timeline = 0
          AND conn_origin_dboid = 0;
    END;

    -- Schedule the apply worker launch for commit time
    PERFORM bdr.bdr_connections_changed();

    -- and ensure the apply worker is launched on other nodes
    -- when this transaction replicates there, too.
    INSERT INTO bdr.bdr_queued_commands
    (lsn, queued_at, perpetrator, command_tag, command)
    VALUES
    (pg_current_xlog_insert_location(), current_timestamp, current_user,
    'SELECT', 'SELECT bdr.bdr_connections_changed()');
END;
$body$;


SET bdr.permit_unsafe_ddl_commands = false;
RESET search_path;
