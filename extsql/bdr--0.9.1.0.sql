--\echo Use "CREATE EXTENSION bdr" to load this file. \quit

--CREATE ROLE bdr NOLOGIN SUPERUSER;
--SET ROLE bdr;

CREATE SCHEMA bdr;
GRANT USAGE ON SCHEMA bdr TO public;

SET LOCAL search_path = bdr;
-- We must be able to use exclusion constraints for global sequences
SET bdr.permit_unsafe_ddl_commands = true;
-- We don't want to replicate commands from in here
SET bdr.skip_ddl_replication = true;
CREATE OR REPLACE FUNCTION bdr_version()
RETURNS TEXT
LANGUAGE C
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION bdr_variant()
RETURNS TEXT
LANGUAGE C
AS 'MODULE_PATHNAME'
;

CREATE FUNCTION pg_stat_get_bdr(
    OUT rep_node_id oid,
    OUT rilocalid oid,
    OUT riremoteid text,
    OUT nr_commit int8,
    OUT nr_rollback int8,
    OUT nr_insert int8,
    OUT nr_insert_conflict int8,
    OUT nr_update int8,
    OUT nr_update_conflict int8,
    OUT nr_delete int8,
    OUT nr_delete_conflict int8,
    OUT nr_disconnect int8
)
RETURNS SETOF record
LANGUAGE C
AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION pg_stat_get_bdr() FROM PUBLIC;

CREATE VIEW pg_stat_bdr AS SELECT * FROM pg_stat_get_bdr();


CREATE TABLE bdr_sequence_values
(
    owning_sysid text NOT NULL COLLATE "C",
    owning_tlid oid NOT NULL,
    owning_dboid oid NOT NULL,
    owning_riname text NOT NULL COLLATE "C",

    seqschema text NOT NULL COLLATE "C",
    seqname text NOT NULL COLLATE "C",
    seqrange int8range NOT NULL,

    -- could not acquire chunk
    failed bool NOT NULL DEFAULT false,

    -- voting successfull
    confirmed bool NOT NULL,

    -- empty, not referenced
    emptied bool NOT NULL CHECK(NOT emptied OR confirmed),

    -- used in sequence
    in_use bool NOT NULL CHECK(NOT in_use OR confirmed),

    EXCLUDE USING gist(seqschema WITH =, seqname WITH =, seqrange WITH &&) WHERE (confirmed),
    PRIMARY KEY(owning_sysid, owning_tlid, owning_dboid, owning_riname, seqschema, seqname, seqrange)
);
REVOKE ALL ON TABLE bdr_sequence_values FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_sequence_values', '');

CREATE INDEX bdr_sequence_values_chunks ON bdr_sequence_values(seqschema, seqname, seqrange);
CREATE INDEX bdr_sequence_values_newchunk ON bdr_sequence_values(seqschema, seqname, upper(seqrange));

CREATE TABLE bdr_sequence_elections
(
    owning_sysid text NOT NULL COLLATE "C",
    owning_tlid oid NOT NULL,
    owning_dboid oid NOT NULL,
    owning_riname text NOT NULL COLLATE "C",
    owning_election_id bigint NOT NULL,

    seqschema text NOT NULL COLLATE "C",
    seqname text NOT NULL COLLATE "C",
    seqrange int8range NOT NULL,

    /* XXX id */

    vote_type text NOT NULL COLLATE "C",

    open bool NOT NULL,
    success bool NOT NULL DEFAULT false,

    PRIMARY KEY(owning_sysid, owning_tlid, owning_dboid, owning_riname, seqschema, seqname, seqrange)
);
REVOKE ALL ON TABLE bdr_sequence_values FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_sequence_elections', '');

CREATE INDEX bdr_sequence_elections__open_by_sequence ON bdr.bdr_sequence_elections USING gist(seqschema, seqname, seqrange) WHERE open;
CREATE INDEX bdr_sequence_elections__by_sequence ON bdr.bdr_sequence_elections USING gist(seqschema, seqname, seqrange);
CREATE INDEX bdr_sequence_elections__owning_election_id ON bdr.bdr_sequence_elections (owning_election_id);
CREATE INDEX bdr_sequence_elections__owner_range ON bdr.bdr_sequence_elections USING gist(owning_election_id, seqrange);

CREATE TABLE bdr_votes
(
    vote_sysid text NOT NULL COLLATE "C",
    vote_tlid oid NOT NULL,
    vote_dboid oid NOT NULL,
    vote_riname text NOT NULL COLLATE "C",
    vote_election_id bigint NOT NULL,

    voter_sysid text NOT NULL COLLATE "C",
    voter_tlid oid NOT NULL,
    voter_dboid oid NOT NULL,
    voter_riname text NOT NULL COLLATE "C",

    vote bool NOT NULL,
    reason text COLLATE "C" CHECK (reason IS NULL OR vote = false),
    UNIQUE(vote_sysid, vote_tlid, vote_dboid, vote_riname, vote_election_id, voter_sysid, voter_tlid, voter_dboid, voter_riname)
);
REVOKE ALL ON TABLE bdr_votes FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_votes', '');

CREATE INDEX bdr_votes__by_voter ON bdr.bdr_votes(voter_sysid, voter_tlid, voter_dboid, voter_riname);

-- register bdr am if seqam is supported
DO $DO$BEGIN
PERFORM 1 FROM pg_catalog.pg_class WHERE relname = 'pg_seqam' AND relnamespace = 11;
IF NOT FOUND THEN
    RETURN;
END IF;

CREATE OR REPLACE FUNCTION bdr_sequence_alloc(INTERNAL)
RETURNS INTERNAL
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION bdr_sequence_setval(INTERNAL)
RETURNS INTERNAL
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION bdr_sequence_options(INTERNAL)
RETURNS INTERNAL
LANGUAGE C
STABLE STRICT
AS 'MODULE_PATHNAME'
;

-- not tracked yet, can we trick pg_depend instead?
DELETE FROM pg_seqam WHERE seqamname = 'bdr';

INSERT INTO pg_seqam(
    seqamname,
    seqamalloc,
    seqamsetval,
    seqamoptions
)
VALUES (
    'bdr',
    'bdr_sequence_alloc',
    'bdr_sequence_setval',
    'bdr_sequence_options'
);
END;$DO$;


CREATE TYPE bdr_conflict_type AS ENUM
(
    'insert_insert',
    'insert_update',
    'update_update',
    'update_delete',
    'delete_delete',
    'unhandled_tx_abort'
);

COMMENT ON TYPE bdr_conflict_type IS 'The nature of a BDR apply conflict - concurrent updates (update_update), conflicting inserts, etc.';

CREATE TYPE bdr.bdr_conflict_handler_action
    AS ENUM('IGNORE', 'ROW', 'SKIP');

CREATE TABLE bdr.bdr_conflict_handlers (
    ch_name NAME NOT NULL,
    ch_type bdr.bdr_conflict_type NOT NULL,
    ch_reloid Oid NOT NULL,
    ch_fun regprocedure NOT NULL,
    ch_timeframe INTERVAL,
    PRIMARY KEY(ch_reloid, ch_name)
) WITH OIDS;
REVOKE ALL ON TABLE bdr_conflict_handlers FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_conflict_handlers', '');

CREATE INDEX bdr_conflict_handlers_ch_type_reloid_idx
    ON bdr_conflict_handlers(ch_reloid, ch_type);

CREATE FUNCTION bdr.bdr_create_conflict_handler(
    ch_rel REGCLASS,
    ch_name NAME,
    ch_proc REGPROCEDURE,
    ch_type bdr.bdr_conflict_type,
    ch_timeframe INTERVAL
)
RETURNS VOID
LANGUAGE C
STRICT
AS 'MODULE_PATHNAME'
;

CREATE FUNCTION bdr.bdr_create_conflict_handler(
    ch_rel REGCLASS,
    ch_name NAME,
    ch_proc REGPROCEDURE,
    ch_type bdr.bdr_conflict_type
)
RETURNS VOID
LANGUAGE C
STRICT
AS 'MODULE_PATHNAME'
;

CREATE FUNCTION bdr.bdr_drop_conflict_handler(ch_rel REGCLASS, ch_name NAME)
RETURNS VOID
LANGUAGE C
STRICT
AS 'MODULE_PATHNAME'
;


CREATE VIEW bdr_list_conflict_handlers(ch_name, ch_type, ch_reloid, ch_fun) AS
    SELECT ch_name, ch_type, ch_reloid, ch_fun, ch_timeframe
    FROM bdr.bdr_conflict_handlers
;


CREATE TYPE bdr_conflict_resolution AS ENUM
(
    'conflict_trigger_skip_change',
    'conflict_trigger_returned_tuple',
    'last_update_wins_keep_local',
    'last_update_wins_keep_remote',
	'apply_change',
	'skip_change',
    'unhandled_tx_abort'
);

COMMENT ON TYPE bdr_conflict_resolution IS 'Resolution of a bdr conflict - if a conflict was resolved by a conflict trigger, by last-update-wins tests on commit timestamps, etc.';

--
-- bdr_conflict_history records apply conflicts so they can be queried and
-- analysed by administrators.
--
-- This must remain in sync with bdr_log_handled_conflict(...) and
-- struct BdrApplyConflict
--

-- when seqam is present, make sure the sequence is using local AM
DO $DO$BEGIN
PERFORM 1 FROM pg_catalog.pg_class WHERE relname = 'pg_seqam' AND relnamespace = 11;
IF FOUND THEN
    EXECUTE 'CREATE SEQUENCE bdr_conflict_history_id_seq USING local';
ELSE
    CREATE SEQUENCE bdr_conflict_history_id_seq;
END IF;
END;$DO$;

CREATE TABLE bdr_conflict_history (
    conflict_id         bigint not null default nextval('bdr_conflict_history_id_seq'),
    local_node_sysid    text not null, -- really uint64 but we don't have the type for it
    PRIMARY KEY (local_node_sysid, conflict_id),

    local_conflict_xid  xid not null,     -- xid of conflicting apply tx
    local_conflict_lsn  pg_lsn not null,  -- lsn of local node at the time the conflict was detected
    local_conflict_time timestamptz not null,
    object_schema       text,
    object_name         text,
    remote_node_sysid   text not null, -- again, really uint64
    remote_txid         xid not null,
    remote_commit_time  timestamptz not null,
    remote_commit_lsn   pg_lsn not null,
    conflict_type       bdr_conflict_type not null,
    conflict_resolution bdr_conflict_resolution not null,
    local_tuple         json,
    remote_tuple        json,
    local_tuple_xmin    xid,
    local_tuple_origin_sysid text,        -- also really uint64

    -- The following apply only for unhandled apply errors and
    -- correspond to fields in ErrorData in elog.h .
    error_message       text,
    error_sqlstate      text CHECK (length(error_sqlstate) = 5),
    error_querystring   text,
    error_cursorpos     integer,
    error_detail        text,
    error_hint          text,
    error_context       text,
    error_columnname    text, -- schema and table in object_schema, object_name above
    error_typename      text,
    error_constraintname text,
    error_filename      text,
    error_lineno        integer,
    error_funcname      text
);
REVOKE ALL ON TABLE bdr_conflict_history FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_conflict_history', '');

ALTER SEQUENCE bdr_conflict_history_id_seq OWNED BY bdr_conflict_history.conflict_id;

COMMENT ON TABLE bdr_conflict_history IS 'Log of all conflicts in this BDR group';
COMMENT ON COLUMN bdr_conflict_history.local_node_sysid IS 'sysid of the local node where the apply conflict occurred';
COMMENT ON COLUMN bdr_conflict_history.remote_node_sysid IS 'sysid of the remote node the conflicting transaction originated from';
COMMENT ON COLUMN bdr_conflict_history.object_schema IS 'Schema of the object involved in the conflict';
COMMENT ON COLUMN bdr_conflict_history.object_name IS 'Name of the object (table, etc) involved in the conflict';
COMMENT ON COLUMN bdr_conflict_history.local_conflict_xid IS 'Transaction ID of the apply transaction that encountered the conflict';
COMMENT ON COLUMN bdr_conflict_history.local_conflict_lsn IS 'xlog position at the time the conflict occured on the applying node';
COMMENT ON COLUMN bdr_conflict_history.local_conflict_time IS 'The time the conflict was detected on the applying node';
COMMENT ON COLUMN bdr_conflict_history.remote_txid IS 'xid of the remote transaction involved in the conflict';
COMMENT ON COLUMN bdr_conflict_history.remote_commit_time IS 'The time the remote transaction involved in this conflict committed';
COMMENT ON COLUMN bdr_conflict_history.remote_commit_lsn IS 'LSN on remote node at which conflicting transaction committed';
COMMENT ON COLUMN bdr_conflict_history.conflict_type IS 'Nature of the conflict - insert/insert, update/delete, etc';
COMMENT ON COLUMN bdr_conflict_history.local_tuple IS 'For DML conflicts, the conflicting tuple from the local DB (as json), if logged';
COMMENT ON COLUMN bdr_conflict_history.local_tuple_xmin IS 'If local_tuple is set, the xmin of the conflicting local tuple';
COMMENT ON COLUMN bdr_conflict_history.local_tuple_origin_sysid IS 'The node id for the true origin of the local tuple. Differs from local_node_sysid if the tuple was originally replicated from another node.';
COMMENT ON COLUMN bdr_conflict_history.remote_tuple IS 'For DML conflicts, the conflicting tuple from the remote DB (as json), if logged';
COMMENT ON COLUMN bdr_conflict_history.conflict_resolution IS 'How the conflict was resolved/handled; see the enum definition';
COMMENT ON COLUMN bdr_conflict_history.error_message IS 'On apply error, the error message from ereport/elog. Other error fields match.';

-- The bdr_nodes table tracks members of a BDR group; it's only concerned with
-- one bdr group so it only has to track enough to uniquely identify each member
-- node, which is the (sysid, timeline, dboid) tuple for that node.
--
-- The sysid must be a numeric (or string) because PostgreSQL has no uint64 SQL
-- type.
--
CREATE TABLE bdr_nodes (
    node_sysid text not null, -- Really a uint64 but we have no type for that
    node_timeline oid not null,
    node_dboid oid not null,  -- This is an oid local to the node_sysid cluster
    node_status "char" not null,
    primary key(node_sysid, node_timeline, node_dboid),
    check (node_status in ('i', 'c', 'r'))
);
REVOKE ALL ON TABLE bdr_nodes FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_nodes', '');

COMMENT ON TABLE bdr_nodes IS 'All known nodes in this BDR group.';
COMMENT ON COLUMN bdr_nodes.node_sysid IS 'system_identifier from the control file of the node';
COMMENT ON COLUMN bdr_nodes.node_timeline IS 'timeline ID of this node';
COMMENT ON COLUMN bdr_nodes.node_dboid IS 'local database oid on the cluster (node_sysid, node_timeline)';
COMMENT ON COLUMN bdr_nodes.node_status IS 'Readiness of the node: [i]nitializing, [c]atchup, [r]eady. Doesn''t indicate connected/disconnected.';

-- We don't exclude bdr_nodes with pg_extension_config_dump
-- because this is a global table that's sync'd between nodes.

CREATE TABLE bdr_global_locks(
    locktype text NOT NULL,

    owning_sysid text NOT NULL,
    owning_timeline oid NOT NULL,
    owning_datid oid NOT NULL,

    owner_created_lock_at pg_lsn NOT NULL,

    acquired_sysid text NOT NULL,
    acquired_timeline oid NOT NULL,
    acquired_datid oid NOT NULL,

    acquired_lock_at pg_lsn,

    state text NOT NULL
);
REVOKE ALL ON TABLE bdr_global_locks FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_global_locks', '');

CREATE UNIQUE INDEX bdr_global_locks_byowner
ON bdr_global_locks(locktype, owning_sysid, owning_timeline, owning_datid);

CREATE TABLE bdr_queued_commands (
    lsn pg_lsn NOT NULL,
    queued_at TIMESTAMP WITH TIME ZONE NOT NULL,
    perpetrator TEXT NOT NULL,
    command_tag TEXT NOT NULL,
    command TEXT NOT NULL
);
REVOKE ALL ON TABLE bdr_queued_commands FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_queued_commands', '');

CREATE OR REPLACE FUNCTION bdr.queue_truncate()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $function$
DECLARE
    ident TEXT;
BEGIN
    -- don't recursively log truncation commands
    IF bdr.bdr_replication_identifier_is_replaying() THEN
       RETURN NULL;
    END IF;

    ident := quote_ident(TG_TABLE_SCHEMA)||'.'||quote_ident(TG_TABLE_NAME);

    INSERT INTO bdr.bdr_queued_commands (
        lsn, queued_at, perpetrator,
        command_tag, command
    )
    VALUES (
        pg_current_xlog_location(),
        NOW(), CURRENT_USER,
        'TRUNCATE (automatic)',
        'TRUNCATE TABLE ONLY ' || ident
        );
    RETURN NULL;
END;
$function$;

CREATE OR REPLACE FUNCTION bdr.bdr_replicate_ddl_command(cmd TEXT)
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME'
;

DO $DO$BEGIN
IF bdr.bdr_variant() = 'BDR' THEN

	CREATE OR REPLACE FUNCTION bdr.bdr_queue_ddl_commands()
	RETURNS event_trigger
	LANGUAGE C
	AS 'MODULE_PATHNAME';

END IF;
END;$DO$;

CREATE OR REPLACE FUNCTION bdr.bdr_truncate_trigger_add()
RETURNS event_trigger
LANGUAGE C
AS 'MODULE_PATHNAME'
;

-- This type is tailored to use as input to get_object_address
CREATE TYPE bdr.dropped_object AS (
    objtype text,
    objnames text[],
    objargs text[]
);

CREATE TABLE bdr.bdr_queued_drops (
    lsn pg_lsn NOT NULL,
    queued_at timestamptz NOT NULL,
    dropped_objects bdr.dropped_object[] NOT NULL
);
REVOKE ALL ON TABLE bdr_queued_drops FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_queued_drops', '');

DO $DO$BEGIN
IF bdr.bdr_variant() = 'BDR' THEN

	CREATE OR REPLACE FUNCTION bdr.queue_dropped_objects()
	RETURNS event_trigger
	LANGUAGE C
	AS 'MODULE_PATHNAME', 'bdr_queue_dropped_objects';

	CREATE EVENT TRIGGER queue_drops
	ON sql_drop
	EXECUTE PROCEDURE bdr.queue_dropped_objects();

END IF;
END;$DO$;

CREATE OR REPLACE FUNCTION bdr_apply_pause()
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION bdr_apply_resume()
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME'
;

---
--- Replication identifier emulation
---
DO $DO$BEGIN
IF bdr.bdr_variant() = 'UDR' THEN

	CREATE TABLE bdr_replication_identifier (
		riident oid NOT NULL,
		riname text,
		riremote_lsn pg_lsn,
		rilocal_lsn pg_lsn
	);

	PERFORM pg_catalog.pg_extension_config_dump('bdr_replication_identifier', '');

	CREATE UNIQUE INDEX bdr_replication_identifier_riiident_index ON bdr_replication_identifier(riident);
	CREATE UNIQUE INDEX bdr_replication_identifier_riname_index ON bdr_replication_identifier(riname varchar_pattern_ops);

	CREATE TABLE bdr_replication_identifier_pos (
		riident oid NOT NULL,
		riremote_lsn pg_lsn,
		rilocal_lsn pg_lsn
	);

	PERFORM pg_catalog.pg_extension_config_dump('bdr_replication_identifier_pos', '');

	CREATE UNIQUE INDEX bdr_replication_identifier_pos_riiident_index ON bdr_replication_identifier_pos(riident);

	CREATE OR REPLACE FUNCTION bdr_replication_identifier_create(i_riname text) RETURNS Oid
	AS $func$
	DECLARE
		i smallint := 1;
	BEGIN
		LOCK TABLE bdr.bdr_replication_identifier;
		WHILE (SELECT 1 FROM bdr.bdr_replication_identifier WHERE riident = i) LOOP
			i := i += 1;
		END LOOP;
		INSERT INTO bdr.bdr_replication_identifier(riident, riname) VALUES(i, i_riname);
		INSERT INTO bdr.bdr_replication_identifier_pos(riident) VALUES(i);

		RETURN i;
	END;
	$func$ STRICT LANGUAGE plpgsql;

	CREATE OR REPLACE FUNCTION bdr_replication_identifier_advance(i_riname text, i_remote_lsn pg_lsn, i_local_lsn pg_lsn)
	RETURNS VOID
	LANGUAGE C
	AS 'MODULE_PATHNAME';

	CREATE OR REPLACE FUNCTION bdr_replication_identifier_drop(i_riname text)
	RETURNS VOID
	LANGUAGE C
	AS 'MODULE_PATHNAME';

	CREATE OR REPLACE FUNCTION bdr.bdr_replication_identifier_is_replaying()
	RETURNS boolean
	LANGUAGE C
	AS 'MODULE_PATHNAME';

ELSE

	CREATE OR REPLACE FUNCTION bdr.bdr_replication_identifier_is_replaying()
	RETURNS boolean
	LANGUAGE SQL
	AS 'SELECT pg_replication_identifier_is_replaying()';

END IF;
END;$DO$;

---
--- Funtions for manipulating/displaying replications sets
---
CREATE OR REPLACE FUNCTION bdr.table_get_replication_sets(relation regclass, OUT sets text[])
  VOLATILE
  STRICT
  LANGUAGE 'sql'
  AS $$
    SELECT
        ARRAY(
            SELECT *
            FROM json_array_elements_text(COALESCE((
                SELECT label::json->'sets'
                FROM pg_seclabel
                WHERE provider = 'bdr'
                     AND classoid = 'pg_class'::regclass
                     AND objoid = $1::regclass
                ), '["default"]'))
        )|| '{all}';
$$;

CREATE OR REPLACE FUNCTION bdr.table_set_replication_sets(p_relation regclass, p_sets text[])
  RETURNS void
  VOLATILE
  LANGUAGE 'plpgsql'
  AS $$
DECLARE
    v_label json;
BEGIN
    -- emulate STRICT for p_relation parameter
    IF p_relation IS NULL THEN
        RETURN;
    END IF;

    -- query current label
    SELECT label::json INTO v_label
    FROM pg_seclabel
    WHERE provider = 'bdr'
        AND classoid = 'pg_class'::regclass
        AND objoid = p_relation;

    -- replace old 'sets' parameter with new value
    SELECT json_object_agg(key, value) INTO v_label
    FROM (
        SELECT key, value
        FROM json_each(v_label)
        WHERE key <> 'sets'
      UNION ALL
        SELECT
            'sets', to_json(p_sets)
        WHERE p_sets IS NOT NULL
    ) d;

    -- and now set the appropriate label
    EXECUTE format('SECURITY LABEL FOR bdr ON TABLE %I IS %L',
                   p_relation, v_label) ;
END;
$$;


---
--- this should always be last to avoid replicating our internal schema
---

DO $DO$BEGIN
IF bdr.bdr_variant() = 'BDR' THEN

	CREATE EVENT TRIGGER bdr_queue_ddl_commands
	ON ddl_command_end
	EXECUTE PROCEDURE bdr.bdr_queue_ddl_commands();

END IF;
END;$DO$;

CREATE EVENT TRIGGER bdr_truncate_trigger_add
ON ddl_command_end
EXECUTE PROCEDURE bdr.bdr_truncate_trigger_add();

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
CREATE TABLE bdr.bdr_replication_set_config
(
    set_name name PRIMARY KEY,
    replicate_inserts bool NOT NULL DEFAULT true,
    replicate_updates bool NOT NULL DEFAULT true,
    replicate_deletes bool NOT NULL DEFAULT true
);
ALTER TABLE bdr.bdr_replication_set_config SET (user_catalog_table = true);
REVOKE ALL ON TABLE bdr.bdr_replication_set_config FROM PUBLIC;


-- We can't use ALTER TYPE ... ADD inside transaction, so do it the hard way...
ALTER TYPE bdr.bdr_conflict_resolution RENAME TO bdr_conflict_resolution_old;

CREATE TYPE bdr.bdr_conflict_resolution AS ENUM
(
    'conflict_trigger_skip_change',
    'conflict_trigger_returned_tuple',
    'last_update_wins_keep_local',
    'last_update_wins_keep_remote',
    'apply_change',
    'skip_change',
    'unhandled_tx_abort'
);

COMMENT ON TYPE bdr.bdr_conflict_resolution IS 'Resolution of a bdr conflict - if a conflict was resolved by a conflict trigger, by last-update-wins tests on commit timestamps, etc.';

ALTER TABLE bdr.bdr_conflict_history ALTER COLUMN conflict_resolution TYPE bdr.bdr_conflict_resolution USING conflict_resolution::text::bdr.bdr_conflict_resolution;

DROP TYPE bdr.bdr_conflict_resolution_old;
DO $$
BEGIN
	IF EXISTS(SELECT 1 FROM pg_catalog.pg_enum WHERE enumlabel = 'apply_change' AND enumtypid = 'bdr.bdr_conflict_resolution'::regtype) THEN
		RETURN;
	END IF;

	-- We can't use ALTER TYPE ... ADD inside transaction, so do it the hard way...
	ALTER TYPE bdr.bdr_conflict_resolution RENAME TO bdr_conflict_resolution_old;

	CREATE TYPE bdr.bdr_conflict_resolution AS ENUM
	(
		'conflict_trigger_skip_change',
		'conflict_trigger_returned_tuple',
		'last_update_wins_keep_local',
		'last_update_wins_keep_remote',
		'apply_change',
		'skip_change',
		'unhandled_tx_abort'
	);

	COMMENT ON TYPE bdr.bdr_conflict_resolution IS 'Resolution of a bdr conflict - if a conflict was resolved by a conflict trigger, by last-update-wins tests on commit timestamps, etc.';

	ALTER TABLE bdr.bdr_conflict_history ALTER COLUMN conflict_resolution TYPE bdr.bdr_conflict_resolution USING conflict_resolution::text::bdr.bdr_conflict_resolution;

	DROP TYPE bdr.bdr_conflict_resolution_old;
END;$$;


		CREATE OR REPLACE FUNCTION bdr.bdr_variant()
		RETURNS TEXT
		LANGUAGE C
		AS 'MODULE_PATHNAME';

		CREATE OR REPLACE FUNCTION bdr.bdr_replicate_ddl_command(cmd TEXT)
		RETURNS VOID
		LANGUAGE C
		AS 'MODULE_PATHNAME';

		CREATE OR REPLACE FUNCTION bdr.bdr_truncate_trigger_add()
		RETURNS event_trigger
		LANGUAGE C
		AS 'MODULE_PATHNAME';

		CREATE EVENT TRIGGER bdr_truncate_trigger_add
		ON ddl_command_end
		EXECUTE PROCEDURE bdr.bdr_truncate_trigger_add();
SET bdr.skip_ddl_replication = on;
SET bdr.permit_unsafe_ddl_commands = on;

ALTER TABLE bdr.bdr_sequence_values
    ALTER COLUMN owning_sysid TYPE text COLLATE "C",
    ALTER COLUMN owning_riname TYPE text COLLATE "C",
    ALTER COLUMN seqschema TYPE text COLLATE "C",
    ALTER COLUMN seqname TYPE text COLLATE "C";

ALTER TABLE bdr.bdr_sequence_elections
    ALTER COLUMN owning_sysid TYPE text COLLATE "C",
    ALTER COLUMN owning_riname TYPE text COLLATE "C",
    ALTER COLUMN seqschema TYPE text COLLATE "C",
    ALTER COLUMN seqname TYPE text COLLATE "C",
    ALTER COLUMN vote_type TYPE text COLLATE "C"
    ;

ALTER TABLE bdr.bdr_votes
    ALTER COLUMN voter_sysid TYPE text COLLATE "C",
    ALTER COLUMN voter_riname TYPE text COLLATE "C",
    ALTER COLUMN vote_sysid TYPE text COLLATE "C",
    ALTER COLUMN vote_riname TYPE text COLLATE "C",
    ALTER COLUMN reason TYPE text COLLATE "C"
    ;
SET bdr.skip_ddl_replication = on;
SET bdr.permit_unsafe_ddl_commands = on;

CREATE OR REPLACE FUNCTION bdr.bdr_sequencer_vote(p_sysid text, p_tlid oid, p_dboid oid, p_riname text)
RETURNS int
VOLATILE
LANGUAGE plpgsql
AS $body$
DECLARE
    v_curel record;
    v_curvote bool;
    v_ourvote_failed bool;
    v_nvotes int := 0;
BEGIN
    FOR v_curel IN SELECT *
        FROM bdr_sequence_elections election
        WHERE
            election.open
            -- not our election
            AND NOT (
                owning_sysid = p_sysid
                AND owning_tlid = p_tlid
                AND owning_dboid = p_dboid
                AND owning_riname = p_riname
            )
            -- we haven't voted about this yet
            AND NOT EXISTS (
                SELECT *
                FROM bdr_votes
                WHERE true
                    AND owning_sysid = vote_sysid
                    AND owning_tlid = vote_tlid
                    AND owning_dboid = vote_dboid
                    AND owning_riname = vote_riname
                    AND owning_election_id = vote_election_id

                    AND voter_sysid = p_sysid
                    AND voter_tlid = p_tlid
                    AND voter_dboid = p_dboid
                    AND voter_riname = p_riname
            )
        LOOP

        v_ourvote_failed = false;

        -- We haven't allowed anybody else to use it.
        IF EXISTS(
            SELECT *
            FROM bdr_sequence_elections other_election
                JOIN bdr_votes vote ON (
                    other_election.owning_sysid = vote.vote_sysid
                    AND other_election.owning_tlid = vote.vote_tlid
                    AND other_election.owning_dboid = vote.vote_dboid
                    AND other_election.owning_riname = vote.vote_riname
                    AND other_election.owning_election_id = vote.vote_election_id
                )
            WHERE true
                AND vote.voter_sysid = p_sysid
                AND vote.voter_tlid = p_tlid
                AND vote.voter_dboid = p_dboid
                AND vote.voter_riname = p_riname
                AND other_election.seqname = v_curel.seqname
                AND other_election.seqschema = v_curel.seqschema
                AND other_election.seqrange && v_curel.seqrange
        ) THEN
            v_curvote = false;
        -- If we already got the chunk, it's over
        ELSEIF EXISTS(
            SELECT *
            FROM bdr_sequence_values val
            WHERE true
                AND val.confirmed
                AND val.seqschema = v_curel.seqschema
                AND val.seqname = v_curel.seqname
                AND val.seqrange && v_curel.seqrange
                AND val.owning_sysid = p_sysid
                AND val.owning_tlid = p_tlid
                AND val.owning_dboid = p_dboid
                AND val.owning_riname = p_riname
        ) THEN
            v_curvote = false;
        -- If we have allocated the value ourselves, check whether we
        -- should be allowed, or whether we want to allow the other
        -- guy.
        ELSEIF EXISTS(
            SELECT *
            FROM bdr_sequence_values val
            WHERE true
                AND NOT val.confirmed
                AND val.seqschema = v_curel.seqschema
                AND val.seqname = v_curel.seqname
                AND val.seqrange && v_curel.seqrange
                AND val.owning_sysid = p_sysid
                AND val.owning_tlid = p_tlid
                AND val.owning_dboid = p_dboid
                AND val.owning_riname = p_riname
        ) THEN
            /* allow the guy with the smaller (sysid, tlid, dboid, riname) pair */
            IF (p_sysid, p_tlid, p_dboid, p_riname) <
               (v_curel.owning_sysid,
                v_curel.owning_tlid,
                v_curel.owning_dboid,
                v_curel.owning_riname)
            THEN
                /* this side wins */
                v_curvote = false;
            ELSE
                /* other side wins*/
                v_curvote = true;

                /*
                 * Delay update to after the insertion into bdr_votes so we
                 * have consistent lock acquiration order with
                 * bdr_sequencer_lock().
                 */

                v_ourvote_failed = true;
            END IF;
        ELSE
            v_curvote = true;
        END IF;

        /* now actually do the vote */
        INSERT INTO bdr_votes (
            vote_sysid,
            vote_tlid,
            vote_dboid,
            vote_riname,
            vote_election_id,

            voter_sysid,
            voter_tlid,
            voter_dboid,
            voter_riname,
            vote
        )
        VALUES
        (
            v_curel.owning_sysid,
            v_curel.owning_tlid,
            v_curel.owning_dboid,
            v_curel.owning_riname,
            v_curel.owning_election_id,
            p_sysid,
            p_tlid,
            p_dboid,
            p_riname,
            v_curvote
        );

        IF v_ourvote_failed THEN
            UPDATE bdr.bdr_sequence_elections AS ourel
            SET open = false, success = false
            WHERE true
                AND ourel.seqschema = v_curel.seqschema
                AND ourel.seqname = v_curel.seqname
                AND ourel.seqrange && v_curel.seqrange
                AND ourel.owning_sysid = p_sysid
                AND ourel.owning_tlid = p_tlid
                AND ourel.owning_dboid = p_dboid
                AND ourel.owning_riname = p_riname;

            UPDATE bdr.bdr_sequence_values AS ourchunk
            SET failed = true
            WHERE true
                AND ourchunk.seqschema = v_curel.seqschema
                AND ourchunk.seqname = v_curel.seqname
                AND ourchunk.seqrange && v_curel.seqrange
                AND ourchunk.owning_sysid = p_sysid
                AND ourchunk.owning_tlid = p_tlid
                AND ourchunk.owning_dboid = p_dboid
                AND ourchunk.owning_riname = p_riname;
        END IF;

        v_nvotes = v_nvotes + 1;
    END LOOP;

    RETURN v_nvotes;
END
$body$;
REVOKE ALL ON FUNCTION bdr.bdr_sequencer_vote(p_sysid text, p_tlid oid, p_dboid oid, p_riname text) FROM public;
-- fix quoting for format() arguments by directly using regclass with %s instead of %I
CREATE OR REPLACE FUNCTION bdr.table_set_replication_sets(p_relation regclass, p_sets text[])
  RETURNS void
  VOLATILE
  LANGUAGE 'plpgsql'
  AS $$
DECLARE
    v_label json;
BEGIN
    -- emulate STRICT for p_relation parameter
    IF p_relation IS NULL THEN
        RETURN;
    END IF;

    -- query current label
    SELECT label::json INTO v_label
    FROM pg_seclabel
    WHERE provider = 'bdr'
        AND classoid = 'pg_class'::regclass
        AND objoid = p_relation;

    -- replace old 'sets' parameter with new value
    SELECT json_object_agg(key, value) INTO v_label
    FROM (
        SELECT key, value
        FROM json_each(v_label)
        WHERE key <> 'sets'
      UNION ALL
        SELECT
            'sets', to_json(p_sets)
        WHERE p_sets IS NOT NULL
    ) d;

    -- and now set the appropriate label
    EXECUTE format('SECURITY LABEL FOR bdr ON TABLE %s IS %L',
                   p_relation, v_label) ;
END;
$$;
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE FUNCTION bdr_get_local_nodeid( sysid OUT oid, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

-- bdr_get_local_nodeid is intentionally not revoked from all, it's read-only

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
SET LOCAL search_path = bdr;
SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SET LOCAL bdr.skip_ddl_replication = true;

DROP FUNCTION bdr_get_local_nodeid( sysid OUT oid, timeline OUT oid, dboid OUT oid);

CREATE FUNCTION bdr_get_local_nodeid( sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION bdr_version_num()
RETURNS integer LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION bdr_version_num()
IS 'This BDR version represented as (major)*10^4 + (minor)*10^2 + (revision). The subrevision is not included. So 0.8.0.1 is 800';

CREATE FUNCTION bdr_min_remote_version_num()
RETURNS integer LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION bdr_min_remote_version_num()
IS 'The oldest BDR version that this BDR extension can exchange data with';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
-- BDR release 0.9.0.0
SET LOCAL search_path = bdr;
SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SET LOCAL bdr.skip_ddl_replication = true;

CREATE FUNCTION bdr_get_remote_nodeinfo(
	dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid,
	variant OUT text, version OUT text, version_num OUT integer,
	min_remote_version_num OUT integer, is_superuser OUT boolean)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_get_remote_nodeinfo(text) FROM public;

COMMENT ON FUNCTION bdr_get_remote_nodeinfo(text) IS 'Get node identity and BDR info from a remote server by dsn';

CREATE FUNCTION bdr_test_replication_connection(dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_test_replication_connection(text) FROM public;

COMMENT ON FUNCTION bdr_test_replication_connection(text)
IS 'Make a replication-mode connection to the specified DSN and get its node identity.';

CREATE FUNCTION bdr_test_remote_connectback(
    remote_dsn text, local_dsn text,
    sysid OUT text, timeline OUT oid, dboid OUT oid,
    variant OUT text, version OUT text, version_num OUT integer,
    min_remote_version_num OUT integer, is_superuser OUT boolean)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_test_remote_connectback(text,text) FROM public;

COMMENT ON FUNCTION bdr_test_remote_connectback(text,text)
IS 'Connect to remote_dsn and from there connect back to local_dsn and report nodeinfo for local_dsn';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

ALTER TABLE bdr.bdr_nodes
  ADD COLUMN node_name text,
  ADD COLUMN node_local_dsn text,
  ADD COLUMN node_init_from_dsn text;

ALTER TABLE bdr.bdr_nodes
  DROP CONSTRAINT bdr_nodes_node_status_check;

ALTER TABLE bdr.bdr_nodes
  ADD CONSTRAINT bdr_nodes_node_status_check
    CHECK (node_status in ('b', 'i', 'c', 'o', 'r', 'k'));

CREATE TABLE bdr_connections (
    conn_sysid text not null,
    conn_timeline oid not null,
    conn_dboid oid not null,  -- This is an oid local to the node_sysid cluster

    -- Wondering why there's no FOREIGN KEY to bdr.bdr_nodes?
    -- bdr.bdr_nodes won't be populated when the bdr.bdr_connections
    -- row gets created on the local node.

    -- These fields may later be used by BDR to override connection
    -- settings from one node to a particular other node. At the
    -- moment their main use is for UDR connections, where we must
    -- ensure that the connection is only made from one particular
    -- node.
    conn_origin_sysid text,
    conn_origin_timeline oid,
    conn_origin_dboid oid,

    PRIMARY KEY(conn_sysid, conn_timeline, conn_dboid,
                conn_origin_sysid, conn_origin_timeline, conn_origin_dboid),

    -- Either a whole origin ID (for an override or UDR entry) or no
    -- origin ID may be provided.
    CONSTRAINT origin_all_or_none_null
        CHECK ((conn_origin_sysid = '0') = (conn_origin_timeline = 0)
           AND (conn_origin_sysid = '0') = (conn_origin_dboid = 0)),

    -- Indicates that this connection is unidirectional; there won't be
    -- a corresponding inbound connection from the peer node. Only permitted
    -- where the conn_origin fields are set.
    conn_is_unidirectional boolean not null default false,

    CONSTRAINT unidirectional_conn_must_have_origin
        CHECK ((NOT conn_is_unidirectional) OR (conn_origin_sysid <> '0')),

    conn_dsn text not null,

    conn_apply_delay integer
        CHECK (conn_apply_delay >= 0),

    conn_replication_sets text[]
);

REVOKE ALL ON TABLE bdr_connections FROM public;

COMMENT ON TABLE bdr_connections IS 'Connection information for nodes in the group. Don''t modify this directly, use the provided functions. One entry should exist per node in the group.';

COMMENT ON COLUMN bdr_connections.conn_sysid IS 'System identifer for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_timeline IS 'System timeline ID for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_dboid IS 'System database OID for the node this entry''s dsn refers to';
COMMENT ON COLUMN bdr_connections.conn_origin_sysid IS 'If set, ignore this entry unless the local sysid is this';
COMMENT ON COLUMN bdr_connections.conn_origin_timeline IS 'If set, ignore this entry unless the local timeline is this';
COMMENT ON COLUMN bdr_connections.conn_origin_dboid IS 'If set, ignore this entry unless the local dboid is this';
COMMENT ON COLUMN bdr_connections.conn_dsn IS 'A libpq-style connection string specifying how to make a connection to this node from other nodes.';
COMMENT ON COLUMN bdr_connections.conn_apply_delay IS 'If set, milliseconds to wait before applying each transaction from the remote node. Mainly for debugging. If null, the global default applies.';
COMMENT ON COLUMN bdr_connections.conn_replication_sets IS 'Replication sets this connection should participate in, if non-default.';

SELECT pg_catalog.pg_extension_config_dump('bdr_connections', '');

CREATE FUNCTION bdr_connections_changed()
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_connections_changed() FROM public;

COMMENT ON FUNCTION bdr_connections_changed() IS 'Internal BDR function, do not call directly.';


--
-- This is a helper for node_join, for internal use only. It's called
-- on the remote end by the init code when joining an existing group,
-- to do the remote-side setup.
--
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


CREATE FUNCTION bdr.internal_update_seclabel()
RETURNS void LANGUAGE plpgsql
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    v_label json;
BEGIN
    -- Update 'bdr' parameter in the current label if there's one.
    -- (Right now there's not much point to this but later we'll be
    -- possibly have more information in there.)

    -- first select existing label
    SELECT label::json INTO v_label
    FROM pg_catalog.pg_shseclabel
    WHERE provider = 'bdr'
      AND classoid = 'pg_database'::regclass
      AND objoid = (SELECT oid FROM pg_database WHERE datname = current_database());

    -- then replace 'bdr' with 'bdr'::true
    SELECT json_object_agg(key, value) INTO v_label
    FROM (
        SELECT key, value
        FROM json_each(v_label)
        WHERE key <> 'bdr'
      UNION ALL
        SELECT 'bdr', to_json(true)
    ) d;

    -- and set the newly computed label
    -- (It's safe to do this early, it won't take effect
    -- until commit)
    EXECUTE format('SECURITY LABEL FOR bdr ON DATABASE %I IS %L',
                   current_database(), v_label);
END;
$body$;

-- Setup that's common to BDR and UDR joins
CREATE FUNCTION bdr.internal_begin_join(
    caller text, local_node_name text, node_local_dsn text, remote_dsn text,
    remote_sysid OUT text, remote_timeline OUT oid, remote_dboid OUT oid
)
RETURNS record LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    localid RECORD;
    localid_from_dsn RECORD;
    remote_nodeinfo RECORD;
BEGIN
    -- Only one tx can be adding connections
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    -- If there's already an entry for ourselves in bdr.bdr_connections
    -- then we know this node is part of an active BDR group and cannot
    -- be joined to another group. Unidirectional connections are ignored.
    PERFORM 1 FROM bdr_connections
    WHERE conn_sysid = localid.sysid
      AND conn_timeline = localid.timeline
      AND conn_dboid = localid.dboid
      AND (conn_origin_sysid = '0'
           AND conn_origin_timeline = 0
           AND conn_origin_dboid = 0)
      AND conn_is_unidirectional = 'f';

    IF FOUND THEN
        RAISE USING
            MESSAGE = 'This node is already a member of a BDR group',
            HINT = 'Connect to the node you wish to add and run '||caller||' from it instead',
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Validate that the local connection is usable and matches
    -- the node identity of the node we're running on.
    --
    -- For BDR this will NOT check the 'dsn' if 'node_local_dsn'
    -- gets supplied. We don't know if 'dsn' is even valid
    -- for loopback connections and can't assume it is. That'll
    -- get checked later by BDR specific code.
    SELECT * INTO localid_from_dsn
    FROM bdr_get_remote_nodeinfo(node_local_dsn);

    IF localid_from_dsn.sysid <> localid.sysid
        OR localid_from_dsn.timeline <> localid.timeline
        OR localid_from_dsn.dboid <> localid.dboid
    THEN
        RAISE USING
            MESSAGE = 'node identity for local dsn does not match current node',
            DETAIL = format($$The dsn '%s' connects to a node with identity (%s,%s,%s) but the local node is (%s,%s,%s)$$,
                node_local_dsn, localid_from_dsn.sysid, localid_from_dsn.timeline,
                localid_from_dsn.dboid, localid.sysid, localid.timeline, localid.dboid),
            HINT = 'The node_local_dsn (or, for bdr, dsn if node_local_dsn is null) parameter must refer to the node you''re running this function from',
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    IF NOT localid_from_dsn.is_superuser THEN
        RAISE USING
            MESSAGE = 'local dsn does not have superuser rights',
            DETAIL = format($$The dsn '%s' connects successfully but does not grant superuser rights$$, node_local_dsn),
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Now interrogate the remote node, if specified, and sanity
    -- check its connection too. The discovered node identity is
    -- returned if found.
    --
    -- This will error out if there are issues with the remote
    -- node.
    IF remote_dsn IS NOT NULL THEN
        SELECT * INTO remote_nodeinfo
        FROM bdr_get_remote_nodeinfo(remote_dsn);

        remote_sysid := remote_nodeinfo.sysid;
        remote_timeline := remote_nodeinfo.timeline;
        remote_dboid := remote_nodeinfo.dboid;

        IF NOT remote_nodeinfo.is_superuser THEN
            RAISE USING
                MESSAGE = 'connection to remote node does not have superuser rights',
                DETAIL = format($$The dsn '%s' connects successfully but does not grant superuser rights$$, remote_dsn),
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;

        IF remote_nodeinfo.version_num < bdr_min_remote_version_num() THEN
            RAISE USING
                MESSAGE = 'remote node''s BDR version is too old',
                DETAIL = format($$The dsn '%s' connects successfully but the remote node version %s is less than the required version %s$$,
                    remote_dsn, remote_nodeinfo.version_num, bdr_min_remote_version_num()),
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;

        IF remote_nodeinfo.min_remote_version_num > bdr_version_num() THEN
            RAISE USING
                MESSAGE = 'remote node''s BDR version is too new or this node''s version is too old',
                DETAIL = format($$The dsn '%s' connects successfully but the remote node version %s requires this node to run at least bdr %s, not the current %s$$,
                    remote_dsn, remote_nodeinfo.version_num, remote_nodeinfo.min_remote_version_num,
                    bdr_min_remote_version_num()),
                ERRCODE = 'object_not_in_prerequisite_state';

        END IF;

    END IF;

    -- Create local node record if needed
    PERFORM 1 FROM bdr_nodes
    WHERE node_sysid = localid.sysid
      AND node_timeline = localid.timeline
      AND node_dboid = localid.dboid;

    IF NOT FOUND THEN
        INSERT INTO bdr_nodes (
            node_name,
            node_sysid, node_timeline, node_dboid,
            node_status, node_local_dsn, node_init_from_dsn
        ) VALUES (
            local_node_name,
            localid.sysid, localid.timeline, localid.dboid,
            'b', node_local_dsn, remote_dsn
        );
    END IF;

    PERFORM bdr.internal_update_seclabel();
END;
$body$;

--
-- The public interface for node join/addition, to be run to join a currently
-- unconnected node with a blank database to a BDR group.
--
CREATE FUNCTION bdr.bdr_group_join(
    local_node_name text,
    node_external_dsn text,
    join_using_dsn text,
    node_local_dsn text DEFAULT NULL,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default']
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    localid record;
    connectback_nodeinfo record;
    remoteinfo record;
BEGIN
    IF node_external_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'dsn may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    IF bdr_variant() <> 'BDR' THEN
        RAISE USING
            MESSAGE = 'Full BDR required but this module is built for '||bdr_variant(),
            DETAIL = 'The local node is not running full BDR, which is required to use bdr_join',
            HINT = 'Install full BDR if possible or use the UDR functions.',
            ERRCODE = 'feature_not_supported';
    END IF;

    PERFORM bdr.internal_begin_join(
        'bdr_group_join',
        local_node_name,
        CASE WHEN node_local_dsn IS NULL THEN node_external_dsn ELSE node_local_dsn END,
        join_using_dsn);

    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    -- Request additional connection tests to determine that the remote is
    -- reachable for replication and non-replication mode and that the remote
    -- can connect back to us via 'dsn' on non-replication and replication
    -- modes.
    --
    -- This cannot be checked for the first node since there's no peer
    -- to ask for help.
    IF join_using_dsn IS NOT NULL THEN

        SELECT * INTO connectback_nodeinfo
        FROM bdr.bdr_test_remote_connectback(join_using_dsn, node_external_dsn);

        -- The connectback must actually match our local node identity
        -- and must provide a superuser connection.
        IF NOT connectback_nodeinfo.is_superuser THEN
            RAISE USING
                MESSAGE = 'node_external_dsn does not have superuser rights when connecting via remote node',
                DETAIL = format($$The dsn '%s' connects successfully but does not grant superuser rights$$, dsn),
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;

        IF connectback_nodeinfo.sysid <> localid.sysid
           OR connectback_nodeinfo.timeline <> localid.timeline
           OR connectback_nodeinfo.dboid <> localid.dboid
        THEN
            RAISE USING
                MESSAGE = 'node identity for node_external_dsn does not match current node when connecting back via remote',
                DETAIL = format($$The dsn '%s' connects to a node with identity (%s,%s,%s) but the local node is (%s,%s,%s)$$,
                    node_local_dsn, connectback_nodeinfo.sysid, connectback_nodeinfo.timeline,
                    connectback_nodeinfo.dboid, localid.sysid, localid.timeline, localid.dboid),
                HINT = 'The ''node_external_dsn'' parameter must refer to the node you''re running this function from, from the perspective of the node pointed to by join_using_dsn',
                ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
    END IF;

    -- Null/empty checks are skipped, the underlying constraints on the table
    -- will catch that for us.
    INSERT INTO bdr.bdr_connections (
        conn_sysid, conn_timeline, conn_dboid,
        conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,
        conn_dsn, conn_apply_delay, conn_replication_sets,
        conn_is_unidirectional
    ) VALUES (
        localid.sysid, localid.timeline, localid.dboid,
        '0', 0, 0,
        node_external_dsn, apply_delay, replication_sets, false
    );

    -- Now ensure the per-db worker is started if it's not already running.
    -- This won't actually take effect until commit time, it just adds a commit
    -- hook to start the worker when we commit.
    PERFORM bdr.bdr_connections_changed();
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_group_join(text, text, text, text, integer, text[])
IS 'Join an existing BDR group by connecting to a member node and copying its contents';

CREATE FUNCTION bdr.bdr_group_create(
    local_node_name text,
    node_external_dsn text,
    node_local_dsn text DEFAULT NULL,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default']
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
BEGIN
    PERFORM bdr.bdr_group_join(
        local_node_name := local_node_name,
        node_external_dsn := node_external_dsn,
        join_using_dsn := null,
        node_local_dsn := node_local_dsn,
        apply_delay := apply_delay,
        replication_sets := replication_sets);
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_group_create(text, text, text, integer, text[])
IS 'Create a BDR group, turning a stand-alone database into the first node in a BDR group';

--
-- The public interface for unidirectional replication setup.
--
CREATE FUNCTION bdr.bdr_subscribe(
    local_node_name text,
    subscribe_to_dsn text,
    node_local_dsn text,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default']
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    localid record;
    remoteid record;
BEGIN
    IF node_local_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'node_local_dsn may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    IF subscribe_to_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'remote may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT remote_sysid AS sysid, remote_timeline AS timeline,
           remote_dboid AS dboid INTO remoteid
    FROM bdr.internal_begin_join('bdr_subscribe',
         local_node_name || '-subscriber',
         node_local_dsn, subscribe_to_dsn);

    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    PERFORM 1 FROM bdr_connections
    WHERE conn_sysid = remoteid.sysid
      AND conn_timeline = remoteid.timeline
      AND conn_dboid = remoteid.dboid
      AND conn_origin_sysid = localid.sysid
      AND conn_origin_timeline = localid.timeline
      AND conn_origin_dboid = localid.dboid
      AND conn_is_unidirectional = 't';

    IF FOUND THEN
        RAISE USING
            MESSAGE = 'This node is already connected to given remote node',
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Insert node entry representing remote node
    PERFORM 1 FROM bdr_nodes
    WHERE node_sysid = remoteid.sysid
      AND node_timeline = remoteid.timeline
      AND node_dboid = remoteid.dboid;

    IF NOT FOUND THEN
        INSERT INTO bdr_nodes (
            node_name,
            node_sysid, node_timeline, node_dboid,
            node_status
        ) VALUES (
            local_node_name,
            remoteid.sysid, remoteid.timeline, remoteid.dboid,
            'r'
        );
    END IF;

    -- Null/empty checks are skipped, the underlying constraints on the table
    -- will catch that for us.
    INSERT INTO bdr.bdr_connections (
        conn_sysid, conn_timeline, conn_dboid,
        conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,
        conn_dsn, conn_apply_delay, conn_replication_sets,
        conn_is_unidirectional
    ) VALUES (
        remoteid.sysid, remoteid.timeline, remoteid.dboid,
        localid.sysid, localid.timeline, localid.dboid,
        subscribe_to_dsn, apply_delay, replication_sets, true
    );

    -- Now ensure the per-db worker is started if it's not already running.
    -- This won't actually take effect until commit time, it just adds a commit
    -- hook to start the worker when we commit.
    PERFORM bdr.bdr_connections_changed();
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_subscribe(text, text, text, integer, text[])
IS 'Subscribe to remote logical changes';

CREATE OR REPLACE FUNCTION bdr.bdr_part_by_node_names(p_nodes text[])
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
BEGIN
    -- concurrency
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    UPDATE bdr.bdr_nodes
    SET node_status = 'k'
    WHERE node_name = ANY(p_nodes);

    -- Notify local perdb worker to kill nodes.
    PERFORM bdr.bdr_connections_changed();

END;
$body$;

CREATE FUNCTION bdr.bdr_node_join_wait_for_ready()
RETURNS void LANGUAGE plpgsql VOLATILE AS $body$
DECLARE
    _node_status "char";
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'Can only wait for node join in an ISOLATION LEVEL READ COMMITTED transaction, not %',
                        current_setting('transaction_isolation');
    END IF;

    LOOP
        SELECT INTO _node_status
          node_status
        FROM bdr.bdr_nodes
        WHERE (node_sysid, node_timeline, node_dboid)
              = bdr.bdr_get_local_nodeid();

    PERFORM pg_sleep(0.5);

        EXIT WHEN _node_status = 'r';
    END LOOP;
END;
$body$;

CREATE FUNCTION bdr_upgrade_to_090(my_conninfo cstring, local_conninfo cstring, remote_conninfo cstring)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_upgrade_to_090(cstring,cstring,cstring) FROM public;

COMMENT ON FUNCTION bdr_upgrade_to_090(cstring,cstring,cstring)
IS 'Upgrade a BDR 0.7.x or 0.8.x node to BDR 0.9.0 dynamic configuration. remote_conninfo is the node to connect to to perform the upgrade, my_conninfo is the dsn for other nodes to connect to this node with, local_conninfo is used to connect locally back to the node. Use null remote conninfo on the first node.';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE TYPE bdr.bdr_sync_type AS ENUM
(
	'none',
	'full'
);

--
-- The public interface for unidirectional replication setup.
--
DROP FUNCTION bdr.bdr_subscribe(text, text, text, integer, text[]);

CREATE FUNCTION bdr.bdr_subscribe(
    local_node_name text,
    subscribe_to_dsn text,
    node_local_dsn text,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default'],
    synchronize bdr.bdr_sync_type DEFAULT 'full'
    )
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    localid record;
    remoteid record;
BEGIN
    IF node_local_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'node_local_dsn may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    IF subscribe_to_dsn IS NULL THEN
        RAISE USING
            MESSAGE = 'remote may not be null',
            ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT remote_sysid AS sysid, remote_timeline AS timeline,
           remote_dboid AS dboid INTO remoteid
    FROM bdr.internal_begin_join('bdr_subscribe',
         local_node_name || '-subscriber',
         node_local_dsn, subscribe_to_dsn);

    SELECT sysid, timeline, dboid INTO localid
    FROM bdr.bdr_get_local_nodeid();

    PERFORM 1 FROM bdr_connections
    WHERE conn_sysid = remoteid.sysid
      AND conn_timeline = remoteid.timeline
      AND conn_dboid = remoteid.dboid
      AND conn_origin_sysid = localid.sysid
      AND conn_origin_timeline = localid.timeline
      AND conn_origin_dboid = localid.dboid
      AND conn_is_unidirectional = 't';

    IF FOUND THEN
        RAISE USING
            MESSAGE = 'This node is already connected to given remote node',
            ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Insert node entry representing remote node
    PERFORM 1 FROM bdr_nodes
    WHERE node_sysid = remoteid.sysid
      AND node_timeline = remoteid.timeline
      AND node_dboid = remoteid.dboid;

    IF NOT FOUND THEN
        INSERT INTO bdr_nodes (
            node_name,
            node_sysid, node_timeline, node_dboid,
            node_status
        ) VALUES (
            local_node_name,
            remoteid.sysid, remoteid.timeline, remoteid.dboid,
			'r'
        );
    END IF;

	-- If user does not want synchronization, update the local node status
	-- to catched up so that the initial dump/restore does not happen
	IF synchronize = 'none' THEN
		UPDATE bdr_nodes SET node_status = 'o'
	     WHERE node_sysid = localid.sysid
		   AND node_timeline = localid.timeline
		   AND node_dboid = localid.dboid;
	END IF;

    -- Null/empty checks are skipped, the underlying constraints on the table
    -- will catch that for us.
    INSERT INTO bdr.bdr_connections (
        conn_sysid, conn_timeline, conn_dboid,
        conn_origin_sysid, conn_origin_timeline, conn_origin_dboid,
        conn_dsn, conn_apply_delay, conn_replication_sets,
        conn_is_unidirectional
    ) VALUES (
        remoteid.sysid, remoteid.timeline, remoteid.dboid,
        localid.sysid, localid.timeline, localid.dboid,
        subscribe_to_dsn, apply_delay, replication_sets, true
    );

    -- Now ensure the per-db worker is started if it's not already running.
    -- This won't actually take effect until commit time, it just adds a commit
    -- hook to start the worker when we commit.
    PERFORM bdr.bdr_connections_changed();
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_subscribe(text, text, text, integer, text[], bdr.bdr_sync_type)
IS 'Subscribe to remote logical changes';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
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
-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

---
--- Replication identifier emulation
---
DO $DO$BEGIN
IF bdr.bdr_variant() = 'UDR' THEN

	ALTER TABLE bdr.bdr_replication_identifier RENAME TO bdr_replication_identifier_old;
	CREATE TABLE bdr.bdr_replication_identifier (
		riident oid NOT NULL,
		riname text
	);
	INSERT INTO bdr.bdr_replication_identifier SELECT riident, riname FROM bdr.bdr_replication_identifier_old;

	DROP TABLE bdr.bdr_replication_identifier_old;

	PERFORM pg_catalog.pg_extension_config_dump('bdr_replication_identifier', '');
	CREATE UNIQUE INDEX bdr_replication_identifier_riiident_index ON bdr.bdr_replication_identifier(riident);
	CREATE UNIQUE INDEX bdr_replication_identifier_riname_index ON bdr.bdr_replication_identifier(riname varchar_pattern_ops);

END IF;
END;$DO$;


RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

--
-- set_replication_sets knows what it's doing and needs to be able to
-- replicate SECURITY LABEL commands
--
ALTER FUNCTION bdr.table_set_replication_sets(p_relation regclass, p_sets text[])
SET bdr.permit_unsafe_ddl_commands = true;

CREATE OR REPLACE FUNCTION bdr.bdr_apply_is_paused()
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
-- Just a version bump for the 0.9.1 release
