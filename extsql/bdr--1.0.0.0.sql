--\echo Use "CREATE EXTENSION bdr" to load this file. \quit
--
--
-- You might read this extension script and wonder why we create so much
-- then drop it again, or ALTER it, etc. This matters, and you should
-- be careful what you "simplify" or "fix".
--
-- BDR replicates based on attribute numbers for tables. Dropped
-- attributes are thus significant. You can't replicate from
--
--   CREATE TABLE x (a integer, b integer);
--   ALTER TABLE x DROP COLUMN a;
--   ALTER TABLE x ADD COLUMN c integer;
--
-- to
--
--   CREATE TABLE x (b integer, c integer);
--
-- Similar concerns apply for composite types and enums, but not
-- for functions or domains.
--
-- So be careful what you change. In fact, preferably change nothing.
-- That's why we have extension upgrade scripts.
--

--
-- This script is a simplified and edited version of the concatenation of all
-- BDR scripts from 0.7.x to 1.0 inclusive. It does NOT produce exactly the
-- same results as running them because this version is updated with
-- compatibility fixes for 9.6 support. However, those fixes only matter when
-- running on 9.6, where there can be no older version of the extension.
--

-- We must be able to use exclusion constraints for global sequences
-- among other things.
SET bdr.permit_unsafe_ddl_commands = true;

-- We don't want to replicate commands from in here
SET bdr.skip_ddl_replication = true;

CREATE SCHEMA bdr;
GRANT USAGE ON SCHEMA bdr TO public;

-- Everything should assume the 'bdr' prefix
SET LOCAL search_path = bdr;

CREATE OR REPLACE FUNCTION bdr_version()
RETURNS TEXT
LANGUAGE C
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION bdr.bdr_variant()
RETURNS TEXT
LANGUAGE C
AS 'MODULE_PATHNAME';

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

ALTER TABLE bdr.bdr_conflict_handlers ALTER COLUMN ch_fun TYPE text USING (ch_fun::text);

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

CREATE VIEW bdr.bdr_list_conflict_handlers(ch_name, ch_type, ch_reloid, ch_fun) AS
	SELECT ch_name, ch_type, ch_reloid, ch_fun, ch_timeframe
	FROM bdr.bdr_conflict_handlers;

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
    CHECK (node_status in ('b', 'i', 'c', 'o', 'r', 'k'))
);
REVOKE ALL ON TABLE bdr_nodes FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('bdr_nodes', '');

ALTER TABLE bdr.bdr_nodes
  ADD COLUMN node_name text,
  ADD COLUMN node_local_dsn text,
  ADD COLUMN node_init_from_dsn text;

-- add constrains ensuring node_names are unique and not null
ALTER TABLE bdr.bdr_nodes ALTER COLUMN node_name SET NOT NULL;

CREATE UNIQUE INDEX bdr_nodes_node_name
ON bdr.bdr_nodes(node_name);

ALTER TABLE bdr.bdr_nodes ADD COLUMN node_read_only boolean;
ALTER TABLE bdr.bdr_nodes ALTER COLUMN node_read_only SET DEFAULT false;

COMMENT ON TABLE bdr_nodes IS 'All known nodes in this BDR group.';
COMMENT ON COLUMN bdr_nodes.node_sysid IS 'system_identifier from the control file of the node';
COMMENT ON COLUMN bdr_nodes.node_timeline IS 'timeline ID of this node';
COMMENT ON COLUMN bdr_nodes.node_dboid IS 'local database oid on the cluster (node_sysid, node_timeline)';
COMMENT ON COLUMN bdr_nodes.node_status IS 'Readiness of the node: [b]eginning setup, [i]nitializing, [c]atchup, creating [o]utbound slots, [r]eady, [k]illed. Doesn''t indicate connected/disconnected.';

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

CREATE OR REPLACE FUNCTION bdr.bdr_replicate_ddl_command(cmd TEXT)
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME'
;

DO
LANGUAGE plpgsql
$$
BEGIN
	IF (current_setting('server_version_num')::int / 100) = 904 THEN
		-- We only use the event trigger on 9.4bdr, since 9.6 lacks
		-- ddl deparse.
		CREATE OR REPLACE FUNCTION bdr.bdr_queue_ddl_commands()
		RETURNS event_trigger
		LANGUAGE C
		AS 'MODULE_PATHNAME';

		CREATE EVENT TRIGGER bdr_queue_ddl_commands
		ON ddl_command_end
		EXECUTE PROCEDURE bdr.bdr_queue_ddl_commands();

		CREATE OR REPLACE FUNCTION bdr.queue_dropped_objects()
		RETURNS event_trigger
		LANGUAGE C
		AS 'MODULE_PATHNAME', 'bdr_queue_dropped_objects';

		CREATE EVENT TRIGGER queue_drops
		ON sql_drop
		EXECUTE PROCEDURE bdr.queue_dropped_objects();
	END IF;

END;
$$;

CREATE OR REPLACE FUNCTION bdr.bdr_truncate_trigger_add()
RETURNS event_trigger
LANGUAGE C
AS 'MODULE_PATHNAME'
;

CREATE FUNCTION bdr.bdr_internal_create_truncate_trigger(regclass)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.queue_truncate()
    RETURNS TRIGGER
	LANGUAGE C
	AS 'MODULE_PATHNAME','bdr_queue_truncate';

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

CREATE OR REPLACE FUNCTION bdr.bdr_apply_pause()
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME'
;

CREATE OR REPLACE FUNCTION bdr.bdr_apply_resume()
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME'
;

---
--- Replication identifiers
---
DO $DO$
BEGIN
	CASE current_setting('server_version_num')::int / 100
	WHEN 904 THEN
		-- 9.4 replication identifiers
		CREATE OR REPLACE FUNCTION bdr.bdr_replication_identifier_is_replaying()
		RETURNS boolean
		LANGUAGE SQL
		AS 'SELECT pg_replication_identifier_is_replaying()';
	WHEN 906 THEN
		-- 9.6 replication origins
		CREATE OR REPLACE FUNCTION bdr.bdr_replication_identifier_is_replaying()
		RETURNS boolean
		LANGUAGE SQL
		AS 'SELECT pg_replication_origin_session_is_setup()';
	ELSE
		RAISE EXCEPTION 'Pg version % not supported', current_setting('server_version_num');
	END CASE;
END;
$DO$;

---
--- Functions for manipulating/displaying replications sets
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

CREATE TABLE bdr.bdr_replication_set_config
(
    set_name name PRIMARY KEY,
    replicate_inserts bool NOT NULL DEFAULT true,
    replicate_updates bool NOT NULL DEFAULT true,
    replicate_deletes bool NOT NULL DEFAULT true
);
ALTER TABLE bdr.bdr_replication_set_config SET (user_catalog_table = true);
REVOKE ALL ON TABLE bdr.bdr_replication_set_config FROM PUBLIC;

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
  SET bdr.permit_unsafe_ddl_commands = true
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

CREATE FUNCTION bdr.bdr_get_local_nodeid( sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION bdr.bdr_version_num()
RETURNS integer LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION bdr.bdr_version_num()
IS 'This BDR version represented as (major)*10^4 + (minor)*10^2 + (revision). The subrevision is not included. So 0.8.0.1 is 800';

CREATE FUNCTION bdr.bdr_min_remote_version_num()
RETURNS integer LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION bdr.bdr_min_remote_version_num()
IS 'The oldest BDR version that this BDR extension can exchange data with';

CREATE FUNCTION bdr.bdr_get_remote_nodeinfo(
	dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid,
	variant OUT text, version OUT text, version_num OUT integer,
	min_remote_version_num OUT integer, is_superuser OUT boolean)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr.bdr_get_remote_nodeinfo(text) FROM public;

COMMENT ON FUNCTION bdr.bdr_get_remote_nodeinfo(text) IS 'Get node identity and BDR info from a remote server by dsn';

CREATE FUNCTION bdr.bdr_test_replication_connection(dsn text, sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr.bdr_test_replication_connection(text) FROM public;

COMMENT ON FUNCTION bdr.bdr_test_replication_connection(text)
IS 'Make a replication-mode connection to the specified DSN and get its node identity.';

CREATE FUNCTION bdr.bdr_test_remote_connectback(
    remote_dsn text, local_dsn text,
    sysid OUT text, timeline OUT oid, dboid OUT oid,
    variant OUT text, version OUT text, version_num OUT integer,
    min_remote_version_num OUT integer, is_superuser OUT boolean)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr.bdr_test_remote_connectback(text,text) FROM public;

COMMENT ON FUNCTION bdr.bdr_test_remote_connectback(text,text)
IS 'Connect to remote_dsn and from there connect back to local_dsn and report nodeinfo for local_dsn';

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

CREATE FUNCTION bdr.bdr_connections_changed()
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

    -- Assert that the joining node has a bdr_nodes entry with state = i on this join-target node
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
CREATE OR REPLACE FUNCTION bdr.internal_begin_join(
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
    remote_nodeinfo_r RECORD;
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

    -- Verify that we can make a replication connection to the remote node
    -- so that pg_hba.conf issues get caught early.
    IF remote_dsn IS NOT NULL THEN
        -- Returns (sysid, timeline, dboid) on success, else ERRORs
        SELECT * FROM bdr_test_replication_connection(remote_dsn)
        INTO remote_nodeinfo_r;

        IF (remote_nodeinfo_r.sysid, remote_nodeinfo_r.timeline, remote_nodeinfo_r.dboid)
            IS DISTINCT FROM
           (remote_sysid, remote_timeline, remote_dboid)
            AND
           (remote_sysid, remote_timeline, remote_dboid)
            IS DISTINCT FROM
           (NULL, NULL, NULL)
        THEN
            -- This just shouldn't happen, so no fancy error.
            -- The all-NULLs case only arises when we're connecting to a 0.7.x
            -- peer, where we can't get the sysid etc from SQL.
            RAISE USING
                MESSAGE = 'Replication and non-replication connections to remote node reported different node id';
        END IF;

        -- In case they're NULL because of bdr_get_remote_nodeinfo
        -- due to an old upstream
        remote_sysid := remote_nodeinfo_r.sysid;
        remote_timeline := remote_nodeinfo_r.timeline;
        remote_dboid := remote_nodeinfo_r.dboid;

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

        IF (connectback_nodeinfo.sysid, connectback_nodeinfo.timeline, connectback_nodeinfo.dboid)
          IS DISTINCT FROM
           (localid.sysid, localid.timeline, localid.dboid)
          AND
           (connectback_nodeinfo.sysid, connectback_nodeinfo.timeline, connectback_nodeinfo.dboid)
          IS DISTINCT FROM
           (NULL, NULL, NULL) -- Returned by old versions' dummy functions
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

CREATE OR REPLACE FUNCTION bdr.bdr_group_create(
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
DECLARE
	t record;
BEGIN

    -- Prohibit enabling BDR where exclusion constraints exist
    FOR t IN
        SELECT n.nspname, r.relname, c.conname, c.contype
        FROM pg_constraint c
          INNER JOIN pg_namespace n ON c.connamespace = n.oid
          INNER JOIN pg_class r ON c.conrelid = r.oid
          INNER JOIN LATERAL unnest(bdr.table_get_replication_sets(c.conrelid)) rs(rsname) ON (rs.rsname = ANY(replication_sets))
        WHERE c.contype = 'x'
          AND r.relpersistence = 'p'
          AND r.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
    LOOP
        RAISE USING
            MESSAGE = 'BDR can''t be enabled because exclusion constraints exist on persistent tables that are not excluded from replication',
            ERRCODE = 'object_not_in_prerequisite_state',
            DETAIL = format('Table %I.%I has exclusion constraint %I', t.nspname, t.relname, t.conname),
            HINT = 'Drop the exclusion constraint(s), change the table(s) to UNLOGGED if they don''t need to be replicated, or exclude the table(s) from the active replication set(s).';
    END LOOP;

    -- Warn users about secondary unique indexes
    FOR t IN
        SELECT n.nspname, r.relname, c.conname, c.contype
        FROM pg_constraint c
          INNER JOIN pg_namespace n ON c.connamespace = n.oid
          INNER JOIN pg_class r ON c.conrelid = r.oid
          INNER JOIN LATERAL unnest(bdr.table_get_replication_sets(c.conrelid)) rs(rsname) ON (rs.rsname = ANY(replication_sets))
        WHERE c.contype = 'u'
          AND r.relpersistence = 'p'
          AND r.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
    LOOP
        RAISE WARNING USING
            MESSAGE = 'Secondary unique constraint(s) exist on replicated table(s)',
            DETAIL = format('Table %I.%I has secondary unique constraint %I. This may cause unhandled replication conflicts.', t.nspname, t.relname, t.conname),
            HINT = 'Drop the secondary unique constraint(s), change the table(s) to UNLOGGED if they don''t need to be replicated, or exclude the table(s) from the active replication set(s).';
    END LOOP;

    -- Warn users about missing primary keys
    FOR t IN
        SELECT n.nspname, r.relname, c.conname
        FROM pg_class r INNER JOIN pg_namespace n ON r.relnamespace = n.oid
          LEFT OUTER JOIN pg_constraint c ON (c.conrelid = r.oid AND c.contype = 'p')
        WHERE n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
          AND relkind = 'r'
          AND relpersistence = 'p'
          AND c.oid IS NULL
    LOOP
        RAISE WARNING USING
            MESSAGE = format('Table %I.%I has no PRIMARY KEY', t.nspname, t.relname),
            HINT = 'Tables without a PRIMARY KEY cannot be UPDATEd or DELETEd from, only INSERTed into. Add a PRIMARY KEY.';
    END LOOP;

    -- Create ON TRUNCATE triggers for BDR on existing tables
    -- See bdr_truncate_trigger_add for the matching event trigger for tables
    -- created after join.
    --
    -- The triggers may be created already because the bdr event trigger
    -- runs when the bdr extension is created, even if there's no active
    -- bdr connections yet, so tables created after the extension is created
    -- will get the trigger already. So skip tables that have a tg named
    -- 'truncate_trigger' calling proc 'bdr.queue_truncate'.
    FOR t IN
        SELECT r.oid AS relid
        FROM pg_class r
          INNER JOIN pg_namespace n ON (r.relnamespace = n.oid)
          LEFT JOIN pg_trigger tg ON (r.oid = tg.tgrelid AND tgname = 'truncate_trigger')
          LEFT JOIN pg_proc p ON (p.oid = tg.tgfoid AND p.proname = 'queue_truncate')
          LEFT JOIN pg_namespace pn ON (pn.oid = p.pronamespace AND pn.nspname = 'bdr')
        WHERE r.relpersistence = 'p'
          AND r.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'bdr', 'information_schema')
          AND tg.oid IS NULL AND p.oid IS NULL and pn.oid IS NULL
    LOOP
        -- We use a C function here because in addition to trigger creation
        -- we must also mark it tgisinternal.
        PERFORM bdr.bdr_internal_create_truncate_trigger(t.relid);
    END LOOP;

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

CREATE OR REPLACE FUNCTION bdr.bdr_part_by_node_names(p_nodes text[])
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
DECLARE
    unknown_node_names text := NULL;
    r record;
BEGIN
    -- concurrency
    LOCK TABLE bdr.bdr_connections IN EXCLUSIVE MODE;
    LOCK TABLE bdr.bdr_nodes IN EXCLUSIVE MODE;
    LOCK TABLE pg_catalog.pg_shseclabel IN EXCLUSIVE MODE;

    -- Ensure we're not running on the node being parted.
    -- We can't safely ensure that the change gets replicated
    -- to peer nodes before we cut off our local connections
    -- if running on the node being parted.
    --
    -- (This restriction can be lifted later if we add
    --  multi-phase negotiated part).
    --
    IF bdr.bdr_get_local_node_name() = ANY(p_nodes) THEN
        RAISE USING
            MESSAGE = 'Cannot part a node from its self',
            DETAIL = 'Attempted to bdr_part_by_node_names(...) on node '||bdr.bdr_get_local_node_name()||' which is one of the nodes being parted',
            HINT = 'You must call call bdr_part_by_node_names on a node that is not being removed',
            ERRCODE = 'object_in_use';
    END IF;

    SELECT
        string_agg(to_remove.remove_node_name, ', ')
    FROM
        bdr.bdr_nodes
        RIGHT JOIN unnest(p_nodes) AS to_remove(remove_node_name)
        ON (bdr_nodes.node_name = to_remove.remove_node_name)
    WHERE bdr_nodes.node_name IS NULL
    INTO unknown_node_names;

    IF unknown_node_names IS NOT NULL THEN
        RAISE USING
            MESSAGE = format('No node(s) named %s found', unknown_node_names),
            ERRCODE = 'no_data_found';
    END IF;

    FOR r IN
        SELECT
            node_name, node_status
        FROM
            bdr.bdr_nodes
            INNER JOIN unnest(p_nodes) AS to_remove(remove_node_name)
            ON (bdr_nodes.node_name = to_remove.remove_node_name)
        WHERE bdr_nodes.node_status <> 'r'
    LOOP
        RAISE WARNING 'Node % is in state % not expected ''r''. Attempting to remove anyway.',
            r.node_name, r.node_status;
    END LOOP;

    UPDATE bdr.bdr_nodes
    SET node_status = 'k'
    WHERE node_name = ANY(p_nodes);

    -- Notify local perdb worker to kill nodes.
    PERFORM bdr.bdr_connections_changed();

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

CREATE FUNCTION bdr_upgrade_to_090(my_conninfo text, local_conninfo text, remote_conninfo text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'Upgrades from 0.7.x or 0.8.x direct to 1.0+ not supported';
END;
$$;

REVOKE ALL ON FUNCTION bdr_upgrade_to_090(text,text,text) FROM public;

CREATE TYPE bdr.bdr_sync_type AS ENUM
(
	'none',
	'full'
);

CREATE FUNCTION bdr.bdr_subscribe(
    local_node_name text,
    subscribe_to_dsn text,
    node_local_dsn text,
    apply_delay integer DEFAULT NULL,
    replication_sets text[] DEFAULT ARRAY['default'],
    synchronize bdr.bdr_sync_type DEFAULT 'full'
    )
RETURNS void LANGUAGE plpgsql VOLATILE
AS $body$
BEGIN
	RAISE EXCEPTION 'Unidirectional subscribe is no longer supported by BDR, use pglogical';
END;
$body$;

COMMENT ON FUNCTION bdr.bdr_subscribe(text, text, text, integer, text[], bdr.bdr_sync_type)
IS 'Subscribe to remote logical changes - no longer supported, use pglogical instead';

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

CREATE OR REPLACE FUNCTION bdr.bdr_apply_is_paused()
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.bdr_apply_is_paused()
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION bdr.bdr_node_set_read_only(
    node_name text,
    read_only boolean
) RETURNS void LANGUAGE C VOLATILE
AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.bdr_drop_remote_slot(sysid text, timeline oid, dboid oid)
RETURNS boolean LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.bdr_get_apply_pid(sysid text, timeline oid, dboid oid)
RETURNS integer LANGUAGE C VOLATILE STRICT
AS 'MODULE_PATHNAME';

CREATE OR REPLACE FUNCTION bdr.bdr_unsubscribe(node_name text, drop_slot boolean DEFAULT true)
RETURNS void LANGUAGE plpgsql VOLATILE
SET search_path = bdr, pg_catalog
SET bdr.permit_unsafe_ddl_commands = on
SET bdr.skip_ddl_replication = on
SET bdr.skip_ddl_locking = on
AS $body$
BEGIN
	RAISE EXCEPTION 'bdr unidirectional subscriptions no longer supported, use pglogical';
END;
$body$;

CREATE OR REPLACE FUNCTION bdr.bdr_internal_sequence_reset_cache(seq regclass)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME' STRICT;

-- Define an alias of pg_catalog.pg_get_replication_slots to expose the extra
-- pid and confirmed_flush_lsn columns from 9.5 and 9.6 that have been
-- backported to 94bdr. The new columns MUST be last.
--
-- On 9.6 we skip creating this function and make the view a simple wrapper
-- for the real one. This means the column order will be different, but
-- meh, whatever.
--
DO $DO$
BEGIN
	CASE current_setting('server_version_num')::int / 100
	WHEN 904 THEN

		CREATE OR REPLACE FUNCTION
		bdr.pg_get_replication_slots(OUT slot_name name, OUT plugin name, OUT slot_type text, OUT datoid oid, OUT active boolean, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn pg_lsn, OUT active_pid integer, OUT confirmed_flush_lsn pg_lsn)
		 RETURNS SETOF record
		 LANGUAGE internal
		 STABLE ROWS 10
		AS $function$pg_get_replication_slots$function$;


		-- And a replacement pg_replication_slots view, so if you set your search_path
		-- appropriately it'll "just work". The view fixes the column ordering to be
		-- the same as 9.6.
		CREATE VIEW bdr.pg_replication_slots AS
			SELECT
					L.slot_name,
					L.plugin,
					L.slot_type,
					L.datoid,
					D.datname AS database,
					L.active,
					L.active_pid,
					L.xmin,
					L.catalog_xmin,
					L.restart_lsn,
					L.confirmed_flush_lsn
			FROM bdr.pg_get_replication_slots() AS L
					LEFT JOIN pg_catalog.pg_database D ON (L.datoid = D.oid);

	WHEN 906 THEN

		CREATE VIEW bdr.pg_replication_slots AS
		SELECT slot_name, plugin, slot_type, datoid, database, active, active_pid, xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn
		FROM pg_replication_slots;

	ELSE
		RAISE EXCEPTION 'Pg version % not supported', current_setting('server_version_num');
	END CASE;
END;
$DO$;

CREATE OR REPLACE FUNCTION terminate_apply_workers(node_name text)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_apply_workers_byname';

CREATE OR REPLACE FUNCTION terminate_apply_workers(sysid text, timeline oid, dboid oid)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_apply_workers';

CREATE OR REPLACE FUNCTION terminate_walsender_workers(node_name text)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_walsender_workers_byname';

CREATE OR REPLACE FUNCTION terminate_walsender_workers(sysid text, timeline oid, dboid oid)
RETURNS boolean LANGUAGE c AS 'MODULE_PATHNAME','bdr_terminate_walsender_workers';

COMMENT ON FUNCTION terminate_walsender_workers(node_name text)
IS 'terminate walsender workers connected to the named node';

COMMENT ON FUNCTION terminate_apply_workers(node_name text)
IS 'terminate apply workers connected to the named node';

COMMENT ON FUNCTION terminate_walsender_workers(sysid text, timeline oid, dboid oid)
IS 'terminate walsender connected to the node with the given identity';

COMMENT ON FUNCTION terminate_apply_workers(sysid text, timeline oid, dboid oid)
IS 'terminate apply workers connected to the node with the given identity';

CREATE OR REPLACE FUNCTION bdr.skip_changes_upto(from_sysid text,
    from_timeline oid, from_dboid oid, upto_lsn pg_lsn)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME','bdr_skip_changes_upto';

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
    RAISE WARNING 'There are node-specific override entries for node % in bdr.bdr_connections. Only the default connection''s replication sets will be changed. Use the 6-argument form of this function to change others.',node_name;
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
    RAISE WARNING 'There are node-specific override entries for node % in bdr.bdr_connections. Only the default connection''s replication sets will be returned.',node_name;
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
    IF bdr.have_global_sequences() THEN
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
      RAISE INFO 'BDR 1.0 global sequences not supported, nothing to convert';
    END IF;
  ELSE
    RAISE NOTICE 'BDR 1.0 global sequences not converted to local; they will not work until a new nodegroup is created';
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

  -- and replication origins/identifiers
  CASE current_setting('server_version_num')::int / 100
  WHEN 904 THEN
    PERFORM pg_replication_identifier_drop(riname)
    FROM pg_catalog.pg_replication_identifier,
         bdr.bdr_parse_replident_name(riname) pi
    WHERE pi.local_dboid = (select oid from pg_database where datname = current_database());
  WHEN 906 THEN
    PERFORM pg_replication_origin_drop(roname)
    FROM pg_catalog.pg_replication_origin,
         bdr.bdr_parse_replident_name(roname) pi
    WHERE pi.local_dboid = (select oid from pg_database where datname = current_database());
  ELSE
    RAISE EXCEPTION 'Only PostgreSQL 9.4bdr and 9.6 are supported';
  END CASE;

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

CREATE FUNCTION bdr.bdr_is_active_in_db()
RETURNS boolean LANGUAGE c
AS 'MODULE_PATHNAME','bdr_is_active_in_db';

DROP VIEW bdr.bdr_node_slots;

CREATE VIEW bdr.bdr_node_slots AS
SELECT n.node_name,
 s.slot_name, s.restart_lsn AS slot_restart_lsn, s.confirmed_flush_lsn AS slot_confirmed_lsn,
 s.active AS walsender_active,
 s.active_pid AS walsender_pid,
 r.sent_location, r.write_location, r.flush_location, r.replay_location
FROM
 bdr.pg_replication_slots s
 CROSS JOIN LATERAL bdr.bdr_parse_slot_name(s.slot_name) ps(remote_sysid, remote_timeline, remote_dboid, local_dboid, replication_name)
 INNER JOIN bdr.bdr_nodes n ON ((n.node_sysid = ps.remote_sysid) AND (n.node_timeline = ps.remote_timeline) AND (n.node_dboid = ps.remote_dboid))
 LEFT JOIN pg_catalog.pg_stat_replication r ON (r.pid = s.active_pid)
WHERE ps.local_dboid = (select oid from pg_database where datname = current_database())
  AND s.plugin = 'bdr';

CREATE EVENT TRIGGER bdr_truncate_trigger_add
ON ddl_command_end
EXECUTE PROCEDURE bdr.bdr_truncate_trigger_add();

ALTER EVENT TRIGGER bdr_truncate_trigger_add ENABLE ALWAYS;

-- a.k.a have_seqam , HAVE_SEQAM
CREATE OR REPLACE FUNCTION bdr.have_global_sequences()
RETURNS boolean
LANGUAGE SQL
AS $$
SELECT (current_setting('server_version_num')::int / 100) = 904;
$$;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
