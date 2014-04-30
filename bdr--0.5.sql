--\echo Use "CREATE EXTENSION bdr" to load this file. \quit

--CREATE ROLE bdr NOLOGIN SUPERUSER;
--SET ROLE bdr;

CREATE SCHEMA bdr;
GRANT USAGE ON SCHEMA bdr TO public;

SET LOCAL search_path = bdr;

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

-- We must be able to use exclusion constraints for global sequences
SET bdr.permit_unsafe_ddl_commands=true;

CREATE TABLE bdr_sequence_values
(
    owning_sysid text NOT NULL,
    owning_tlid oid NOT NULL,
    owning_dboid oid NOT NULL,
    owning_riname text NOT NULL,

    seqschema text NOT NULL,
    seqname text NOT NULL,
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
SELECT pg_catalog.pg_extension_config_dump('bdr_sequence_values', '');

SET bdr.permit_unsafe_ddl_commands=false;

REVOKE ALL ON TABLE bdr_sequence_values FROM PUBLIC;

CREATE INDEX bdr_sequence_values_chunks ON bdr_sequence_values(seqschema, seqname, seqrange);
CREATE INDEX bdr_sequence_values_newchunk ON bdr_sequence_values(seqschema, seqname, upper(seqrange));

CREATE TABLE bdr_sequence_elections
(
    owning_sysid text NOT NULL,
    owning_tlid oid NOT NULL,
    owning_dboid oid NOT NULL,
    owning_riname text NOT NULL,
    owning_election_id bigint NOT NULL,

    seqschema text NOT NULL,
    seqname text NOT NULL,
    seqrange int8range NOT NULL,

    /* XXX id */

    vote_type text NOT NULL,

    open bool NOT NULL,
    success bool NOT NULL DEFAULT false,

    PRIMARY KEY(owning_sysid, owning_tlid, owning_dboid, owning_riname, seqschema, seqname, seqrange)
);
SELECT pg_catalog.pg_extension_config_dump('bdr_sequence_elections', '');
REVOKE ALL ON TABLE bdr_sequence_values FROM PUBLIC;


CREATE TABLE bdr_votes
(
    vote_sysid text NOT NULL,
    vote_tlid oid NOT NULL,
    vote_dboid oid NOT NULL,
    vote_riname text NOT NULL,
    vote_election_id bigint NOT NULL,

    voter_sysid text NOT NULL,
    voter_tlid oid NOT NULL,
    voter_dboid bigint NOT NULL,
    voter_riname text NOT NULL,

    vote bool NOT NULL,
    reason text CHECK (reason IS NULL OR vote = false),
    UNIQUE(vote_sysid, vote_tlid, vote_dboid, vote_riname, vote_election_id, voter_sysid, voter_tlid, voter_dboid, voter_riname)
);
SELECT pg_catalog.pg_extension_config_dump('bdr_votes', '');
REVOKE ALL ON TABLE bdr_votes FROM PUBLIC;

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

CREATE TABLE bdr_queued_commands (
    lsn pg_lsn NOT NULL,
    queued_at timestamptz NOT NULL,
    obj_type text,
    obj_identity text,
    command text,
    executed bool
);

CREATE OR REPLACE FUNCTION bdr.queue_truncate()
 RETURNS TRIGGER
 LANGUAGE plpgsql
AS $function$
DECLARE
    ident TEXT;
BEGIN

    ident := quote_ident(TG_TABLE_SCHEMA)||'.'||quote_ident(TG_TABLE_NAME);

    INSERT INTO bdr.bdr_queued_commands (
        lsn, queued_at,
        obj_type, obj_identity, command, executed
    )
        VALUES (
            pg_current_xlog_location(),
            NOW(),
            'table',
            ident,
            'TRUNCATE TABLE ONLY ' || ident,
            'false');
    RETURN NULL;
END;
$function$;

CREATE OR REPLACE FUNCTION bdr.queue_commands()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    r RECORD;
BEGIN
    IF pg_replication_identifier_is_replaying() THEN
       RETURN;
    END IF;
    IF current_setting('bdr.skip_ddl_replication')::boolean THEN
        -- If we're doing a pg_restore from a remote BDR node's
        -- state, we must not create truncate triggers etc because
        -- they'll get copied over in the dump.
        RETURN;
    END IF;

    FOR r IN SELECT * FROM pg_event_trigger_get_creation_commands()
    LOOP
        /* ignore temporary objects */
        IF r.schema = 'pg_temp' THEN
            CONTINUE;
        END IF;

        /* ignore objects that are part of an extension */
        IF r.in_extension THEN
            CONTINUE;
        END IF;

        INSERT INTO bdr.bdr_queued_commands(
            lsn, queued_at,
            obj_type, obj_identity, command, executed
        )
            VALUES (
                pg_current_xlog_location(),
                NOW(),
                r.object_type,
                r.identity,
                pg_catalog.pg_event_trigger_expand_command(r.command),
                'false');

        IF r.command_tag = 'CREATE TABLE' and r.object_type = 'table' THEN
            EXECUTE 'CREATE TRIGGER truncate_trigger AFTER TRUNCATE ON ' ||
                    r.identity ||
                    ' FOR EACH STATEMENT EXECUTE PROCEDURE bdr.queue_truncate()';
        END IF;
    END LOOP;
END;
$function$;



-- The bdr_nodes table tracks members of a BDR group; it's only concerned with
-- one database, so the local and foreign database names are implicit.  All we
-- care about is the sysid.
--
-- The sysid must be a numeric (or string) because PostgreSQL has no uint64 SQL
-- type.
--
-- In future we may support different local dbnames, so store the dbname too.
-- It's even possible we might replicate from one local DB to another (though
-- who knows why we'd want to) so the PK should be the (dbname, sysid) tuple.
--
CREATE TABLE bdr_nodes (
    node_sysid numeric,
    node_dbname name not null,
    node_status "char" not null,
    primary key(node_sysid, node_dbname),
    check (node_status in ('i', 'c', 'r'))
);

COMMENT ON TABLE bdr_nodes IS 'All known nodes in this BDR group.';
COMMENT ON COLUMN bdr_nodes.node_sysid IS 'system_identifier from the control file of the node';
COMMENT ON COLUMN bdr_nodes.node_dbname IS 'local database name on the node';
COMMENT ON COLUMN bdr_nodes.node_status IS 'Readiness of the node: [i]nitializing, [c]atchup, [r]eady. Doesn''t indicate connected/disconnected.';

SELECT pg_catalog.pg_extension_config_dump('bdr_nodes', '');

-- This type is tailored to use as input to get_object_address
CREATE TYPE bdr.dropped_object AS
  (objtype text, objnames text[], objargs text[]);

CREATE TABLE bdr.bdr_queued_drops(
    lsn pg_lsn NOT NULL,
    queued_at timestamptz NOT NULL,
    dropped_objects bdr.dropped_object[] NOT NULL
);

CREATE OR REPLACE FUNCTION bdr.queue_dropped_objects()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    r RECORD;
	dropped bdr.dropped_object;
	otherobjs bdr.dropped_object[] = '{}';
BEGIN
	FOR r IN SELECT * FROM pg_event_trigger_dropped_objects()
	LOOP
		IF r.original OR r.normal THEN
			dropped.objtype = r.object_type;
			dropped.objnames = r.address_names;
			dropped.objargs = r.address_args;
			otherobjs := otherobjs || dropped;
			RAISE LOG 'object is: %', dropped;
		END IF;
	END LOOP;

	IF otherobjs <> '{}' THEN
		INSERT INTO bdr.bdr_queued_drops (
			lsn, queued_at, dropped_objects
		)
		VALUES (pg_current_xlog_location(),
			NOW(),
			otherobjs
		);
	END IF;
END;
$function$;

CREATE EVENT TRIGGER queue_drops
ON sql_drop
EXECUTE PROCEDURE bdr.queue_dropped_objects();

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
--- this should always be last to avoid replicating our internal schema
---

CREATE EVENT TRIGGER queue_commands
ON ddl_command_end
WHEN tag IN ('create table', 'create index', 'create sequence',
     'create schema', 'alter sequence',
     'create trigger', 'alter table', 'create extension', 'create type')
EXECUTE PROCEDURE bdr.queue_commands();

RESET search_path;
