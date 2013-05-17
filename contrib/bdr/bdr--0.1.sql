--\echo Use "CREATE EXTENSION bdr" to load this file. \quit

CREATE FUNCTION pg_stat_bdr(
    OUT rep_node_id oid,
    OUT riremotesysid name,
    OUT riremotedb oid,
    OUT rilocaldb oid,
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

CREATE VIEW pg_stat_bdr AS SELECT * FROM pg_stat_bdr();
REVOKE ALL ON FUNCTION pg_stat_bdr() FROM PUBLIC;
