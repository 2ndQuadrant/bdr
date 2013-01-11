-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_logical_decoding" to load this file. \quit

CREATE FUNCTION start_logical_replication (slotname name, pos text, VARIADIC options text[] DEFAULT '{}', OUT location text, OUT xid bigint, OUT data text) RETURNS SETOF record
AS 'MODULE_PATHNAME', 'start_logical_replication'
LANGUAGE C IMMUTABLE STRICT;
