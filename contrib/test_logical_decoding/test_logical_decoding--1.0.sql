-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_logical_decoding" to load this file. \quit

CREATE FUNCTION init_logical_replication (slotname name, plugin name, OUT slotname text, OUT xlog_position text)
AS 'MODULE_PATHNAME', 'init_logical_replication'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION start_logical_replication (slotname name, pos text, VARIADIC options text[] DEFAULT '{}', OUT location text, OUT xid bigint, OUT data text) RETURNS SETOF record
AS 'MODULE_PATHNAME', 'start_logical_replication'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION stop_logical_replication (slotname name) RETURNS int
AS 'MODULE_PATHNAME', 'stop_logical_replication'
LANGUAGE C IMMUTABLE STRICT;
