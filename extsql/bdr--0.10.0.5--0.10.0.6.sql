SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE OR REPLACE FUNCTION bdr.bdr_internal_sequence_reset_cache(seq regclass)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME' STRICT;

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
