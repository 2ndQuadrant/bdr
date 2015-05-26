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
