SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE FUNCTION bdr_get_local_nodeid( sysid OUT oid, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

-- bdr_get_local_nodeid is intentionally not revoked from all, it's read-only

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
