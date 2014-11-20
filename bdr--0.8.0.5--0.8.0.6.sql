-- Data structures for BDR's dynamic configuration management

SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE FUNCTION bdr_get_local_nodeid( sysid OUT oid, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'MODULE_PATHNAME';

-- bdr_get_local_nodeid is intentionally not revoked from all, it's read-only

CREATE FUNCTION bdr_start_perdb_worker()
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

REVOKE ALL ON FUNCTION bdr_start_perdb_worker() FROM public;

COMMENT ON FUNCTION bdr_start_perdb_worker() IS 'Internal BDR function, do not call directly.';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
