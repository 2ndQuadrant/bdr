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
