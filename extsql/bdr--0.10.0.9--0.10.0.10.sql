SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

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

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
