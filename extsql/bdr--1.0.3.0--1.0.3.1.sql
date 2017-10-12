SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;
SET LOCAL search_path = bdr;

CREATE OR REPLACE FUNCTION
bdr.wait_slot_confirm_lsn(slotname name, target pg_lsn)
RETURNS void LANGUAGE c AS 'bdr','bdr_wait_slot_confirm_lsn';

COMMENT ON FUNCTION bdr.wait_slot_confirm_lsn(name,pg_lsn) IS
'wait until slotname (or all slots, if null) has passed specified lsn (or current lsn, if null)';

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
