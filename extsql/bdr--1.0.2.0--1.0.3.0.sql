--
-- We need these columns to exist on BDR 1.0 in order for an upgrade to BDR 2.0
-- to succeed.
--

SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SET LOCAL bdr.skip_ddl_replication = true;
SET LOCAL search_path = bdr;

ALTER TABLE bdr_queued_commands
  ADD COLUMN search_path TEXT;

UPDATE bdr_queued_commands
SET search_path = '';

ALTER TABLE bdr_queued_commands
  ALTER COLUMN search_path SET DEFAULT '';

ALTER TABLE bdr_queued_commands
  ALTER COLUMN search_path SET NOT NULL;

ALTER TABLE bdr.bdr_nodes
  ADD COLUMN node_seq_id smallint;

ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN remote_node_timeline oid;
ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN remote_node_dboid oid;
ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN local_tuple_origin_timeline oid;
ALTER TABLE bdr.bdr_conflict_history
  ADD COLUMN local_tuple_origin_dboid oid;

-- Conflict history should never be in dumps
SELECT pg_catalog.pg_extension_config_dump('bdr_conflict_history', 'WHERE false');

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
