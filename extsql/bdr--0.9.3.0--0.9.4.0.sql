--
-- We need these columns to exist on BDR 0.9 in order for an upgrade to BDR 1.0.3, which 
-- in turn needed them to support upgrade to 2.0.
--
-- 2.0.6 is actually tolerant of missing columns and of extra columns if they're null,
-- but that doesn't help us with upgrades from back branches.
--

SET LOCAL bdr.permit_unsafe_ddl_commands = true;
SET LOCAL bdr.skip_ddl_replication = true;
SET LOCAL search_path = bdr;

ALTER TABLE bdr_queued_commands
  ADD COLUMN search_path TEXT;

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

RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
