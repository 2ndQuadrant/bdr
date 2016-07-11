SET LOCAL search_path = bdr;
-- We must be able to use exclusion constraints for global sequences
SET bdr.permit_unsafe_ddl_commands=true;

-- Fix an oversight in the update to the bdr.bdr_nodes table
-- for 0.9.x compatibility: alter the check constraint to allow
-- all the 0.9.x and 0.10.x node states.

ALTER TABLE bdr.bdr_nodes
  DROP CONSTRAINT bdr_nodes_node_status_check;

ALTER TABLE bdr.bdr_nodes
  ADD CONSTRAINT bdr_nodes_node_status_check
    CHECK (node_status in ('b', 'i', 'c', 'o', 'r', 'k'));

SET bdr.permit_unsafe_ddl_commands = false;
RESET search_path;
