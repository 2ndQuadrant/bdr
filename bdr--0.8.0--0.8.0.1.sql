CREATE TABLE bdr.bdr_replication_set_config
(
    set_name name PRIMARY KEY,
    replicate_inserts bool NOT NULL DEFAULT true,
    replicate_updates bool NOT NULL DEFAULT true,
    replicate_deletes bool NOT NULL DEFAULT true
);
ALTER TABLE bdr.bdr_replication_set_config SET (user_catalog_table = true);
REVOKE ALL ON TABLE bdr.bdr_replication_set_config FROM PUBLIC;


-- We can't use ALTER TYPE ... ADD inside transaction, so do it the hard way...
ALTER TYPE bdr.bdr_conflict_resolution RENAME TO bdr_conflict_resolution_old;

CREATE TYPE bdr.bdr_conflict_resolution AS ENUM
(
    'conflict_trigger_skip_change',
    'conflict_trigger_returned_tuple',
    'last_update_wins_keep_local',
    'last_update_wins_keep_remote',
    'apply_change',
    'skip_change',
    'unhandled_tx_abort'
);

COMMENT ON TYPE bdr.bdr_conflict_resolution IS 'Resolution of a bdr conflict - if a conflict was resolved by a conflict trigger, by last-update-wins tests on commit timestamps, etc.';

ALTER TABLE bdr.bdr_conflict_history ALTER COLUMN conflict_resolution TYPE bdr.bdr_conflict_resolution USING conflict_resolution::text::bdr.bdr_conflict_resolution;

DROP TYPE bdr.bdr_conflict_resolution_old;
