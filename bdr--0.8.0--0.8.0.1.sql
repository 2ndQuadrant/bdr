CREATE TABLE bdr.bdr_replication_set_config
(
    set_name name PRIMARY KEY,
    replicate_inserts bool NOT NULL DEFAULT true,
    replicate_updates bool NOT NULL DEFAULT true,
    replicate_deletes bool NOT NULL DEFAULT true
);
ALTER TABLE bdr.bdr_replication_set_config SET (user_catalog_table = true);
REVOKE ALL ON TABLE bdr.bdr_replication_set_config FROM PUBLIC;
