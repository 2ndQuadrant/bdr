SET bdr.skip_ddl_replication = on;
SET bdr.permit_unsafe_ddl_commands = on;

ALTER TABLE bdr.bdr_sequence_values
    ALTER COLUMN owning_sysid TYPE text COLLATE "C",
    ALTER COLUMN owning_riname TYPE text COLLATE "C",
    ALTER COLUMN seqschema TYPE text COLLATE "C",
    ALTER COLUMN seqname TYPE text COLLATE "C";

ALTER TABLE bdr.bdr_sequence_elections
    ALTER COLUMN owning_sysid TYPE text COLLATE "C",
    ALTER COLUMN owning_riname TYPE text COLLATE "C",
    ALTER COLUMN seqschema TYPE text COLLATE "C",
    ALTER COLUMN seqname TYPE text COLLATE "C",
    ALTER COLUMN vote_type TYPE text COLLATE "C"
    ;

ALTER TABLE bdr.bdr_votes
    ALTER COLUMN voter_sysid TYPE text COLLATE "C",
    ALTER COLUMN voter_riname TYPE text COLLATE "C",
    ALTER COLUMN vote_sysid TYPE text COLLATE "C",
    ALTER COLUMN vote_riname TYPE text COLLATE "C",
    ALTER COLUMN reason TYPE text COLLATE "C"
    ;
