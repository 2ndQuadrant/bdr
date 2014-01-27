-- will fail if wal_level < logical, separate expected file
SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');
SELECT 'init' FROM pg_create_decoding_replication_slot('regression_slot', 'test_decoding');
COPY (SELECT data FROM pg_decoding_slot_get_changes('regression_slot', 'now', 'include-xids', '0')) TO STDOUT;
