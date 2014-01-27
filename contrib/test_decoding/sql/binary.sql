CREATE EXTENSION test_decoding;
-- predictability
SET synchronous_commit = on;

SELECT 'init' FROM pg_create_decoding_replication_slot('regression_slot', 'test_decoding');
-- succeeds, textual plugin, textual consumer
SELECT data FROM pg_decoding_slot_get_changes('regression_slot', 'now', 'force-binary', '0');
-- fails, binary plugin, textual consumer
SELECT data FROM pg_decoding_slot_get_changes('regression_slot', 'now', 'force-binary', '1');
-- succeeds, textual plugin, binary consumer
SELECT data FROM pg_decoding_slot_get_binary_changes('regression_slot', 'now', 'force-binary', '0');
-- succeeds, binary plugin, binary consumer
SELECT data FROM pg_decoding_slot_get_binary_changes('regression_slot', 'now', 'force-binary', '1');
