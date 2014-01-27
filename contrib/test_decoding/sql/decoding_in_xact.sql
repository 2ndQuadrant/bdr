CREATE EXTENSION test_decoding;
-- predictability
SET synchronous_commit = on;

BEGIN;
SELECT txid_current() = 0;
SELECT 'init' FROM pg_create_decoding_replication_slot('regression_slot', 'test_decoding');
ROLLBACK;

SELECT 'init' FROM pg_create_decoding_replication_slot('regression_slot', 'test_decoding');
BEGIN;
SELECT txid_current() = 0;
SELECT data FROM pg_decoding_slot_get_changes('regression_slot', 'now', 'include-xids', '0');
ROLLBACK;

BEGIN;
SELECT data FROM pg_decoding_slot_get_changes('regression_slot', 'now', 'include-xids', '0');
COMMIT;

BEGIN;
COMMIT;

SELECT data FROM pg_decoding_slot_get_changes('regression_slot', 'now', 'include-xids', '0');

SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');

BEGIN;
SELECT 'init' FROM pg_create_decoding_replication_slot('regression_slot', 'test_decoding');
COMMIT;

SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');
