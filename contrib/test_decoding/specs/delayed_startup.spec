setup
{
    DROP TABLE IF EXISTS do_write;
    DROP EXTENSION IF EXISTS test_decoding;
    CREATE EXTENSION test_decoding;
    CREATE TABLE do_write(id serial primary key);
}

teardown
{
    DROP TABLE do_write;
    DROP EXTENSION test_decoding;
    SELECT 'stop' FROM pg_drop_replication_slot('isolation_slot');
}

session "s1"
setup { SET synchronous_commit=on; }
step "s1b" { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "s1w" { INSERT INTO do_write DEFAULT VALUES; }
step "s1c" { COMMIT; }
session "s2"
setup { SET synchronous_commit=on; }
step "s2init" {SELECT 'init' FROM pg_create_decoding_replication_slot('isolation_slot', 'test_decoding');}
step "s2start" {SELECT data FROM pg_decoding_slot_get_changes('isolation_slot', 'now', 'include-xids', 'false');}


permutation "s1b" "s1w" "s2init" "s1c" "s2start" "s1b" "s1w" "s1c" "s2start" "s1b" "s1w" "s2start" "s1c" "s2start"
