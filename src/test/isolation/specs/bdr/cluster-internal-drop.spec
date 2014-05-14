conninfo "d1" "port=5433 dbname=test_db"

session "sess1"
connection "d1"
step "s1_create_table" { CREATE TABLE test(pk int primary key, col1 text); }
step "s1_create_index" { CREATE INDEX test_idx ON test(pk, col1); }

step "s1_drop_index" { DROP INDEX test_idx; }
step "s1_drop_col" { ALTER TABLE test DROP COLUMN col1; }
step "s1_drop_table" { DROP TABLE test; }

permutation "s1_create_table" "s1_drop_table"
permutation "s1_create_table" "s1_create_index" "s1_drop_index" "s1_drop_table"
permutation "s1_create_table" "s1_create_index" "s1_drop_index" "s1_drop_col" "s1_drop_table"
