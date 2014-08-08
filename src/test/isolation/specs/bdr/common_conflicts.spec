conninfo "d1" "port=5433 dbname=postgres"
conninfo "d2" "port=5434 dbname=postgres"

session "sess1"
connection "conn1"
setup { DROP TABLE IF EXISTS t1; CREATE TABLE t1(pk integer primary key, col1 text); }

step "s1_insert_pk1" { INSERT INTO t1 (pk, col1) VALUES (1, 'test'); }
step "s1_insert_pk2" { INSERT INTO t1 (pk, col1) VALUES (2, 'testval'); }
step "s1_update" { UPDATE t1 SET col1 = 'test1' WHERE pk = 1; }
step "s1_update_to_pk2" { UPDATE t1 SET pk = 2 WHERE pk = 1; }
step "s1_delete" { DELETE FROM t1 WHERE pk = 1; }

step "s1_sleep7" { SELECT pg_sleep(7); }
step "s1_wait_repl" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; }

session "sess2"
connection "conn2"

step "s2_insert_pk1" { INSERT INTO t1 (pk, col1) VALUES (1, 'testval'); }
step "s2_insert_pk2" { INSERT INTO t1 (pk, col1) VALUES (2, 'testval'); }
step "s2_update" { UPDATE t1 SET col1 = 'test2' WHERE pk = 1; }
step "s2_update_to_pk2" { UPDATE t1 SET pk = 2 WHERE pk = 1; }
step "s2_delete" { DELETE FROM t1 WHERE pk = 1; }

# INSERT vs INSERT
permutation "s1_wait_repl" "s2_insert_pk1" "s1_insert_pk1" "s1_sleep7"

# UPDATE vs UPDATE
permutation "s1_wait_repl" "s1_insert_pk1" "s1_wait_repl" "s2_update" "s1_update" "s1_sleep7"

# UPDATE vs DELETE
# TODO crashing
permutation "s1_wait_repl" "s1_insert_pk1" "s1_wait_repl" "s2_delete" "s1_update" "s1_sleep7"
permutation "s1_wait_repl" "s1_insert_pk1" "s1_wait_repl" "s2_update" "s1_delete" "s1_sleep7"

# INSERT vs UPDATE
# TODO Crashing
permutation "s1_wait_repl" "s1_insert_pk1" "s1_wait_repl" "s2_insert_pk2" "s1_update_to_pk2" "s1_sleep7"
permutation "s1_wait_repl" "s1_insert_pk1" "s1_wait_repl" "s2_update_to_pk2" "s1_insert_pk2" "s1_sleep7"
