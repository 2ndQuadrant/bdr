# test conflict handling on primary key changes
conninfo "node1" "port=15432 dbname=test_db"
conninfo "node2" "port=15433 dbname=test_db"
conninfo "node3" "port=15434 dbname=test_db"

setup
{
 CREATE TABLE tst (a INTEGER PRIMARY KEY, b TEXT);
 INSERT INTO tst (a, b) VALUES (1, 'one');
}

teardown
{
 DROP TABLE tst;
}

session "s1"
connection "node1"
step "n1read" { SELECT * FROM tst; }
step "n1sync" { select pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication; }
step "n1s1" { UPDATE tst SET a = 2; }
step "n1s2" { BEGIN; }
step "n1s3" { UPDATE tst SET a = 4; }
step "n1s4" { COMMIT; }

session "s2"
connection "node2"
step "n2read" { SELECT * FROM tst; }
step "n2sync" { select pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication; }
step "n2s1" { UPDATE tst SET a = 3; }
step "n2s2" { BEGIN; }
step "n2s3" { UPDATE tst SET a = 5; }
step "n2s4" { COMMIT; }

session "s3"
connection "node3"
step "n3read" { SELECT * FROM tst; }
step "n3sync" { select pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication; }

permutation "n1sync" "n1s1" "n1sync" "n3read" "n2s1" "n2sync" "n3read" "n1s2" "n2s2" "n1s3" "n2s3" "n1s4" "n1sync" "n2s4" "n2sync" "n1read" "n2read" "n3read"
