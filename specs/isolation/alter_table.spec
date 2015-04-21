# various ALTER TABLE combinations
conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"

setup
{
 SET bdr.permit_ddl_locking = true;
 CREATE TABLE tst (a INTEGER PRIMARY KEY, b TEXT);
 INSERT INTO tst (a, b) VALUES (1, 'one'), (2, 'two'), (3, 'three');
}

teardown
{
 SET bdr.permit_ddl_locking = true;
 DROP TABLE tst;
}

session "s1"
connection "node1"
step "n1setup" { SET bdr.permit_ddl_locking = true; }
step "n1read" { SELECT * FROM tst ORDER BY a; }
step "n1sync" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; }
step "n1s1" { INSERT INTO tst (a, b) VALUES (4, 'four'); }
step "n1s2" { ALTER TABLE tst ADD COLUMN c TEXT; }
step "n1s3" { INSERT INTO tst (a, b, c) VALUES (5, 'five', 'new'); }
step "n1s4" { UPDATE tst set c = 'updated' WHERE c IS NULL; }
step "n1s5" { ALTER TABLE tst ALTER COLUMN c SET DEFAULT 'dflt'; }
step "n1s6" { INSERT INTO tst (a, b) VALUES (6, 'six'); }
step "n1s7" { DELETE FROM tst where a = 5; }
step "n1s8" { ALTER TABLE tst ADD COLUMN d INTEGER DEFAULT 100; }
step "n1s9" { BEGIN; }
step "n1s10" { ALTER TABLE tst DROP COLUMN c; }
step "n1s11" { COMMIT; }

session "s2"
connection "node2"
step "n2setup" { SET bdr.permit_ddl_locking = true; }
step "n2sync" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; }
step "n2read" { SELECT * FROM tst ORDER BY a; }
step "n2s1" { UPDATE tst SET c = 'changed' WHERE a = 1; }
step "n2s2" { UPDATE tst SET b = 'changed' WHERE a = 1; }

permutation "n1setup" "n2setup" "n1s1" "n1sync" "n2read" "n1s2" "n1sync" "n2read" "n1s3" "n1s4" "n1sync" "n2read" "n1s5" "n1s6" "n1sync" "n2read" "n1s7" "n1sync" "n2read" "n1s8" "n1s9" "n1s10" "n2s1" "n2sync" "n1s11" "n1sync" "n2sync" "n2read" "n1read" "n2s2" "n2sync" "n1read"
