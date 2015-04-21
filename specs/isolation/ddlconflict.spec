conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"

teardown
{
    SET bdr.permit_ddl_locking = true;
	DROP TABLE IF EXISTS bdr_ddl_conflict_a, bdr_ddl_conflict_b, bdr_ddl_conflict_c;
}

session "snode1"
step "s1b" { BEGIN; SET LOCAL bdr.permit_ddl_locking = true; }
step "s1ct" { CREATE TABLE bdr_ddl_conflict_a(f1 int); }
step "s1c" { COMMIT; }
step "s1wait" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; }

session "snode2"
step "s2a" { SET bdr.permit_ddl_locking = true; }
step "s2b" { BEGIN; }
step "s2ct" { CREATE TABLE bdr_ddl_conflict_b(f1 int); }
step "s2c" { COMMIT; }
step "s2dt" { DROP TABLE bdr_ddl_conflict_a; }
step "s2wait" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; select * from pg_sleep(1); }
step "s2ct2" { CREATE TABLE bdr_ddl_conflict_c(f1 int); }

permutation "s1b" "s1ct" "s2a" "s2b" "s2ct" "s1c" "s2c" "s2dt" "s1wait" "s2wait" "s2ct2"
