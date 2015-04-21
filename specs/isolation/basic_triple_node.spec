conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"
conninfo "node3" "dbname=node3"

setup
{
 SET bdr.permit_ddl_locking = true;
 CREATE TABLE tsta (a INTEGER PRIMARY KEY, b TEXT[]);
 CREATE TABLE tstb (a INTEGER PRIMARY KEY, b tstzrange);
 CREATE TABLE tstc (a INTEGER PRIMARY KEY, b jsonb);
}

teardown
{
 SET bdr.permit_ddl_locking = true;
 DROP TABLE tsta;
 DROP TABLE tstb;
 DROP TABLE tstc;
}

session "s1"
connection "node1"
step "n1setup" { SET bdr.permit_ddl_locking = true; }
step "n1sync" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; }
step "n1reada" { SELECT a, b FROM tsta ORDER BY a; }
step "n1readb" { SELECT a, b FROM tstb ORDER BY a; }
step "n1readc" { SELECT a, b FROM tstc ORDER BY a; }
step "n1s1"
{
 INSERT INTO tsta (a, b) VALUES
  (1, ARRAY['aaa', 'bba', 'cca']),
  (2, ARRAY['aab', 'bbb', 'ccb']),
  (3, ARRAY['aac', 'bbc', 'ccc']);
 INSERT INTO tstb (a, b) VALUES
  (1, '[2001-01-01 01:01:01, 2001-02-03 02:02:02]'),
  (2, '[2002-02-02 02:02:02, 2002-03-04 02:02:02]'),
  (3, '[2003-03-03 03:03:03, 2003-04-05 03:03:03]');
 INSERT INTO tstc (a, b) VALUES
  (1, '{"t1": "aaa", "t2": "bba", "t3": "cca"}'::jsonb),
  (2, '{"t1": "aab", "t2": "bbb", "t3": "ccb"}'::jsonb),
  (3, '{"t1": "aac", "t2": "bbc", "t3": "ccc"}'::jsonb);
}
step "n1s2"
{
 UPDATE tstc set b = '{"t1": "aac", "t2": "bbc", "t3": "ccc"}'::jsonb;
}

session "s2"
connection "node2"
step "n2setup" { SET bdr.permit_ddl_locking = true; }
step "n2sync" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; }
step "n2reada" { SELECT a, b FROM tsta ORDER BY a; }
step "n2readb" { SELECT a, b FROM tstb ORDER BY a; }
step "n2readc" { SELECT a, b FROM tstc ORDER BY a; }
step "n2s1" {
 UPDATE tsta SET b = b || ('dd' || (ARRAY['a', 'b', 'c']::text[])[a]);
}
step "n2s2" {
 DELETE FROM tsta WHERE a = 3;
 DELETE FROM tstb WHERE b = '[2525-01-01 23:55:00, 2525-01-02 00:05:00]';
}

session "s3"
connection "node3"
step "n3sync" { SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication; }
step "n3reada" { SELECT a, b FROM tsta ORDER BY a; }
step "n3readb" { SELECT a, b FROM tstb ORDER BY a; }
step "n3readc" { SELECT a, b FROM tstc ORDER BY a; }
step "n3s1" {
 UPDATE tstb SET b = '[2525-01-01 23:55:00, 2525-01-02 00:05:00]' WHERE a = 3;
}
step "n3s2" {
 INSERT INTO tstb (a, b) VALUES
  (3, '[2003-03-03 03:03:03, 2003-04-05 03:03:03]');
}

permutation "n1setup" "n2setup" "n1s1" "n1sync" "n2reada" "n2readb" "n2readc" "n2s1" "n2sync" "n3reada" "n3s1" "n3sync" "n1readb" "n1s2" "n1sync" "n2readc" "n2s2" "n2sync" "n3reada" "n3readb" "n3s2" "n3sync" "n1readb"
