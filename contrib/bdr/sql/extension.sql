-- check extension creation works
CREATE EXTENSION btree_gist;
CREATE EXTENSION bdr;
SELECT * FROM bdr.pg_stat_bdr;

-- check that only superusers can do this
CREATE ROLE bdr_no_special_perms;
SET ROLE bdr_no_special_perms;
SELECT * FROM bdr.pg_stat_bdr;
SELECT * FROM bdr.pg_stat_get_bdr();

-- reacquire permissions, drop extension, role
RESET role;
DROP EXTENSION bdr;
DROP EXTENSION btree_gist;
DROP ROLE bdr_no_special_perms;
