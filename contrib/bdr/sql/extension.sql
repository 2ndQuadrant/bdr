-- check extension creation works
CREATE EXTENSION bdr;
SELECT * FROM pg_stat_bdr;

-- check that only superusers can do this
CREATE ROLE no_special_perms;
SET ROLE no_special_perms;
SELECT * FROM pg_stat_bdr;
SELECT * FROM pg_stat_bdr();

-- reacquire permissions, drop extension, role
RESET role;
DROP EXTENSION bdr;
DROP ROLE no_special_perms;
