SET bdr.permit_ddl_locking = false;
CREATE TABLE should_fail ( id integer );

SET bdr.permit_ddl_locking = true;
CREATE TABLE create_ok (id integer);

SET bdr.permit_ddl_locking = false;
ALTER TABLE create_ok ADD COLUMN alter_should_fail text;

SET bdr.permit_ddl_locking = true;
DROP TABLE create_ok;

-- Now for the rest of the DDL tests, presume they're allowed,
-- otherwise they'll get pointlessly verbose.
ALTER DATABASE regression SET bdr.permit_ddl_locking = true;
ALTER DATABASE postgres SET bdr.permit_ddl_locking = true;
