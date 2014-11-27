--
-- CREATE_SCHEMA
--

-- Temporary role to test AUTHORIZATION clause
CREATE ROLE schema_role;

CREATE SCHEMA foo;

CREATE SCHEMA IF NOT EXISTS bar;

CREATE SCHEMA baz AUTHORIZATION schema_role;

-- Will not be created, and will not be handled by the
-- event trigger
CREATE SCHEMA IF NOT EXISTS baz AUTHORIZATION schema_role;

CREATE SCHEMA IF NOT EXISTS zzz AUTHORIZATION schema_role;

CREATE SCHEMA element_test
  CREATE TABLE foo (id INT)
  CREATE VIEW bar AS SELECT * FROM foo;

