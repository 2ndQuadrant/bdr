--
-- Test for facilities of security label
--
CREATE EXTENSION dummy_seclabel;

-- initial setups
SET client_min_messages TO 'warning';

DROP ROLE IF EXISTS dummy_seclabel_user1;
DROP ROLE IF EXISTS dummy_seclabel_user2;

DROP TABLE IF EXISTS dummy_seclabel_tbl1;
DROP TABLE IF EXISTS dummy_seclabel_tbl2;
DROP TABLE IF EXISTS dummy_seclabel_tbl3;

CREATE USER dummy_seclabel_user1 WITH CREATEROLE;
CREATE USER dummy_seclabel_user2;

CREATE TABLE dummy_seclabel_tbl1 (a int, b text);
CREATE TABLE dummy_seclabel_tbl2 (x int, y text);
CREATE VIEW dummy_seclabel_view1 AS SELECT * FROM dummy_seclabel_tbl2;
CREATE FUNCTION dummy_seclabel_four() RETURNS integer AS $$SELECT 4$$ language sql;
CREATE DOMAIN dummy_seclabel_domain AS text;

ALTER TABLE dummy_seclabel_tbl1 OWNER TO dummy_seclabel_user1;
ALTER TABLE dummy_seclabel_tbl2 OWNER TO dummy_seclabel_user2;

RESET client_min_messages;

--
-- Test of SECURITY LABEL statement with a plugin
--
SET SESSION AUTHORIZATION dummy_seclabel_user1;

SECURITY LABEL ON TABLE dummy_seclabel_tbl1 IS 'classified';			-- OK
SECURITY LABEL ON COLUMN dummy_seclabel_tbl1.a IS 'unclassified';		-- OK
SECURITY LABEL ON COLUMN dummy_seclabel_tbl1 IS 'unclassified';	-- fail
SECURITY LABEL ON TABLE dummy_seclabel_tbl1 IS '...invalid label...';	-- fail
SECURITY LABEL FOR 'dummy' ON TABLE dummy_seclabel_tbl1 IS 'unclassified';	-- OK
SECURITY LABEL FOR 'unknown_seclabel' ON TABLE dummy_seclabel_tbl1 IS 'classified';	-- fail
SECURITY LABEL ON TABLE dummy_seclabel_tbl2 IS 'unclassified';	-- fail (not owner)
SECURITY LABEL ON TABLE dummy_seclabel_tbl1 IS 'secret';		-- fail (not superuser)
SECURITY LABEL ON TABLE dummy_seclabel_tbl3 IS 'unclassified';	-- fail (not found)

SET SESSION AUTHORIZATION dummy_seclabel_user2;
SECURITY LABEL ON TABLE dummy_seclabel_tbl1 IS 'unclassified';		-- fail
SECURITY LABEL ON TABLE dummy_seclabel_tbl2 IS 'classified';			-- OK

--
-- Test for shared database object
--
SET SESSION AUTHORIZATION dummy_seclabel_user1;

SECURITY LABEL ON ROLE dummy_seclabel_user1 IS 'classified';			-- OK
SECURITY LABEL ON ROLE dummy_seclabel_user1 IS '...invalid label...';	-- fail
SECURITY LABEL FOR 'dummy' ON ROLE dummy_seclabel_user2 IS 'unclassified';	-- OK
SECURITY LABEL FOR 'unknown_seclabel' ON ROLE dummy_seclabel_user1 IS 'unclassified';	-- fail
SECURITY LABEL ON ROLE dummy_seclabel_user1 IS 'secret';	-- fail (not superuser)
SECURITY LABEL ON ROLE dummy_seclabel_user3 IS 'unclassified';	-- fail (not found)

SET SESSION AUTHORIZATION dummy_seclabel_user2;
SECURITY LABEL ON ROLE dummy_seclabel_user2 IS 'unclassified';	-- fail (not privileged)

RESET SESSION AUTHORIZATION;

--
-- Test for various types of object
--
RESET SESSION AUTHORIZATION;

SECURITY LABEL ON TABLE dummy_seclabel_tbl1 IS 'top secret';			-- OK
SECURITY LABEL ON VIEW dummy_seclabel_view1 IS 'classified';			-- OK
SECURITY LABEL ON FUNCTION dummy_seclabel_four() IS 'classified';		-- OK
SECURITY LABEL ON DOMAIN dummy_seclabel_domain IS 'classified';		-- OK
CREATE SCHEMA dummy_seclabel_test;
SECURITY LABEL ON SCHEMA dummy_seclabel_test IS 'unclassified';		-- OK

SELECT objtype, objname, provider, label FROM pg_seclabels
	ORDER BY objtype, objname;
