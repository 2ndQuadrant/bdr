conninfo "d1" "port=5433 dbname=postgres"
conninfo "d2" "port=5434 dbname=postgres"

session "s1"
connection "d1"
setup { CREATE TABLE x(a int, b text); CREATE FUNCTION wait_for(id int) RETURNS void AS $$ DECLARE tmp int; BEGIN LOOP SELECT a INTO tmp FROM x WHERE a=$1; IF found THEN RETURN; END IF; EXECUTE pg_sleep(1); END LOOP; END; $$ LANGUAGE 'plpgsql'; }
step "s1a" { INSERT INTO x(a,b) values (1,'foo'); }
step "s1b" { SELECT wait_for(2); }
step "s1c" { SELECT * from x order by a; }
teardown { DROP TABLE x; DROP FUNCTION wait_for(int); }

session "s2"
connection "d2"
setup { CREATE TABLE x(a int, b text); CREATE FUNCTION wait_for(id int) RETURNS void AS $$ DECLARE tmp int; BEGIN LOOP SELECT a INTO tmp FROM x WHERE a=$1; IF found THEN RETURN; END IF; EXECUTE pg_sleep(1); END LOOP; END; $$ LANGUAGE 'plpgsql'; }
step "s2a" { INSERT INTO x(a,b) values (2,'bar'); }
step "s2b" { SELECT wait_for(1); }
step "s2c" { SELECT * from x order by a; }
teardown { DROP TABLE x; DROP FUNCTION wait_for(int); }

permutation "s1a" "s2a" "s1b" "s2b" "s1c" "s2c"
