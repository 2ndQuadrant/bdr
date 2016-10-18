\c postgres

-- A bunch of repeatable sub-tests run by other suites.
-- This isn't executed directly by pg_regress as a top-level
-- test so it doesn't need an expected file etc.
--
-- ***Starting conflict_cases.sql run***

-- Cleanup
TRUNCATE TABLE city;
TRUNCATE TABLE bdr_log;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
TRUNCATE TABLE bdr.bdr_conflict_history;
\c regression
TRUNCATE TABLE bdr.bdr_conflict_history;
\c postgres

---------------------------------------------------
-- Simple insert/insert conflict         
---------------------------------------------------
INSERT INTO city(city_sid, name) VALUES (1, 'Parabadoo');
\c regression
INSERT INTO city(city_sid, name) VALUES (1, 'Newman');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE city_sid = 1;
\c postgres
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM city WHERE city_sid = 1;
SELECT * FROM bdr_log;

-- Replication still works
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE sydney(dummy integer);
ROLLBACK;

---------------------------------------------------
-- Secondary unique index insert/insert conflict
---------------------------------------------------
INSERT INTO city(city_sid, name) VALUES (2, 'Tom Price');
\c regression
INSERT INTO city(city_sid, name) VALUES (3, 'Tom Price');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE name = 'Tom Price';
\c postgres
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE name = 'Tom Price';

-- We detected the conflict on the secondary index and handled it.

-- Replication still works
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE sydney(dummy integer);
ROLLBACK;

---------------------------------------------------
-- Two unique violations insert/insert conflict
---------------------------------------------------

-- TODO

---------------------------------------------------
-- Partial secondary unique index insert/insert conflict
---------------------------------------------------

-- TODO


---------------------------------------------------
-- Expression secondary unique index insert/insert conflict
---------------------------------------------------

-- TODO

---------------------------------------------------
-- Update/Update conflict on normal data change
---------------------------------------------------
INSERT INTO city(city_sid, name) VALUES (10, 'Carnarvon');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
UPDATE city SET name = 'Dwellingup' WHERE city_sid = 10;
\c regression
UPDATE city SET name = 'Balingup' WHERE city_sid = 10;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE city_sid IN (10);
\c postgres
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE city_sid IN (10);

-- No point testing for broken replication here, it can't break anything.


---------------------------------------------------
-- Update/Update conflict on PRIMARY KEY change
---------------------------------------------------
INSERT INTO city(city_sid, name) VALUES (4, 'Exmouth'), (5, 'Coral Bay');
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
UPDATE city SET city_sid = 6 WHERE city_sid = 4;
\c regression
UPDATE city SET city_sid = 6 WHERE city_sid = 5;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE city_sid IN (4,5,6);
\c postgres
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE city_sid IN (4,5,6);

-- Replication broke, oops!
--
-- The logs will report something like:
--
-- ERROR:  duplicate key value violates unique constraint "city_pk"
-- DETAIL:  Key (city_sid)=(6) already exists.
-- CONTEXT:  apply UPDATE from remote relation public.city in commit 0/1B03740, xid 2052 commited at 2016-10-18 06:45:24.693261+00 (action #2) from node (6342692214835298407,1,13104)
--
-- on one or both nodes. Confirm by taking DDL lock:
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE sydney(dummy integer);
ROLLBACK;

-- We can fix this without discarding transactions from the queue by resolving
-- the conflict on one or both sides to allow replication to complete. Simple
-- pg_regress tests can't do things like look at the logs so assume we conflict
-- on both sides.
DELETE FROM city WHERE city_sid = 6;
\c regression
DELETE FROM city WHERE city_sid = 6;
\c postgres

-- When we get the DDL lock we know we've recovered
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE sydney(dummy integer);
ROLLBACK;

-- FIXME!

---------------------------------------------------
-- Update/Update conflict on secondary unique index
---------------------------------------------------

-- But what about conflicting UPDATEs that cause violation of a secondary
-- unique index?
UPDATE city SET name = 'Kalgoorlie' WHERE city_sid = 1;
\c regression
UPDATE city SET name = 'Kalgoorlie' WHERE city_sid = 3;
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM bdr_log;
SELECT * FROM city WHERE city_sid IN (1,3);
\c postgres
SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);
SELECT object_schema, object_name, conflict_type, conflict_resolution, local_tuple, remote_tuple FROM bdr.bdr_conflict_history;
SELECT * FROM city WHERE city_sid IN (1,3);
SELECT * FROM bdr_log;

-- ... that seemed to work, after all, pg_xlog_wait_remote_apply returned. But
-- it only returned because there are no walsenders anymore, we'lll be in a crash
-- cycle on one or both nodes and nothing will be replicating.
--
-- Lets try to take the DDL lock, we'll get stuck here.
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE sydney(dummy integer);
ROLLBACK;

-- Output on one or both nodes will be something like:
--
-- ERROR:  duplicate key value violates unique constraint "city_un"
-- DETAIL:  Key (name)=(Kalgoorlie) already exists.
-- CONTEXT:  apply UPDATE from remote relation public.city in commit 0/1B08AA8, xid 2076 commited at 2016-10-18 06:54:14.36732+00 (action #2) from node (6342694436480988016,1 ,16385)

-- To fix it, same deal as before, remove a conflicting row.
DELETE FROM city WHERE name = 'Kalgoorlie';
\c regression
DELETE FROM city WHERE name = 'Kalgoorlie';
\c postgres
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE TABLE sydney(dummy integer);
ROLLBACK;

-- FIXME!


---------------------------------------------------
-- Two unique violations update/update conflict
---------------------------------------------------

-- TODO

---------------------------------------------------
-- Update/Update conflict on secondary unique expression index
---------------------------------------------------

-- TODO

---------------------------------------------------
-- Update/Update conflict on secondary partial unique index
---------------------------------------------------

-- TODO


---------------------------------------------------
-- Insert/Update conflicts
---------------------------------------------------

-- TODO
--
-- Note that insert/update vs update/delete is ambiguous
-- (might need 3 nodes)

---------------------------------------------------
-- Update/Delete conflicts
---------------------------------------------------

-- TODO
--
-- Note that insert/update vs update/delete is ambiguous
-- (might need 3 nodes)

---------------------------------------------------
-- Delete/Delete "conflicts"
---------------------------------------------------

-- TODO

---------------------------------------------------
-- Insert/Delete conflicts
---------------------------------------------------

-- TODO
-- This probably needs 3 nodes though...

-- *** Ending conflict_cases.sql run ***
