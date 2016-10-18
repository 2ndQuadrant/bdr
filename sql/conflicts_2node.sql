\c postgres

--
-- Tests for conflict handling and resolution
--
-- Here we use a common trick of ensuring replication is still working by
-- taking the DDL lock using a DDL statement we then roll back, in case you're
-- wondering why those are there...
--

-- Test Table. Note the presence of the secondary UNIQUE constraint,
-- which we'll be using when testing conflict handling.
CREATE TABLE public.city
( city_sid INTEGER NOT NULL,
  name  VARCHAR,
  CONSTRAINT city_pk PRIMARY KEY (city_sid),
  CONSTRAINT city_un UNIQUE (name)
);

\d+ public.city

ALTER TABLE public.city REPLICA IDENTITY DEFAULT;

CREATE SEQUENCE log_seq;

CREATE TABLE public.bdr_log
(
  -- Usually we'd store the whole BDR node ID with something
  -- like assignment of a (text,oid,oid) composite type from
  -- a DEFAULT of ROW((bdr.bdr_get_local_nodeid()).*), but in
  -- this case we know the nodes are on the same instance in
  -- different DBs so we can take a shortcut. Doing so gives us
  -- a stable nodeid that won't upset pg_regress's diff output.
  nodedb text not null default current_database(),
  nodeseq INTEGER NOT NULL default nextval('log_seq'),
  log_info1  VARCHAR,
  log_info2 VARCHAR,
  CONSTRAINT bdr_log_pk PRIMARY KEY (nodeid, nodeseq)
);

-- Now, for this test we need our apply workers to use a replication delay.

ALTER SYSTEM SET bdr.default_apply_delay = 1000;
ALTER SYSTEM SET bdr.log_conflicts_to_table = on;
ALTER SYSTEM SET bdr.conflict_logging_include_tuples = on;

SELECT pg_reload_conf();

SELECT pg_sleep(1);


-- Run the suite with default replica identity and no user conflict handler.
\i sql/conflict_cases.sql

-- Rerun with a user conflict handler:

CREATE OR REPLACE FUNCTION public.city_insert_conflict_resolution(
    IN  rec1 public.city,
    IN  rec2 public.city,
    IN  conflict_info text,
    IN  table_name regclass,
    IN  conflict_type bdr.bdr_conflict_type, 
    OUT rec_resolution public.city,
    OUT conflict_action bdr.bdr_conflict_handler_action)
 RETURNS record AS
$BODY$
BEGIN
  INSERT INTO public.bdr_log(log_info1, log_info2) VALUES (rec1::text, rec2::text);
  rec_resolution := rec1;
  conflict_action := 'ROW';
END
$BODY$
 LANGUAGE plpgsql VOLATILE;

SELECT bdr.bdr_create_conflict_handler(
  ch_rel := 'public.city',
  ch_name := 'city_insert_csh',
  ch_proc := 'public.city_insert_conflict_resolution(public.city, public.city, text, regclass, bdr.bdr_conflict_type)',
  ch_type := 'insert_insert'
);

\i sql/conflict_cases.sql


-- Now with replica identity set to a non-primary-key unique index...
--ALTER TABLE public.city REPLICA IDENTITY USING INDEX city_un;
-- TODO


-- Tidy up
ALTER SYSTEM
  RESET bdr.default_apply_delay;

SELECT pg_reload_conf();

-- We need to drop the conflict handler function because it depends on the type
-- bdr.bdr_conflict_handler_action which will be prevent DROP of the BDR
-- extension during the part_bdr tests.
--
-- ... but doing so gets stuck on a lock.
DROP FUNCTION city_insert_conflict_resolution(city,city,text,regclass,bdr.bdr_conflict_type);
