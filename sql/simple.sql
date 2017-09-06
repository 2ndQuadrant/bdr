SELECT * FROM bdr_regress_variables()
\gset

\c :node1_dsn

SELECT pglogical.replicate_ddl_command($DDL$
CREATE TABLE public.demo(
    id integer primary key
);
$DDL$, 'bdrgroup');

-- TODO: This should happen automatically with BDR, adding tables to
-- the default repset on creation.
SELECT bdr.replication_set_add_table('demo');

SELECT nspname, relname FROM pglogical.tables WHERE set_name = 'bdrgroup';

INSERT INTO demo(id) VALUES (42);

\c :node2_dsn

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

-- TODO: This should happen automatically with BDR
SELECT bdr.replication_set_add_table('demo');

SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'demo';

SELECT nspname, relname FROM pglogical.tables WHERE set_name = 'bdrgroup';

SELECT id FROM demo ORDER BY id;

INSERT INTO demo(id) VALUES (43);

\c :node1_dsn

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

SELECT id FROM demo ORDER BY id;
