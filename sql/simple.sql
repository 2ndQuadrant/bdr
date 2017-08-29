SELECT * FROM bdr_regress_variables()
\gset

\c :node1_dsn

SELECT pglogical.replicate_ddl_command($DDL$
CREATE TABLE public.demo(
    id integer primary key
);
$DDL$);

-- TODO: This should happen automatically with BDR
SELECT pglogical.replication_set_add_table('bdrgroup', 'demo');

SELECT nspname, relname FROM pglogical.tables WHERE set_name = 'bdrgroup';

INSERT INTO demo(id) VALUES (42);

\c :node2_dsn

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

-- TODO: This should happen automatically with BDR
SELECT pglogical.replication_set_add_table('bdrgroup', 'demo');

SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'demo';

SELECT nspname, relname FROM pglogical.tables WHERE set_name = 'bdrgroup';

SELECT id FROM demo ORDER BY id;

INSERT INTO demo(id) VALUES (43);

\c :node1_dsn

SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);

SELECT id FROM demo ORDER BY id;
