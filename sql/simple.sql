SELECT * FROM bdr_regress_variables()
\gset

\c :node1_dsn

SELECT pglogical.replicate_ddl_command($DDL$
CREATE TABLE public.demo(
    id integer primary key
);
$DDL$);

SELECT pglogical.replication_set_add_table('dummy_nodegroup', 'demo');

SELECT * FROM pglogical.tables;

INSERT INTO demo(id) VALUES (42);

\c :node2_dsn

SELECT pglogical_wait_slot_confirm_lsn(NULL, NULL);

SELECT pglogical.replication_set_add_table('dummy_nodegroup', 'demo');

SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'demo';

SELECT * FROM pglogical.tables;

SELECT id FROM demo ORDER BY id;

INSERT INTO demo(id) VALUES (43);

\c :node1_dsn

SELECT pglogical_wait_slot_confirm_lsn(NULL, NULL);

SELECT id FROM demo ORDER BY id;
