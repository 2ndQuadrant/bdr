\c postgres
SELECT bdr.bdr_group_create(
	local_node_name := 'node-pg',
	node_external_dsn := 'dbname=postgres',
	replication_sets := ARRAY['default', 'important', 'for-node-1']
	);

SELECT bdr.bdr_node_join_wait_for_ready();

\c regression
SELECT bdr.bdr_group_join(
	local_node_name := 'node-regression',
	node_external_dsn := 'dbname=regression',
	join_using_dsn := 'dbname=postgres',
	node_local_dsn := 'dbname=regression',
	replication_sets := ARRAY['default', 'important', 'for-node-2', 'for-node-2-insert', 'for-node-2-update', 'for-node-2-delete']
	);

SELECT bdr.bdr_node_join_wait_for_ready();

-- Make sure we see two slots and two active connections
SELECT plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

\c postgres
SELECT conn_dsn, conn_replication_sets FROM bdr.bdr_connections ORDER BY conn_dsn;
SELECT node_status, node_local_dsn, node_init_from_dsn FROM bdr.bdr_nodes ORDER BY node_local_dsn;

\c regression
SELECT conn_dsn, conn_replication_sets FROM bdr.bdr_connections ORDER BY conn_dsn;
SELECT node_status, node_local_dsn, node_init_from_dsn FROM bdr.bdr_nodes ORDER BY node_local_dsn;

SELECT bdr.bdr_replicate_ddl_command($DDL$
CREATE OR REPLACE FUNCTION public.bdr_regress_variables(
    OUT readdb1 text,
    OUT readdb2 text,
    OUT writedb1 text,
    OUT writedb2 text
    ) RETURNS record LANGUAGE SQL AS $f$
SELECT
    current_setting('bdrtest.readdb1'),
    current_setting('bdrtest.readdb2'),
    current_setting('bdrtest.writedb1'),
    current_setting('bdrtest.writedb2')
$f$;
$DDL$);
