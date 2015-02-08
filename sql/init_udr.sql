\c postgres
SELECT bdr.bdr_subscribe(
	local_node_name := 'postgres',
	remote_dsn := 'dbname=regression',
	local_dsn := 'dbname=postgres',
	replication_sets := ARRAY['default', 'important', 'for-node-2', 'for-node-2-insert', 'for-node-2-update', 'for-node-2-delete']
	);

SELECT bdr.bdr_node_join_wait_for_ready();

-- Make sure we see the slot and active connection
SELECT plugin, slot_type, database, active FROM pg_replication_slots;
SELECT count(*) FROM pg_stat_replication;

SELECT conn_dsn, conn_replication_sets FROM bdr.bdr_connections;
SELECT node_status, node_local_dsn, node_init_from_dsn FROM bdr.bdr_nodes;

\c regression
SELECT conn_dsn, conn_replication_sets FROM bdr.bdr_connections;
SELECT node_status, node_local_dsn, node_init_from_dsn FROM bdr.bdr_nodes;

-- emulate the pg_xlog_wait_remote_apply on vanilla postgres
DO $DO$BEGIN
	PERFORM 1 FROM pg_proc WHERE proname = 'pg_xlog_wait_remote_apply';
	IF FOUND THEN
		RETURN;
	END IF;

	PERFORM bdr.bdr_replicate_ddl_command($DDL$
		CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
		AS $FUNC$
		BEGIN
			WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.flush_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
				PERFORM pg_sleep(0.01);
			END LOOP;
		END;$FUNC$ LANGUAGE plpgsql;
	$DDL$);
END;$DO$;

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
