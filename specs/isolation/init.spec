conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"
conninfo "node3" "dbname=node3"

session "snode1"

# pg_xlog_wait_remote_apply isn't good enough alone as it doesn't permit us to
# say how many nodes must be present.  It'll succeed if there are zero nodes.
# So we first have to wait for enough replication connections.
#
# The reason why we call pg_stat_clear_snapshot() is that pg_stat_activity is
# cached when first accessed so repeat access within the same transaction sees
# unchanging results. As pg_stat_replication joins pg_stat_get_wal_senders() on
# pg_stat_activity, new walsenders are filtered out by the join unles we force
# a refresh of pg_stat_activity.

connection "node1"

step "setup1"
{
	CREATE EXTENSION btree_gist;
	CREATE EXTENSION bdr;
}


step "connect1_2"
{
	SELECT bdr.bdr_connection_add(
		conn_name := 'node2',
		dsn := 'dbname=node2'
		);
}

step "connect1_3"
{
	SELECT bdr.bdr_connection_add(
		conn_name := 'node3',
		dsn := 'dbname=node3'
		);
}

step "wait"
{
	-- pg_xlog_wait_remote_apply isn't good enough alone
	-- as it doesn't permit us to say how many nodes must be present.
	-- It'll succeed if there are zero nodes. So we first have to wait
	-- for enough replication connections.
	DO $$
	DECLARE
		nodecount integer := 0;
		target_lsn pg_lsn;
	BEGIN
		WHILE nodecount <> 6
		LOOP
			PERFORM pg_sleep(1);
			PERFORM pg_stat_clear_snapshot();
			-- Now find out how many walsenders are running
			nodecount := (SELECT count(*)
						  FROM pg_catalog.pg_stat_replication);
			RAISE NOTICE 'Found % nodes',nodecount;
		END LOOP;
		-- OK, all nodes seen, now we wait for catchup on them all.
		target_lsn := pg_current_xlog_location();
		RAISE NOTICE 'Found expected % nodes, waiting for xlog catchup to %', 6, target_lsn;
		PERFORM pg_xlog_wait_remote_apply( target_lsn, 0 );
		RAISE NOTICE 'Catchup to LSN completed';
	END;
	$$;
}

session "snode2"
connection "node2"

step "setup2"
{
	CREATE EXTENSION btree_gist;
	CREATE EXTENSION bdr;
}


step "connect2_1"
{
	SELECT bdr.bdr_connection_add(
		conn_name := 'node1',
		dsn := 'dbname=node1',
		init_replica := true,
		replica_local_dsn := 'dbname=node2'
		);
}

step "connect2_3"
{
	SELECT bdr.bdr_connection_add(
		conn_name := 'node3',
		dsn := 'dbname=node3'
		);
}

session "snode3"
connection "node3"

step "setup3"
{
	CREATE EXTENSION btree_gist;
	CREATE EXTENSION bdr;
}


step "connect3_1"
{
	SELECT bdr.bdr_connection_add(
		conn_name := 'node1',
		dsn := 'dbname=node1',
		init_replica := true,
		replica_local_dsn := 'dbname=node3'
		);
}

step "connect3_2"
{
	SELECT bdr.bdr_connection_add(
		conn_name := 'node2',
		dsn := 'dbname=node2'
		);
}

permutation "setup1" "setup2" "setup3" "connect1_2" "connect1_3" "connect2_1" "connect3_1" "connect2_3" "connect3_2" "wait"

#permutation "setup1" "connect1_2" "connect1_3" "setup2" "connect2_1" "connect2_3" "setup3" "connect3_1" "connect3_2" "wait"
