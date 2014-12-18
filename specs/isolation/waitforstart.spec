conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"
conninfo "node3" "dbname=node3"

session "snode1"
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
			-- pg_stat_activity is cached when first accessed so repeat access
			-- within the same transaction sees unchanging results. As pg_stat_replication
			-- joins pg_stat_get_wal_senders() on pg_stat_activity, new walsenders
			-- are filtered out by the join unles we force a refresh of pg_stat_activity.
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

permutation "wait"
