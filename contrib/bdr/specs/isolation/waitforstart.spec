conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"
conninfo "node3" "dbname=node3"

setup
{
	-- This won't work until DDL rep has started properly so sleep5
	-- must run first.
	--
	-- pg_xlog_wait_remote_apply isn't good enough alone
	-- as it doesn't permit us to say how many nodes must be present.
	-- It'll succeed if there are zero nodes. So we first have to wait
	-- for enough replication connections.
	--
	CREATE OR REPLACE FUNCTION wait_for_nodes(n_nodes integer)
	RETURNS void 
	LANGUAGE plpgsql
	AS $$
	DECLARE
		nodecount integer := 0;
		target_lsn pg_lsn;
	BEGIN
		WHILE nodecount <> n_nodes
		LOOP
			PERFORM pg_sleep(1);
			nodecount := (SELECT count(*)
						  FROM pg_catalog.pg_stat_replication);
			RAISE NOTICE 'Found % nodes',nodecount;
		END LOOP;
		-- OK, all nodes seen, now we wait for catchup on them all.
		target_lsn := pg_current_xlog_location();
		RAISE NOTICE 'Found expected % nodes, waiting for xlog catchup to %', n_nodes, target_lsn;
		PERFORM pg_xlog_wait_remote_apply( target_lsn, 0 );
		RAISE NOTICE 'Catchup to LSN completed';
	END;
	$$;
}

session "snode1"
step "s1waitlsn" { SELECT wait_for_nodes(6); }

session "snode2"
step "s2waitlsn" { SELECT wait_for_nodes(6); }

session "snode3"
step "s3waitlsn" { SELECT wait_for_nodes(6); }

permutation "s1waitlsn" "s2waitlsn" "s3waitlsn"
