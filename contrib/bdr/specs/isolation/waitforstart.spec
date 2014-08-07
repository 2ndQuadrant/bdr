conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"
conninfo "node3" "dbname=node3"

setup
{
	SELECT pg_sleep(10);

	-- pg_xlog_wait_remote_apply isn't good enough alone
	-- as it doesn't permit us to say how many nodes must be present.
	-- It'll succeed if there are zero nodes. So we first have to wait
	-- for enough replication connections.
	--
	CREATE OR REPLACE FUNCTION wait_for_3_nodes()
	RETURNS void 
	LANGUAGE plpgsql
	AS $$
	DECLARE
		nodecount integer := 0;
	BEGIN
		WHILE nodecount <> 3
		LOOP
			PERFORM pg_sleep(1);
			nodecount := (SELECT count(*)
						  FROM pg_catalog.pg_stat_replication
						  WHERE application_name LIKE 'bdr (%): receive');
		END LOOP;
		-- OK, all nodes seen, now we wait for catchup on them all.
		PERFORM pg_xlog_wait_remote_apply( pg_current_xlog_location(), 0 );
	END;
	$$;
}

session "snode1"
step "s1waitlsn" { SELECT wait_for_3_nodes(); }

session "snode2"
step "s2waitlsn" { SELECT wait_for_3_nodes(); }

session "snode3"
step "s3waitlsn" { SELECT wait_for_3_nodes(); }

permutation "s1waitlsn" "s2waitlsn" "s3waitlsn"
