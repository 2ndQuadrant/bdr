CREATE SEQUENCE bdr_test_seq USING bdr;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

DO $$
BEGIN
	LOOP
		IF (SELECT amdata IS NOT NULL FROM bdr_test_seq) THEN
			EXIT;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;$$;

CREATE TEMP TABLE bdr_test_seq_val AS SELECT nextval('bdr_test_seq') as last_val;

SELECT bdr.bdr_internal_sequence_reset_cache('bdr_test_seq'::regclass);
SELECT amdata IS NULL FROM bdr_test_seq;

DO $$
BEGIN
	LOOP
		IF (SELECT amdata IS NOT NULL FROM bdr_test_seq) THEN
			EXIT;
		END IF;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;$$;

-- 10000 is maximum cache size in amdata
SELECT nextval('bdr_test_seq') >= last_val + 10000 FROM bdr_test_seq_val;
