\c regression

SELECT datname, node_seq_id
FROM bdr.bdr_nodes
INNER JOIN pg_database ON (node_dboid = pg_database.oid);

CREATE SEQUENCE dummy_seq;

-- Generate enough values to wrap around the sequence bits twice.
-- If a machine can generate 16k sequence values per second it could
-- wrap. Force materialization to a tuplestore so we don't slow down
-- generation.
--
-- We should get no duplicates.
WITH vals(val) AS (
   SELECT bdr.global_seq_nextval('dummy_seq'::regclass)
   FROM generate_series(1, (2 ^ 14)::bigint * 2)
   OFFSET 0
)
SELECT val, 'duplicate'
FROM vals
GROUP BY val
HAVING count(val) > 1
UNION ALL
SELECT count(distinct VAL), 'ndistinct'
FROM vals;

CREATE SEQUENCE dummy_seq2;

CREATE TABLE seqvalues (id bigint);

SELECT node_seq_id FROM bdr.bdr_nodes
WHERE (node_sysid, node_timeline, node_dboid) = bdr.bdr_get_local_nodeid();

-- Generate enough sequences to almost wrap by forcing
-- the same timestamp to be re-used.
INSERT INTO seqvalues(id)
SELECT bdr.global_seq_nextval_test('dummy_seq2'::regclass, '530605914245317'::bigint)
FROM generate_series(0, (2 ^ 14)::bigint - 2);

CREATE UNIQUE INDEX ON seqvalues(id);

-- This should wrap around and fail. Since we're always running on the same
-- node with the same nodeid, and starting at the same initial sequence value,
-- it'll do so at the same value too.
INSERT INTO seqvalues(id)
SELECT bdr.global_seq_nextval_test('dummy_seq2'::regclass, '530605914245317'::bigint);

-- So we'll see the same stop-point
SELECT last_value FROM dummy_seq2;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c postgres

-- We should be able to insert the same number of values on the other node
-- before wrapping, even if we're using the same timestamp-part.

SELECT node_seq_id FROM bdr.bdr_nodes
WHERE (node_sysid, node_timeline, node_dboid) = bdr.bdr_get_local_nodeid();

SELECT last_value FROM dummy_seq2;

INSERT INTO seqvalues(id)
SELECT bdr.global_seq_nextval_test('dummy_seq2'::regclass, '530605914245317'::bigint)
FROM generate_series(0, (2 ^ 14)::bigint - 2);

SELECT last_value FROM dummy_seq2;

SELECT count(id) FROM seqvalues;

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0);

\c regression

SELECT count(id) FROM seqvalues;
