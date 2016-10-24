SELECT datname, node_seq_id
FROM bdr.bdr_nodes
INNER JOIN pg_database ON (node_dboid = pg_database.oid);

CREATE SEQUENCE dummy_seq;

SELECT bdr.global_seq_nextval('dummy_seq'::regclass)
FROM generate_series(1,1000);
