CREATE SEQUENCE dummy_seq;

SELECT bdr.global_seq_nextval('dummy_seq'::regclass)
FROM generate_series(1,1000);
