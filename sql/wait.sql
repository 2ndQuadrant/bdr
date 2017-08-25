SELECT * FROM bdr_regress_variables()
\gset

--
-- This dummy "test" is used while debugging messaging, since
-- we need the system to idle for a while after coming up.
--

\c :node1_dsn
SELECT bdr_submit_comment('XXX test1');

\c :node2_dsn
SELECT pg_sleep(1);
SELECT bdr_submit_comment('XXX test2');

select pg_sleep(30);
