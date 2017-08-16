-- Indirection for connection strings
--
-- This is temporary.
CREATE OR REPLACE FUNCTION public.bdr_regress_variables(
    OUT node1_dsn text,
    OUT node2_dsn text
    ) RETURNS record LANGUAGE SQL AS $f$
SELECT
    current_setting('bdr.node1_dsn'),
    current_setting('bdr.node2_dsn');
$f$;

SELECT * FROM bdr_regress_variables()
\gset

SELECT :'node1_dsn', :'node2_dsn';

\c :node1_dsn
SET client_min_messages = 'warning';
DROP USER IF EXISTS nonsuper;
DROP USER IF EXISTS super;

CREATE USER nonsuper WITH replication;
CREATE USER super SUPERUSER;

\c :node2_dsn
SET client_min_messages = 'warning';
DROP USER IF EXISTS nonsuper;
DROP USER IF EXISTS super;

CREATE USER nonsuper WITH replication;
CREATE USER super SUPERUSER;
