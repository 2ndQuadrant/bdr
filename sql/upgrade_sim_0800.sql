--
-- Attempt to simulate an upgrade from BDR 0.8.0 to the current
-- version.
--
-- 0.8.0 used GUCs for bdr.connections DSN configuration, etc. We can manually
-- create the slots, replication identifiers, and bdr.bdr_nodes entries as if
-- this was a 0.8.0 DB just about to be upgraded, then upgrade the extension
-- and execute the upgrade process.
--


CREATE DATABASE upgrade_sim_0800_a;
CREATE DATABASE upgrade_sim_0800_b;

\c upgrade_sim_0800_a;
------------------------------------------
-- Prepare node upgrade_sim_0800_a      --
------------------------------------------

CREATE EXTENSION btree_gist;
CREATE EXTENSION bdr VERSION '0.8.0';

-- public.bdr_get_local_nodeid() is defined in the bdr ext's C lib
-- exposed in 0.8.0's SQL extension. We have to use it to create
-- the required slots etc, so create it in public.

CREATE FUNCTION public.bdr_get_local_nodeid( sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'bdr';

CREATE TABLE dummytable(
	id integer primary key,
	somevalue text
);

INSERT INTO dummytable(id, somevalue) VALUES (1, '42'), (2, 'fred');

SELECT pg_replication_identifier_create(
	format('bdr_%s_%s_%s_%s__%s',
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_a'),
		sysid, timeline,
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_b'),
		''
	)
)
FROM public.bdr_get_local_nodeid();

INSERT INTO bdr.bdr_nodes
(node_sysid, node_timeline, node_dboid, node_status)
SELECT
	sysid, timeline, (SELECT oid FROM pg_database WHERE datname = dn), 'r'
FROM (VALUES ('upgrade_sim_0800_a'), ('upgrade_sim_0800_b')) x(dn),
	 public.bdr_get_local_nodeid();

SELECT pg_create_logical_replication_slot(
	format('bdr_%s_%s_%s_%s__%s',
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_b'),
		sysid, timeline,
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_a'),
		''
	),
	'bdr')
FROM public.bdr_get_local_nodeid();

DROP FUNCTION public.bdr_get_local_nodeid();






\c upgrade_sim_0800_b;
------------------------------------------
-- Prepare node upgrade_sim_0800_b      --
------------------------------------------

CREATE EXTENSION btree_gist;
CREATE EXTENSION bdr VERSION '0.8.0';

CREATE FUNCTION public.bdr_get_local_nodeid( sysid OUT text, timeline OUT oid, dboid OUT oid)
RETURNS record LANGUAGE c AS 'bdr';

CREATE TABLE dummytable(
	id integer primary key,
	somevalue text
);

INSERT INTO dummytable(id, somevalue) VALUES (1, '42'), (2, 'fred');

SELECT pg_replication_identifier_create(
	format('bdr_%s_%s_%s_%s__%s',
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_b'),
		sysid, timeline,
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_a'),
		''
	)
)
FROM public.bdr_get_local_nodeid();

INSERT INTO bdr.bdr_nodes
(node_sysid, node_timeline, node_dboid, node_status)
SELECT
	sysid, timeline, (SELECT oid FROM pg_database WHERE datname = dn), 'r'
FROM (VALUES ('upgrade_sim_0800_a'), ('upgrade_sim_0800_b')) x(dn),
	 public.bdr_get_local_nodeid();

SELECT pg_create_logical_replication_slot(
	format('bdr_%s_%s_%s_%s__%s',
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_a'),
		sysid, timeline,
		(SELECT oid FROM pg_database WHERE datname = 'upgrade_sim_0800_b'),
		''
	),
	'bdr')
FROM public.bdr_get_local_nodeid();

DROP FUNCTION public.bdr_get_local_nodeid();





------------------------------------------
-- Test the upgrade                     --
------------------------------------------
--
-- We now have two databases that look like they were running BDR, with
-- contents in sync at the time of upgrade. The origin replication identifier
-- information is wrong as both have InvalidRepOriginId but we don't really care
-- about that. It's as if we deleted bdr.bdr_connections then started the DB
-- up.
--
-- Time to upgrade to dynconf. Hope this works!
--

-- First the extension must be updated on BOTH nodes
\c upgrade_sim_0800_a
ALTER EXTENSION bdr UPDATE;
\c upgrade_sim_0800_b
ALTER EXTENSION bdr UPDATE;


-- then one must be upgraded standalone. For this one we'll provide no local
-- dsn; it must be inferred from the node dsn in that case. There's also no
-- remote DSN since it's the first node.
\c upgrade_sim_0800_a
SELECT bdr.bdr_upgrade_to_090('dbname=upgrade_sim_0800_a', NULL, NULL);

SELECT node_timeline, datname, node_status, node_local_dsn, node_init_from_dsn
FROM bdr.bdr_nodes n INNER JOIN pg_database d ON (n.node_dboid = d.oid)
ORDER BY datname;

SELECT * FROM pg_catalog.pg_shseclabel
WHERE classoid = (SELECT oid FROM pg_class WHERE relname = 'pg_database')
  AND objoid = (SELECT oid FROM pg_database WHERE datname = current_database());


-- Upgrade the second node using the first node. This time we'll
-- supply a local dsn too, though it'll be the same.
\c upgrade_sim_0800_b

-- must have old nodes, no replication can have occurred
SELECT node_timeline, datname, node_status, node_local_dsn, node_init_from_dsn
FROM bdr.bdr_nodes n INNER JOIN pg_database d ON (n.node_dboid = d.oid)
ORDER BY datname;

SELECT bdr.bdr_upgrade_to_090('dbname=upgrade_sim_0800_b', 'dbname=upgrade_sim_0800_b', 'dbname=upgrade_sim_0800_a');

-- local node must be updated. Remote node could be either as replication
-- might or might not have sent it yet.
SELECT node_timeline, datname, node_status, node_local_dsn, node_init_from_dsn
FROM bdr.bdr_nodes n INNER JOIN pg_database d ON (n.node_dboid = d.oid)
WHERE datname = current_database()
ORDER BY datname;

SELECT * FROM pg_catalog.pg_shseclabel
WHERE classoid = (SELECT oid FROM pg_class WHERE relname = 'pg_database')
  AND objoid = (SELECT oid FROM pg_database WHERE datname = current_database());

-- TODO: wait for remote apply, switch back

-- TODO: use test table

-- TODO: lots of failure cases
