-- No real way to test the sysid, so ignore it
SELECT timeline= 1, dboid = (SELECT oid FROM pg_database WHERE datname = current_database())
FROM bdr.bdr_get_local_nodeid();

SELECT current_database() = 'regression';

-- Test probing for remote node information
SELECT
	r.sysid = l.sysid,
	r.timeline = l.timeline,
	r.dboid = (SELECT oid FROM pg_database WHERE datname = 'postgres'),
	variant = bdr.bdr_variant(),
	version = bdr.bdr_version(),
	version_num = bdr.bdr_version_num(),
	min_remote_version_num = bdr.bdr_min_remote_version_num(),
	is_superuser = 't'
FROM bdr.bdr_get_remote_nodeinfo('dbname=postgres') r,
     bdr.bdr_get_local_nodeid() l;

-- bdr.bdr_get_remote_nodeinfo can also be used to probe the local dsn
-- and make sure it works.
SELECT
    r.dboid = (SELECT oid FROM pg_database WHERE datname = current_database())
FROM bdr.bdr_get_remote_nodeinfo('dbname='||current_database()) r;

-- Test probing for replication connection
SELECT
	r.sysid = l.sysid,
	r.timeline = l.timeline,
	r.dboid = (SELECT oid FROM pg_database WHERE datname = 'postgres')
FROM bdr.bdr_test_replication_connection('dbname=postgres') r,
     bdr.bdr_get_local_nodeid() l;

-- Probing replication connection for the local dsn will work too
-- even though the identifier is the same.
SELECT
	r.dboid = (SELECT oid FROM pg_database WHERE datname = current_database())
FROM bdr.bdr_test_replication_connection('dbname='||current_database()) r;
