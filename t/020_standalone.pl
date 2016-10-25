use strict;
use warnings;
use Cwd;
use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 13;

my $tempdir = TestLib::tempdir;

my $node_a = get_new_node('node_a');

$node_a->init();
$node_a->append_conf('postgresql.conf', q{
wal_level = logical
track_commit_timestamp = on
shared_preload_libraries = 'bdr'
});
$node_a->start;

my $dbname = 'bdr_test';
$node_a->safe_psql('postgres', qq{CREATE DATABASE $dbname;});
$node_a->safe_psql($dbname, q{CREATE EXTENSION btree_gist;});
$node_a->safe_psql($dbname, q{CREATE EXTENSION bdr;});

is($node_a->safe_psql($dbname, 'SELECT bdr.bdr_is_active_in_db()'), 'f',
	'BDR is not active on node_a after create extension');

# Bring up a single BDR node, stand-alone
$node_a->safe_psql($dbname, qq{
SELECT bdr.bdr_group_create(
	local_node_name := 'node-a',
	node_external_dsn := '@{[ $node_a->connstr($dbname) ]}'
	);
});

is($node_a->safe_psql($dbname, 'SELECT bdr.bdr_is_active_in_db()'), 't',
	'BDR is active on node_a after group create');

ok(!$node_a->safe_psql($dbname, q{
SELECT bdr.bdr_replicate_ddl_command($DDL$
CREATE TABLE public.reptest(
	id integer primary key,
	dummy text
);
$DDL$);
}), 'simple DDL succeeds');

ok(!$node_a->psql($dbname, "INSERT INTO reptest (id, dummy) VALUES (1, '42')"), 'simple DML succeeds');

is($node_a->safe_psql($dbname, 'SELECT dummy FROM reptest WHERE id = 1'), '42', 'simple DDL and insert worked');

is($node_a->safe_psql($dbname, "SELECT node_status FROM bdr.bdr_nodes WHERE node_name = bdr.bdr_get_local_node_name()"), 'r', 'node status is "r"');

ok(!$node_a->psql($dbname, "SELECT bdr.bdr_part_by_node_names(ARRAY['node-a'])"), 'parted without error');

is($node_a->safe_psql($dbname, "SELECT node_status FROM bdr.bdr_nodes WHERE node_name = bdr.bdr_get_local_node_name()"), 'k', 'node status is "k"');

ok($node_a->psql($dbname, "DROP EXTENSION bdr"), 'DROP EXTENSION fails after part');

is($node_a->safe_psql($dbname, 'SELECT bdr.bdr_is_active_in_db();'), 't', 'still active after part');

ok(!$node_a->psql($dbname, 'SELECT bdr.remove_bdr_from_local_node(true, true);'), 'remove_bdr_from_local_node succeeds');

is($node_a->safe_psql($dbname, 'SELECT bdr.bdr_is_active_in_db();'), 'f', 'not active after remove');

ok(!$node_a->psql($dbname, 'DROP EXTENSION bdr;'), 'extension dropped');

$node_a->stop('fast');
