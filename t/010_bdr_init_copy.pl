use strict;
use warnings;
use Cwd;
use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

my $tempdir = TestLib::tempdir;

my $node_a = get_new_node('main');

$node_a->init(hba_permit_replication => 1, allows_streaming => 1);
$node_a->append_conf('postgresql.conf', q{
wal_level = logical
track_commit_timestamp = on
shared_preload_libraries = 'bdr'
max_replication_slots = 4
});
$node_a->start;

command_fails(['bdr_init_copy'],
	'bdr_init_copy needs target directory specified');
command_fails(
	[ 'bdr_init_copy', '-D', "$tempdir/backup" ],
	'bdr_init_copy fails because of missing node name');
command_fails(
	[ 'bdr_init_copy', '-D', "$tempdir/backup", "-n", "newnode"],
	'bdr_init_copy fails because of missing remote conninfo');
command_fails(
	[ 'bdr_init_copy', '-D', "$tempdir/backup", "-n", "newnode", '-d', $node_a->connstr('postgres')],
	'bdr_init_copy fails because of missing local conninfo');
command_fails(
	[ 'bdr_init_copy', '-D', "$tempdir/backup", "-n", "newnode", '-d', $node_a->connstr('postgres'), '--local-dbname', 'postgres', '--local-port', '9999'],
	'bdr_init_copy fails when there is no BDR database');

# Time to bring up BDR
my $dbname = 'bdr_test';
$node_a->safe_psql('postgres', qq{CREATE DATABASE $dbname;});
$node_a->safe_psql($dbname, q{CREATE EXTENSION btree_gist;});
$node_a->safe_psql($dbname, q{CREATE EXTENSION bdr;});
$node_a->safe_psql($dbname, qq{
SELECT bdr.bdr_group_create(
	local_node_name := 'node-a',
	node_external_dsn := '@{[ $node_a->connstr($dbname) ]}',
	replication_sets := ARRAY['default', 'important', 'for-node-1']
	);
});


command_ok(
	[ 'bdr_init_copy', '-D', "$tempdir/backup", "-n", "newnode", '-d', $node_a->connstr('postgres'), '--local-dbname', 'postgres', '--local-port', '9999'],
	'bdr_init_copy fails when there is no BDR database');
