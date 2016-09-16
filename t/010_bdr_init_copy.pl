use strict;
use warnings;
use Cwd;
use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

my $node_b_port = 19995;

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
	[ 'bdr_init_copy', '-D', "$tempdir/backup", "-n", "newnode", '-d', $node_a->connstr('postgres'), '--local-dbname', 'postgres', '--local-port', $node_b_port],
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

# The postgresql.conf copied by bdr_init_copy's pg_basebackup invocation will
# use the same port as node_a . We can't have that, so template a new config file.
open(my $conf_a, "<", $node_a->data_dir . '/postgresql.conf')
	or die ("can't open node_a conf file for reading: $!");

open(my $conf_b, ">", "$tempdir/postgresql.conf.b")
	or die ("can't open node_b conf file for writing: $!");

while (<$conf_a>)
{
	if ($_ =~ "^port")
	{
		print $conf_b "port = " . $node_b_port . "\n";
	}
	else
	{
		print $conf_b $_;
	}
}
close($conf_a) or die ("failed to close old postgresql.conf: $!");
close($conf_b) or die ("failed to close new postgresql.conf: $!");


command_ok(
	[ 'bdr_init_copy', '-v', '-D', "$tempdir/backup", "-n", "newnode", '-d', $node_a->connstr('postgres'), '--local-dbname', 'postgres', '--local-port', $node_b_port, '--postgresql-conf', "$tempdir/postgresql.conf.b"],
	'bdr_init_copy succeeds');
