use strict;
use warnings;
use Cwd;
use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 14;

my $tempdir = TestLib::tempdir;

my $node_a = get_new_node('node_a');

$node_a->init(hba_permit_replication => 1, allows_streaming => 1);
$node_a->append_conf('postgresql.conf', q{
wal_level = logical
track_commit_timestamp = on
shared_preload_libraries = 'bdr'
max_replication_slots = 4
});
$node_a->start;

my $node_b = get_new_node('node_b');

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
	[ 'bdr_init_copy', '-D', "$tempdir/backup", "-n", "newnode", '-d', $node_a->connstr('postgres'), '--local-dbname', 'postgres', '--local-port', $node_b->port],
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
		print $conf_b "port = " . $node_b->port . "\n";
	}
	else
	{
		print $conf_b $_;
	}
}
close($conf_a) or die ("failed to close old postgresql.conf: $!");
close($conf_b) or die ("failed to close new postgresql.conf: $!");


command_ok(
	[ 'bdr_init_copy', '-v', '-D', $node_b->data_dir, "-n", 'node_b', '-d', $node_a->connstr($dbname), '--local-dbname', $dbname, '--local-port', $node_b->port, '--postgresql-conf', "$tempdir/postgresql.conf.b"],
	'bdr_init_copy succeeds');

# ... but does replication actually work? Is this a live, working cluster?
my $bdr_version = $node_b->safe_psql($dbname, 'SELECT bdr.bdr_version()');
diag "BDR version $bdr_version";

is($node_a->safe_psql($dbname, 'SELECT bdr.bdr_is_active_in_db()'), 't',
	'BDR is active on node_a');
is($node_b->safe_psql($dbname, 'SELECT bdr.bdr_is_active_in_db()'), 't',
	'BDR is active on node_b');

my $status_a = $node_a->safe_psql($dbname, 'SELECT bdr.node_status_from_char(node_status) FROM bdr.bdr_nodes WHERE node_name = bdr.bdr_get_local_node_name()');
my $status_b = $node_b->safe_psql($dbname, 'SELECT bdr.node_status_from_char(node_status) FROM bdr.bdr_nodes WHERE node_name = bdr.bdr_get_local_node_name()');

is($status_a, 'BDR_NODE_STATUS_READY', 'first node in ready state');
is($status_b, 'BDR_NODE_STATUS_READY', 'second node in ready state');

diag "Taking ddl lock manually";

$node_a->safe_psql($dbname, "SELECT bdr.acquire_global_lock('write')");

diag "Creating a table...";

$node_b->safe_psql($dbname, q{
SELECT bdr.bdr_replicate_ddl_command($DDL$
CREATE TABLE public.reptest(
	id integer primary key,
	dummy text
);
$DDL$);
});

$node_a->poll_query_until($dbname, q{
SELECT EXISTS (
  SELECT 1 FROM pg_class c INNER JOIN pg_namespace n ON n.nspname = 'public' AND c.relname = 'reptest'
);
});

ok($node_b->safe_psql($dbname, "SELECT 'reptest'::regclass"), "reptest table creation replicated");

$node_a->safe_psql($dbname, "INSERT INTO reptest (id, dummy) VALUES (1, '42')");

$node_b->poll_query_until($dbname, q{
SELECT EXISTS (
  SELECT 1 FROM reptest
);
});

is($node_b->safe_psql($dbname, 'SELECT id, dummy FROM reptest;'), '1|42', "reptest insert successfully replicated");

my $seqid_a = $node_a->safe_psql($dbname, 'SELECT node_seq_id FROM bdr.bdr_nodes WHERE node_name = bdr.bdr_get_local_node_name()');
my $seqid_b = $node_b->safe_psql($dbname, 'SELECT node_seq_id FROM bdr.bdr_nodes WHERE node_name = bdr.bdr_get_local_node_name()');

is($seqid_a, 1, 'first node got global sequence ID 1');
is($seqid_b, 2, 'second node got global sequence ID 2');
