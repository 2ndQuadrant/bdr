#
# Minimal 2- and 3-node BDR join and part testing
#
# Keep this SIMPLE please, it's a basic smoke test. All the fancy
# repset stuff, HA, etc can be in other tests.
#

use strict;
use warnings;
use v5.10.0;
use Cwd;
use Config;
use TestLib;
use Test::More;
use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval);
use Scalar::Util qw(looks_like_number);
# From Pg
use TestLib;
# Local
use PostgresPGLNode qw(get_new_pgl_node);
use PostgresBdrNode qw(get_new_bdr_node);
use BdrDb;

my $dbname = 'bdrtest';
my $ts;

my @nodes = ();
foreach my $nodename ('node0', 'node1', 'node2') {
    my $node = get_new_bdr_node($nodename);
    $node->init;
    $node->start;
    $node->safe_psql('postgres', "CREATE DATABASE $dbname");
    push @nodes, $node;
}

my @dbs = ();
foreach my $node (@nodes)
{
    my $db = BdrDb->new(
        node => $node,
        dbname => $dbname,
        name => $node->name);
    if ($node->name eq 'node1')
    {
        # Deliberately create the BDR node after making a pglogical
        # node first on this instance, so we notice if we break that.
        $db->create;
    }
    $db->bdr_create;
    push @dbs, $db;
}

# With node[0] as the "root", create some tables
$dbs[0]->safe_psql(q[
    CREATE TABLE seed_table (
        id serial primary key,
        blah text not null
    );
]);

$dbs[0]->safe_psql(q[ INSERT INTO seed_table(blah) VALUES ('seed') ]);

# Time to bring up BDR on the first node
my $ngname = 'mygroup';
my $nodegroup = $dbs[0]->bdr_create_node_group($ngname);
BAIL_OUT("test bug, bdr_create_node_group didn't not return BdrDbGroup")
    unless ($nodegroup->isa('BdrDbGroup'));
$dbs[0]->bdr_wait_for_join;

TODO: {
    local $TODO = "RM#999 automatically add tables to replication sets on group create";
    ok($dbs[0]->replication_set_has_table($ngname, 'seed_table'),
        "replicated tables automatically added to dfl nodegroup repset on creation");

    # TODO: this should go away once RM#999 is fixed
    $dbs[0]->bdr_replication_set_add_table('seed_table', $ngname);
}

# Join node1, so we have functioning BDR
$dbs[1]->bdr_join_node_group($nodegroup, $dbs[0]);
$dbs[1]->bdr_wait_for_join;

TODO: {
    local $TODO = "RM#863 replication set membership changes should be replicated to peers";
    ok($dbs[1]->replication_set_has_table($ngname, 'seed_table'),
        "replicated tables automatically added to dfl nodegroup repset on creation");

    # This should go away once RM#863 is fixed
    $dbs[1]->bdr_replication_set_add_table('seed_table');
}

# Create some new tables and contents
$dbs[0]->bdr_replicate_ddl(q[
    CREATE TABLE public.tbl_included (
        id integer primary key,
        other integer,
        blah text
    );
]);

# TODO: should go away when we implement RM#999
$dbs[0]->bdr_replication_set_add_table('tbl_included', $ngname);

ok($dbs[0]->replication_set_has_table($ngname, 'tbl_included'),
    "adding to repset worked")
    or diag "tables: " . $dbs[0]->safe_psql("SELECT * FROM pglogical.TABLES");

$dbs[0]->wait_for_catchup_all;

# OK, check how it replicated to node1

TODO: {
    local $TODO = "RM#863 replicate replication set additions";
    ok($dbs[1]->replication_set_has_table($ngname, 'tbl_included'),
        "replicated tables automatically replicated on peers too");

    # This should go away when RM#863 is fixed
    $dbs[1]->bdr_replication_set_add_table('tbl_included');
}

# Add some data in each node to test simple MM behaviour
foreach my $node (0, 1) {
    $dbs[$node]->safe_psql(qq[INSERT INTO tbl_included (id, blah) VALUES ($node, 'from_node$node')]);
}

$dbs[0]->wait_for_catchup_all;

is($dbs[0]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]),
   "0|from_node0\n1|from_node1",
   'data replicated, node0');
is($dbs[1]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]),
   "0|from_node0\n1|from_node1",
   'data replicated, node1');

#
# Third node
#
# It's time to bring a third node online for our multimaster conflict testing.
# We'll clone from the 2nd node just for variety.
#
# TODO: start the third node in logical standby (catchup) and test catchup mode.

$dbs[2]->bdr_join_node_group($nodegroup, $dbs[0]);
$dbs[2]->bdr_wait_for_join;

TODO: {
    local $TODO = "RM#863 replicate replication set additions";
    ok($dbs[2]->replication_set_has_table($ngname, 'tbl_included'),
        "replicated tables automatically replicated on peers too");

    # This should go away when RM#863 is fixed
    $dbs[2]->bdr_replication_set_add_table('tbl_included');
}

# Initial data is as expected
is($dbs[2]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]),
   '0|from_node0\n1|from_node1',
   'data replicated, node1');

# Do a trivial no-conflict MM insert
$dbs[0]->safe_psql(qq[INSERT INTO tbl_included (id, blah) VALUES (10, 'from_node0');]);
$dbs[1]->safe_psql(qq[INSERT INTO tbl_included (id, blah) VALUES (11, 'from_node2');]);
$dbs[2]->safe_psql(qq[INSERT INTO tbl_included (id, blah) VALUES (12, 'from_node2');]);

# and check it.
$dbs[2]->wait_for_catchup_all;

my $expected = q[0|from_node0
1|from_node1
10|from_node0
11|from_node1
12|from_node2];

foreach my $i (0, 1, 2) {
    is($dbs[$i]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]), $expected, "data replicated, node$i");
}

done_testing();

1;
