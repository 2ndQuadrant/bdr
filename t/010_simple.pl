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
my $query;

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
$dbs[1]->bdr_join_node_group($nodegroup, $dbs[0], pause_in_standby => 1);
$dbs[1]->bdr_wait_for_join;

$query = q[
    SELECT node_name, peer_state_name FROM bdr.node_summary ORDER BY node_name
];
for my $i (0..1)
{
    TODO: {
        local $TODO = "this is racey until we have full consensus";
        is($dbs[$i]->safe_psql($query), q[node0|BDR_PEER_STATE_ACTIVE
node1|BDR_PEER_STATE_STANDBY],
        "node$i local states correct for 2-node standby")
            or diag $dbs[$i]->safe_psql("SELECT * FROM bdr.node_summary");
    };
}

$dbs[1]->bdr_promote();
$dbs[1]->bdr_wait_for_join;

for my $i (0..1)
{
    TODO: {
        local $TODO = "this is racey until we have full consensus";
        is($dbs[$i]->safe_psql($query), q[node0|BDR_PEER_STATE_ACTIVE
node1|BDR_PEER_STATE_ACTIVE],
        "node$i local states correct for 2-node active")
            or diag $dbs[$i]->safe_psql("SELECT * FROM bdr.node_summary");
    };
}

$query = q[SELECT sub_name,origin_name,target_name,subscription_status,bdr_subscription_mode FROM bdr.subscription_summary ORDER BY 1,2,3];
$dbs[0]->poll_query_until(q[SELECT count(*) = 1 FROM bdr.subscription_summary;]);
is($dbs[0]->safe_psql($query),
    q[mygroup_node1|node1|node0|replicating|n],
    'expected subscriptions found on node0 (2-node)');
$dbs[1]->poll_query_until(q[SELECT count(*) = 1 FROM bdr.subscription_summary;]);
is($dbs[1]->safe_psql($query),
    q[mygroup_node0|node0|node1|replicating|n],
    'expected subscriptions found on node1 (2-node)');

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
$dbs[1]->wait_for_catchup_all;

is($dbs[0]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]),
   "0|from_node0\n1|from_node1",
   'data replicated 2-way, node0');
is($dbs[1]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]),
   "0|from_node0\n1|from_node1",
   'data replicated 2-way, node1');

#
# Third node
#
# It's time to bring a third node online for our multimaster conflict testing.
# We'll clone from the 2nd node just for variety.

$dbs[2]->bdr_join_node_group($nodegroup, $dbs[0], pause_in_standby => 1);
$dbs[2]->bdr_wait_for_join;

$query = q[
    SELECT node_name, peer_state_name FROM bdr.node_summary ORDER BY node_name
];

foreach my $i (0..2)
{
    TODO: {
        local $TODO = "this is racey until we have full consensus";
        is($dbs[$i]->safe_psql($query),
            qq[node0|BDR_PEER_STATE_ACTIVE
node1|BDR_PEER_STATE_ACTIVE
node2|BDR_PEER_STATE_STANDBY],
            "node$i local_state local statuses ok for node2 standby")
                or diag $dbs[$i]->safe_psql("SELECT * FROM bdr.node_summary");
    }
}

$dbs[2]->bdr_promote();
$dbs[2]->bdr_wait_for_join;

$query = q[
    SELECT node_name, peer_state_name FROM bdr.node_summary ORDER BY node_name
];

foreach my $i (0..2)
{
    TODO: {
        local $TODO = "this is racey until we have full consensus";
        is($dbs[$i]->safe_psql($query),
            qq[node0|BDR_PEER_STATE_ACTIVE
node1|BDR_PEER_STATE_ACTIVE
node2|BDR_PEER_STATE_ACTIVE],
            "node$i local_state is ACTIVE for all nodes")
                or diag $dbs[$i]->safe_psql("SELECT * FROM bdr.node_summary");
    }
}

TODO: {
    local $TODO = "RM#863 replicate replication set additions";
    ok($dbs[2]->replication_set_has_table($ngname, 'tbl_included'),
        "replicated tables automatically replicated on peers too");

    # This should go away when RM#863 is fixed
    $dbs[2]->bdr_replication_set_add_table('tbl_included');
}

# Initial data is as expected
is($dbs[2]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]),
   "0|from_node0\n1|from_node1",
   'data copied on join, node2');

# All expected subscriptions exist
$query = q[SELECT sub_name,origin_name,target_name,subscription_status,bdr_subscription_mode FROM bdr.subscription_summary ORDER BY 1,2,3];
$dbs[0]->poll_query_until(q[SELECT count(*) = 2 FROM bdr.subscription_summary;]);
is($dbs[0]->safe_psql($query),
    q[mygroup_node1|node1|node0|replicating|n
mygroup_node2|node2|node0|replicating|n],
    'expected subscriptions found on node0');
is($dbs[1]->safe_psql($query),
    q[mygroup_node0|node0|node1|replicating|n
mygroup_node2|node2|node1|replicating|n],
    'expected subscriptions found on node1');
is($dbs[2]->safe_psql($query),
    q[mygroup_node0|node0|node2|replicating|n
mygroup_node1|node1|node2|replicating|n],
    'expected subscriptions found on node2');

# node2 should have a catchup tracking row for node1, but none of the
# others should have any entries since 2-node join doesn't need one,
# and neither do active nodes when a new node is joining.
$query = q[
    SELECT
        ns.node_name,
        jcm.min_slot_lsn IS NOT NULL,
        jcm.passed_min_slot_lsn,
        jcm.min_slot_lsn <= os.remote_lsn
    FROM bdr.join_catchup_minimum jcm
    JOIN bdr.node_summary ns ON (jcm.node_id = ns.node_id)
    CROSS JOIN LATERAL bdr.gen_slot_name(
            current_database(), ns.node_group_name, ns.node_name,
            (select node_name from bdr.local_node_summary)
        ) AS origin_name
    JOIN pg_catalog.pg_replication_origin_status os ON (origin_name = os.external_id)
    ORDER BY ns.node_name;
    ];
is($dbs[0]->safe_psql($query), '', 'no join catchup minimum records on node0');
is($dbs[1]->safe_psql($query), '', 'no join catchup minimum records on node1');
is($dbs[2]->safe_psql($query), 'node1|t|t|t', 'join catchup minimum record for node1 on node2');

# Do a trivial no-conflict MM insert
$dbs[0]->safe_psql(qq[INSERT INTO tbl_included (id, blah) VALUES (10, 'from_node0');]);
$dbs[1]->safe_psql(qq[INSERT INTO tbl_included (id, blah) VALUES (11, 'from_node1');]);
$dbs[2]->safe_psql(qq[INSERT INTO tbl_included (id, blah) VALUES (12, 'from_node2');]);

# and check it.
$dbs[0]->wait_for_catchup_all;
$dbs[1]->wait_for_catchup_all;
$dbs[2]->wait_for_catchup_all;

my $expected = q[0|from_node0
1|from_node1
10|from_node0
11|from_node1
12|from_node2];

foreach my $i (0, 1, 2) {
    is($dbs[$i]->safe_psql(q[SELECT id, blah FROM tbl_included ORDER BY id]), $expected, "data replicated 3-way, node$i");
}

done_testing();

1;
