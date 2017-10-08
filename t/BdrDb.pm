# 
# Model of a BDR database ("bdr node" in BDR terms) on a PostgreSQL instance
# managed by PostgresBdrNode.
#
# This extends PGLDB, and provides pglogical functionality too, so you can test
# mixing pglogical and BDR functionality in a node. BDR specific methods have
# a bdr_ prefix to make it simple to continue to access pglogical functions.
#

use strict;
use warnings;
use v5.10.0;
package BdrDb;
use base ("PGLDB");
use BdrDbGroup;
use PostgresBdrNode;
use PGValues qw(
	quote_ident
	quote_literal
	to_pg_literal
	to_pg_named_arglist
	append_pg_named_arglist
	);
use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);
use Scalar::Util qw(reftype);
use Data::Dumper;

my $trace_sql = $ENV{TRACE_SQL} // 0;
my $verbose_waits = $ENV{VERBOSE_WAITS} // 0;

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw();
@EXPORT_OK = qw();

sub new {
	my ($class, %kwargs) = @_;

	die 'need node arg to be a PostgresBdrNode'
		unless defined $kwargs{node} && $kwargs{node}->isa('PostgresBdrNode');

	die 'need dbname arg'
		unless defined $kwargs{dbname};

	die 'need name arg'
		unless defined $kwargs{name};

	my $self = bless {
		'_node' => $kwargs{node},
		'_dbname' => $kwargs{dbname},
		'_name' => $kwargs{name},
	}, $class;

	return $self;
}

# SQL creation of a BDR node
#
# You can create a PGL node instead with 'create' instead of 'bdr_create'. If
# you later 'bdr_create', it should create the BDR node on top.
#
# Itempotent.
#
sub bdr_create {
	my ($self, %kwargs) = shift;
	my $exists = $self->node->safe_psql('postgres', 'SELECT 1 FROM pg_catalog.pg_database WHERE datname = ' . quote_literal($self->dbname));
	if ($exists eq '')
	{
		$self->node->safe_psql('postgres', 'CREATE DATABASE ' . quote_ident($self->dbname));
	}
	$self->psql('CREATE EXTENSION IF NOT EXISTS bdr CASCADE');
	$self->_bdr_create_node();
}

sub _bdr_create_node {
	my $self = shift;
	my $exists = $self->safe_psql('SELECT 1 FROM bdr.node b INNER JOIN pglogical.node p ON (b.pglogical_node_id = p.node_id) WHERE p.node_name = ' . quote_literal($self->name));
	if (!$exists)
	{
		# 
		# We don't have to check for the case where there's a pglogical node
		# but not BDR node yet. bdr.create_node does that for us.
		#
		$self->safe_psql("SELECT * FROM bdr.create_node("
			. "node_name := " . quote_literal($self->name) . ", "
			. "local_dsn := " . quote_literal($self->connstr) . ")");
	}
}

# Create a new BDR node group
# 
# kwargs may be supplied to set extra options for bdr.create_node_group(...)
#
sub bdr_create_node_group {
	my ($self, $node_group_name) = @_;

	my $node_group = BdrDbGroup->new(name => $node_group_name);

	my $query = "SELECT * FROM bdr.create_node_group("
		. quote_literal($node_group->name)
		. ");";
	printf("creating node group [%s] on bdr node [%s]\n",
 		$node_group->name, $self->name);
	print("Creating with query:\n$query\n") if $trace_sql;
	$self->safe_psql($query);

	$node_group->add($self);

	return $node_group;
}

sub bdr_join_node_group {
	my ($self, $joingroup, $joinvia, %kwargs) = @_;

	die 'joingroup is not a BdrDbGroup'
		unless $joingroup->isa('BdrDbGroup');

	if (defined($joinvia))
	{
		die 'joinnode ' . $joinvia->name . " is not a BdrDb"
			unless $joinvia->isa('BdrDb');
	}
	else
	{
		$joinvia = ($joingroup->members)[0];
		die "nodegroup " . $joingroup->name . " has no members"
			unless defined($joinvia);
	}

	my $pause_standby = '';
	if ($kwargs{pause_in_standby}) {
		$pause_standby = ", pause_in_standby := 't'";
	}

	my $query = "SELECT * FROM bdr.join_node_group("
		. "join_target_dsn := " . quote_literal($joinvia->connstr)
		. ", node_group_name := " . quote_literal($joingroup->name)
		. $pause_standby
		. ");";

	print("Joining with query:\n$query\n") if $trace_sql;

	# This launches an asynchronous join
	$self->safe_psql($query);
}

sub bdr_promote {
	my $self = shift;
	$self->safe_psql('SELECT bdr.promote_node()');
}

# Query bdr_state_journal_details for the latest state tuple. (Will fail
# horribly if the state tuple extradata has any |s in it).
sub bdr_state {
	my $self = shift;
	my @fieldnames = qw(state_counter state state_name entered_time peer_id peer_name extra_data);
	my $query = "SELECT " . (join ",", @fieldnames)
				. " FROM bdr.state_journal_details ORDER BY state_counter DESC LIMIT 1";
	my %ret;
	@ret{@fieldnames} = split /\|/, $self->safe_psql($query);
	return \%ret;
}

sub bdr_wait_for_state {
	my ($self, $expected, $die_on_failed) = @_;
	while (1)
	{
		local $Data::Dumper::Sortkeys = 1;
		my $stateinfo = $self->bdr_state;
		my $state = $stateinfo->{'state_name'};
		last if ($state eq $expected);
		print("state is $state, waiting for $expected; state tuple " . Dumper($stateinfo) . "\n")
			if $verbose_waits;
		die "reached state BDR_NODE_STATE_JOIN_FAILED instead"
			if ($die_on_failed && $state eq 'BDR_NODE_STATE_JOIN_FAILED');
		sleep(1);
	}
}

sub bdr_wait_for_join {
	my $self = shift;
	$self->safe_psql("SELECT bdr.wait_for_join_completion(verbose_progress := true)");
}

sub bdr_replication_set_add_table {
	my ($self, $tablename, $setname, %kwargs) = @_;
	$self->safe_psql("SELECT * FROM bdr.replication_set_add_table(" . quote_literal($tablename) . ", " . quote_literal($setname)
		. append_pg_named_arglist(%kwargs)
		. ")");
}

sub bdr_replication_set_remove_table {
	my ($self, $setname, $tablename) = @_;
	$self->safe_psql("SELECT * FROM bdr.replication_set_remove_table(" . quote_literal($tablename) . ", " . quote_literal($setname) . ")");
}

sub bdr_replicate_ddl {
	my ($self, $ddl) = @_;
	$self->safe_psql(qq[SELECT bdr.replicate_ddl_command(\$DDL\$\n$ddl\n\$DDL\$);]);
}

sub wait_for_catchup_all {
    my ($self) = shift;
    $self->safe_psql(qq[SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);]);
}

1;
