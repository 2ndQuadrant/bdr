#!/usr/bin/env perl
#
# A model of a BDR node group, here called BdrDbGroup because PostgresNode
# claimed the term "Node" first. It's a collection of mutually replicating BdrDb,
# i.e. BDR nodes.
#
# To use this, first create one or more PostgresBdrNode s, i.e. BDR-enabled postgres
# instances. Then create BdrDb instances for each. Create a new nodegroup with one
# of the BdrDb instances as the initial node. Then join the others.
#

use strict;
use warnings;
use v5.10.0;
package BdrDbGroup;
use PostgresBdrNode;
use BdrDb;
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

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw();
@EXPORT_OK = qw();

sub new {
	my ($class, %kwargs) = @_;

	die 'need node group name'
		unless defined $kwargs{name};

	my $self = bless {
		'_members' => [],
		'_membernames' => [],
		'_name' => $kwargs{name},
	}, $class;

	return $self;
}

sub add {
	my ($self, @toadd) = @_;
	# TODO: check membership?
	push @{$self->{_members}}, @toadd;
	push @{$self->{_membernames}}, map { $_->name } @toadd;
}

sub name {
	my $self = shift;
	return $self->{_name};
}

sub members {
	my $self = shift;
	return $self->{_members};
}

sub membernames {
	my $self = shift;
	return $self->{_membernames};
}

1;
