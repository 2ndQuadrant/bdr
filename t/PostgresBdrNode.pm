#!/usr/bin/env perl
#
# This class extends PostgresNdoe with BDR-specific routines
# for setup, building on those for pglogical.
# 

package PostgresBdrNode;
use base ("PostgresPGLNode");
use strict;
use warnings;
use v5.10.0;
use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw(
	get_new_bdr_node
	);

@EXPORT_OK = qw(
	);

#
# Get a new BDR-enabled PostgreSQL instance, ready to spawn
# BDR-enabled databases on. It's not a "BDR Node" in the sense
# we use in BDR, but "Node" in the "PostgresNode.pm" sense.
#
# It's not created yet, but ready to restore from backup,
# initdb, etc.
#
sub get_new_bdr_node
{
	my ($name, $class) = @_;

	$class //= 'PostgresBdrNode';

	return PostgresPGLNode::get_new_pgl_node($name, $class);
}

sub init
{
	my ($self, %kwargs) = @_;
	PostgresPGLNode::init($self, %kwargs);
	# Override earlier entry made by pglogical
	$self->append_conf('postgresql.conf', qq[
shared_preload_libraries = 'pglogical,bdr'
]);
}

sub init_physical_clone
{
	die("Physical clone not supported for BDR yet, see RM#996");
}

1;
