#!/usr/bin/env perl

# dump_regress_db.pl
#
# After running pg_regress, dump the deparse regression database
# as plain text and munge any hard-coded paths to pg_regress
# tokens.
#
# The resultant output can - assuming it's known to be correct - be
# used to (re)generate output/deparse_test.source

use strict;
use warnings;

use Getopt::Long;

our %options = (
    'pgdata' => undef,
    'port' => 9999,
    'top-builddir' => undef,
);

our %option_defs = (
    'pgdata:s'       => \$options{pgdata},
    'port:i'         => \$options{port},
    'top-builddir:s' => \$options{'top-builddir'},
);

GetOptions(%option_defs);

if(!-d $options{'top-builddir'}) {
    die(qq|Build directory $options{'top-builddir'} not found or not a directory\n|);
}

if(!-d $options{'pgdata'}) {
    die(qq|Data directory $options{'pgdata'} not found or not a directory\n|);
}

# TODO: check port not in use

# TODO: check for active instance
# TODO: make unique log file

my $log_dir = '/tmp/dump-regress.log';
my $pg_ctl= sprintf(
    q|%s/src/bin/pg_ctl/pg_ctl -p %s/src/backend/postgres --pgdata %s -o '-p%i' -l %s -w|,
    $options{'top-builddir'},
    $options{'top-builddir'},
    $options{'pgdata'},
    $options{'port'},
    $log_dir,
);

#print "$pg_ctl\n";exit;
# TODO: check success
`${pg_ctl} start`;

my $pg_dump_cmd = sprintf(
    q`%s/src/bin/pg_dump/pg_dump -p %s --schema-only --no-owner --no-privileges -Fp regression_deparse | egrep -v '^-- Dumped'`,
    $options{'top-builddir'},
    $options{'port'},
);

my $dump_output = `$pg_dump_cmd`;

# TODO: replace hard-coded paths

# Dump input/deparse_test.source

open(SRC, '< ./input/deparse_test.source');
while(<SRC>) {
    print unless /^\s*$/;
}

print $dump_output;

`${pg_ctl} -m fast stop`;
