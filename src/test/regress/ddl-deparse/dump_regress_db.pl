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

use Cwd;
use File::Temp qw(tempfile);
use Getopt::Long;
use IO::Socket::INET;

our %options = (
    'dlpath'       => getcwd(),
    'DLSUFFIX'     => '.so',
    'pgdata'       => undef,
    'port'         => 9999,
    'top-builddir' => undef,
);

our %option_defs = (
    'dlpath:s'       => \$options{'dlpath'},
    'DLSUFFIX:s'     => \$options{'DLSUFFIX'},
    'pgdata:s'       => \$options{'pgdata'},
    'port:i'         => \$options{'port'},
    'top-builddir:s' => \$options{'top-builddir'},
);

GetOptions(%option_defs);

if(!-d $options{'top-builddir'}) {
    die(qq|Build directory $options{'top-builddir'} not found or not a directory\n|);
}

if(!-d $options{'pgdata'}) {
    die(qq|Data directory $options{'pgdata'} not found or not a directory\n|);
}

# Check specified port not in use
my $socket = IO::Socket::INET->new(
    LocalAddr =>'localhost',
    LocalPort => $options{'port'},
    Proto     => 'tcp',
    ReusePort => 1
);

if(!$socket) {
    die qq|Port '$options{'port'}' appears to be in use\n|;
}
close($socket);

my ($fh, $log_file) = tempfile( 'dump_regress_XXXXXX', TMPDIR => 1, SUFFIX => '.log', EXLOCK => 0);

# check for extant pidfile in specified data directory
# (not 100% foolproof but good enough for now)
my $pidfile = qq|$options{'pgdata'}/postmaster.pid|;
if(-e $pidfile) {
    die qq|'$pidfile' exists - is another instance using the data directory?|;
}

my $pg_ctl= sprintf(
    q|%s/src/bin/pg_ctl/pg_ctl -p %s/src/backend/postgres --pgdata %s -o '-p%i' -l %s -w|,
    $options{'top-builddir'},
    $options{'top-builddir'},
    $options{'pgdata'},
    $options{'port'},
    $log_file,
);


# TODO: check success
`${pg_ctl} start`;

my $psql_cmd = sprintf(
    q|%s/src/bin/psql/psql -p %s -d regression -c 'DROP EVENT TRIGGER deparse_test_trg_ddl_command_end'|,
    $options{'top-builddir'},
    $options{'port'},
);

`${psql_cmd}`;

my $pg_dump_cmd = sprintf(
    q`%s/src/bin/pg_dump/pg_dump -p %s --schema-only --no-owner --no-privileges --exclude-schema=deparse -Fp regression | egrep -v '^-- Dumped'`,
    $options{'top-builddir'},
    $options{'port'},
);

my $pg_dump_output = `$pg_dump_cmd`;



# Dump `input/deparse_test.source`, which needs to be prepended to the
# pg_dump output

open(SRC, '< ./input/deparse_test.source');
while(<SRC>) {
    print unless /^\s*$/;
}
my @pg_dump_lines = split(/\n/, $pg_dump_output);

# Replace hard-coded paths with pg_regress tokens
foreach my $line (@pg_dump_lines) {
    $line =~ s/$options{'dlpath'}/\@libdir\@/;
    $line =~ s/$options{'DLSUFFIX'}/\@DLSUFFIX\@/;
    print qq|$line\n|;
}

print qq|\n|;

`${pg_ctl} -m fast stop`;
