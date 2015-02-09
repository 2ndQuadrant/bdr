# This module sets up a test server, for the SSL regression tests.
#
# The server is configured as follows:
#
# - SSL enabled, with the server certificate specified by argument to
#   switch_server_cert function.
# - ssl/root+client_ca.crt as the CA root for validating client certs.
# - reject non-SSL connections
# - a database called trustdb that lets anyone in
# - another database called certdb that uses certificate authentiction, ie.
#   the client must present a valid certificate signed by the client CA
# - two users, called ssltestuser and anotheruser.
#
# The server is configured to only accept connections from localhost. If you
# want to run the client from another host, you'll have to configure that
# manually.
package ServerSetup;

use strict;
use warnings;
use TestLib;
use Test::More;

use Exporter 'import';
our @EXPORT = qw(
  configure_test_server_for_ssl switch_server_cert
);

sub configure_test_server_for_ssl
{
  my $tempdir = $_[0];

  # Create test users and databases
  psql 'postgres', "CREATE USER ssltestuser";
  psql 'postgres', "CREATE USER anotheruser";
  psql 'postgres', "CREATE DATABASE trustdb";
  psql 'postgres', "CREATE DATABASE certdb";

  # enable logging etc.
  open CONF, ">>$tempdir/pgdata/postgresql.conf";
  print CONF "fsync=off\n";
  print CONF "log_connections=on\n";
  print CONF "log_hostname=on\n";
  print CONF "log_statement=all\n";

  # enable SSL and set up server key
  print CONF "include 'sslconfig.conf'";

  close CONF;


  # Copy all server certificates and keys, and client root cert, to the data dir
  system_or_bail "cp ssl/server-*.crt '$tempdir'/pgdata";
  system_or_bail "cp ssl/server-*.key '$tempdir'/pgdata";
  system_or_bail "chmod 0600 '$tempdir'/pgdata/server-*.key";
  system_or_bail "cp ssl/root+client_ca.crt '$tempdir'/pgdata";
  system_or_bail "cp ssl/root+client.crl '$tempdir'/pgdata";

  # Only accept SSL connections from localhost. Our tests don't depend on this
  # but seems best to keep it as narrow as possible for security reasons.
  #
  # When connecting to certdb, also check the client certificate.
  open HBA, ">$tempdir/pgdata/pg_hba.conf";
  print HBA "# TYPE  DATABASE        USER            ADDRESS                 METHOD\n";
  print HBA "hostssl trustdb         ssltestuser     127.0.0.1/32            trust\n";
  print HBA "hostssl trustdb         ssltestuser     ::1/128                 trust\n";
  print HBA "hostssl certdb          ssltestuser     127.0.0.1/32            cert\n";
  print HBA "hostssl certdb          ssltestuser     ::1/128                 cert\n";
  close HBA;
}

# Change the configuration to use given server cert file, and restart
# the server so that the configuration takes effect.
sub switch_server_cert
{
  my $tempdir = $_[0];
  my $certfile = $_[1];

  diag "Restarting server with certfile \"$certfile\"...";

  open SSLCONF, ">$tempdir/pgdata/sslconfig.conf";
  print SSLCONF "ssl=on\n";
  print SSLCONF "ssl_ca_file='root+client_ca.crt'\n";
  print SSLCONF "ssl_cert_file='$certfile.crt'\n";
  print SSLCONF "ssl_key_file='$certfile.key'\n";
  print SSLCONF "ssl_crl_file='root+client.crl'\n";
  close SSLCONF;

  # Stop and restart server to reload the new config. We cannot use
  # restart_test_server() because that overrides listen_addresses to only all
  # Unix domain socket connections.

  system_or_bail 'pg_ctl', 'stop', '-s', '-D', "$tempdir/pgdata", '-w';
  system_or_bail 'pg_ctl', 'start', '-s', '-D', "$tempdir/pgdata", '-w', '-l',
        "$tempdir/logfile";
}
