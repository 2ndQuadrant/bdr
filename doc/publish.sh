#!/bin/sh

set -e -u

rev=$(awk '/^#define BDR_VERSION / { match($3,"\"([0-9]+\\.[0-9]+)\\.[0-9]+",a); print a[1] }' ../bdr_version.h)

pgrev=$(awk -F '=' '/^BDR_PG_MAJORVERSION=/ { print $2; }' ../config.log)

MAKE=${MAKE:-make}
DOCHOST="bdr-project.org"
DOCDIR="/var/www_bdr-project.org/docs/"

if [ "$pgrev" != "'9.4'" ]; then
  echo "documentation should be built when BDR is configured against PostgreSQL 9.4, not $pgrev"
  exit 1
fi

if [ -z "$rev" ]; then
  echo "Couldn't determine version"
  exit 1
else
  echo "Building docs for bdr-plugin $rev"
fi

$MAKE clean
rm -rf html
$MAKE JADEFLAGS="-V website-build"

echo "***Confirm that the bdrversion generated matches this git branch $(git rev-parse --abbrev-ref HEAD)***"
read -p "Enter to configure, control-C to cancel: " DISCARD

echo -n "Uploading... "
RSYNC_RSH="ssh -o StrictHostKeyChecking=no" rsync \
  -r --delete html/ \
  $DOCHOST:$DOCDIR/$rev/
echo "done"
