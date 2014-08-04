#!/usr/bin/env sh

#CONFIG
DATADIR=./tmp_check
SCALE=4
CLIENTS=4
RUN_ON_SLAVE="1"
if [ ! -n "$RUNTIME" ]; then
    RUNTIME=100
fi

#INTERNAL
TOPBUILDDIR=@top_srcdir@
BINDIR=@bindir@
LIBDIR=@libdir@
MAKE=@MAKE@
PGCONF=bdr_pgbench.conf
HBACONF=pg_hba.conf
PRIMARY_HOST=localhost
PRIMARY_PORT=7432
PRIMARY_DB=bdr_pgbench
SLAVE_HOST=localhost
SLAVE_DB=bdr_pgbench2
SLAVE_PORT=7432
SCRIPTDIR="$( cd "$(dirname "$0")" ; pwd -P )"

set -e

# get full paths
mkdir -p $DATADIR
rm -r $DATADIR/*
cd $DATADIR
DATADIR=`pwd -P`

BINDIR=$DATADIR/install/$BINDIR
LIBDIR=$DATADIR/install/$LIBDIR

cd $SCRIPTDIR

cd $TOPBUILDDIR
TOPBUILDDIR=`pwd -P`

# install pg and contrib
echo "installing postgres"
cd $TOPBUILDDIR
$MAKE DESTDIR="$DATADIR/install" install >$DATADIR/pginstall.log 2>&1
echo "installing postgres contrib modules"
cd contrib
$MAKE DESTDIR="$DATADIR/install" install >$DATADIR/contribinstall.log 2>&1

# setup environment
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBDIR
DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$LIBDIR
LIBPATH=$LIBPATH:$LIBDIR

# create and start pg instance
echo "initializing postgres"
cd $SCRIPTDIR
$BINDIR/initdb -D $DATADIR/data >$DATADIR/initdb.log 2>&1
cp $PGCONF $DATADIR/data/postgresql.conf
cp $HBACONF $DATADIR/data/pg_hba.conf

$BINDIR/pg_ctl -D $DATADIR/data start -w -l $DATADIR/bdr_pgbench_check_pg.log

# create databases
echo "creating databases"
$BINDIR/psql -h $PRIMARY_HOST -p $PRIMARY_PORT postgres -c "CREATE DATABASE $PRIMARY_DB"
$BINDIR/psql -h $SLAVE_HOST -p $SLAVE_PORT postgres -c "CREATE DATABASE $SLAVE_DB"
$BINDIR/psql -h $PRIMARY_HOST -p $PRIMARY_PORT $PRIMARY_DB -c "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;" > /dev/null
$BINDIR/psql -h $SLAVE_HOST -p $SLAVE_PORT $SLAVE_DB -c "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;" > /dev/null
$BINDIR/psql -h $SLAVE_HOST -p $SLAVE_PORT $SLAVE_DB -c "SELECT pg_sleep(5); CREATE SCHEMA pgbench2; ALTER DATABASE $SLAVE_DB SET search_path=pgbench2,pg_catalog;"

# initialize pgbench
echo "setting up pgbench schema on primary db"
$BINDIR/pgbench -i -s $SCALE -h $PRIMARY_HOST -p $PRIMARY_PORT $PRIMARY_DB 2>&1
$BINDIR/psql -h $PRIMARY_HOST -p $PRIMARY_PORT $PRIMARY_DB -c "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;" > /dev/null
if [ -n "$RUN_ON_SLAVE" ]; then
	echo "setting up pgbench schema on slave db"
	$BINDIR/pgbench -i -s $SCALE -h $SLAVE_HOST -p $SLAVE_PORT $SLAVE_DB 2>&1
	$BINDIR/psql -h $SLAVE_HOST -p $SLAVE_PORT $SLAVE_DB -c "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;" > /dev/null
fi

# run pgbench
echo "running pgbench..."
$BINDIR/pgbench -T $RUNTIME -r -j $CLIENTS -c $CLIENTS -h $PRIMARY_HOST -p $PRIMARY_PORT $PRIMARY_DB 2>&1 &
PRIMARY_BENCHPID=$!
if [ -n "$RUN_ON_SLAVE" ]; then
	$BINDIR/pgbench -T $RUNTIME -r -j $CLIENTS -c $CLIENTS -h $SLAVE_HOST -p $SLAVE_PORT $SLAVE_DB 2>&1 &
	SLAVE_BENCHPID=$!
fi

# wait for pgbench instance(s) to finish
while kill -0 $PRIMARY_BENCHPID 2>>/dev/null || ( [ -n "$SLAVE_BENCHPID" ] && kill -0 $SLAVE_BENCHPID 2>>/dev/null ) ; do
    sleep 1
done

SQL=$(cat <<EOF
SET search_path=pg_catalog;
DO \$\$
DECLARE
    relid oid;
    cnt bigint;
    hsh bigint;
BEGIN
	FOR relid IN SELECT t.relid FROM pg_stat_user_tables t WHERE schemaname NOT IN ('bdr') ORDER BY schemaname, relname
    LOOP
        EXECUTE 'SELECT count(*), sum(hashtext((t.*)::text)) FROM ' || relid::regclass::text || ' t' INTO cnt, hsh;
        RAISE NOTICE '%: %, %', relid::regclass::text, cnt, hsh;
    END LOOP;
END;\$\$;
EOF
)

$BINDIR/psql -h $PRIMARY_HOST -p $PRIMARY_PORT $PRIMARY_DB -c "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;" > /dev/null
$BINDIR/psql -h $SLAVE_HOST -p $SLAVE_PORT $SLAVE_DB -c "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location()::text, pid) FROM pg_stat_replication;" > /dev/null

$BINDIR/psql -h $PRIMARY_HOST -p $PRIMARY_PORT $PRIMARY_DB -c "$SQL" >$DATADIR/primary.chksum 2>&1
$BINDIR/psql -h $SLAVE_HOST -p $SLAVE_PORT $SLAVE_DB -c "$SQL" >$DATADIR/slave.chksum 2>&1

echo "pgbench finished, cleaning up"
# stop pg
$BINDIR/pg_ctl -D $DATADIR/data stop -w -mfast

cd $SCRIPTDIR

diff -u $DATADIR/primary.chksum $DATADIR/slave.chksum > $DATADIR/chksum.diff
if [ ! -n "$?" ]; then
	echo "ERROR: data in databases differ, check $DATADIR/chksum.diff"
	exit 1
fi

