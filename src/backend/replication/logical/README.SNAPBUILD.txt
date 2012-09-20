= Snapshot Building =
:author: Andres Freund, 2nQuadrant Ltd

== Why do we need timetravel catalog access ==

When doing WAL decoding (see DESIGN.txt for reasons to do so), we need to know
how the catalog looked at the point a record was inserted into the WAL, because
without that information we don't know much more about the record other than
its length.  It's just an arbitrary bunch of bytes without further information.
Unfortunately, due the possibility that the table definition might change we
cannot just access a newer version of the catalog and assume the table
definition continues to be the same.

If only the type information were required, it might be enough to annotate the
wal records with a bit more information (table oid, table name, column name,
column type) --- but as we want to be able to convert the output to more useful
formats such as text, we additionally need to be able to call output functions.
Those need a normal environment including the usual caches and normal catalog
access to lookup operators, functions and other types.

Our solution to this is to add the capability to access the catalog such as it
was at the time the record was inserted into the WAL. The locking used during
WAL generation guarantees the catalog is/was in a consistent state at that
point.  We call this 'time-travel catalog access'.

Interesting cases include:

- enums
- composite types
- extension types
- non-C functions
- relfilenode to table OID mapping

Due to postgres' non-overwriting storage manager, regular modifications of a
table's content are theoretically non-destructive. The problem is that there is
no way to access an arbitrary point in time even if the data for it is there.

This module adds the capability to do so in the very limited set of
circumstances we need it in for WAL decoding. It does *not* provide a general
time-travelling facility.

A 'Snapshot' is the data structure used in postgres to describe which tuples
are visible and which are not. We need to build a Snapshot which can be used to
access the catalog the way it looked when the wal record was inserted.

Restrictions:

- Only works for catalog tables
- Snapshot modifications are somewhat expensive
- it cannot build initial visibility information for every point in time, it
  needs a specific set of circumstances for that
- limited window in which we can build snapshots

== How are time-travel snapshots built ==

'Hot Standby' added infrastructure to build snapshots from WAL during recovery in
the 9.0 release. Most of that can be reused for our purposes.

We cannot reuse all of the hot standby infrastructure because:

- we are not in recovery
- we need to look *inside* transactions
- we need the capability to have multiple different snapshots arround at the same time

We need to provide two kinds of snapshots that are implemented rather
differently in their vanilla postgres incarnation:

- SnapshotNow
- SnapshotMVCC

We need both because if any operators use normal functions they will get
executed with SnapshotMVCC semantics, while the catcache et al
will rely on SnapshotNow semantics.  (Note that SnapshotNow here cannot be a
normal SnapshotNow because we wouldn't access the old version of the catalog in
that case. Instead we need something like an MVCC snapshot with the correct
visibility information.  This also means that such a snapshot won't have some
race issues that normal SnapshotNow has.)

Every time a catalog-changing-transaction commits, all other transactions will
need a new snapshot that marks that transaction (and its subtransactions) as
visible.

Our snapshot representation is a bit different from normal snapshots, but we
still reuse the normal SnapshotData struct:

- +Snapshot->xip+ contains all transaction we consider committed
- +Snapshot->subxip+ contains all transactions belonging to our transaction,
  including the toplevel one

The meaning of +xip+ is inverted in comparison with non-timetravel snapshots
because usually only a tiny percentage of comitted transactions will have
modified the catalog between xmin and xmax. It also makes subtransaction
handling easier (we cannot query pg_subtrans).

== Building of initial snapshot ==

We can start building an initial snapshot as soon as we find either an
+XLOG_RUNNING_XACTS+ or an +XLOG_CHECKPOINT_SHUTDOWN+ record because they allow us
to know how many transactions are running.

We need to know which transactions were running when we start to build a
snapshot/start decoding as we don't have enough information about them (they
could have done catalog modifications before we started watching). Also, we
wouldn't have the complete contents of those transactions, because we started
reading after they began.  (The latter is also important when building
snapshots that can be used to build a consistent initial clone.)

There also is the problem that +XLOG_RUNNING_XACT+ records can be
'suboverflowed' which means there were more running subtransactions than
fitting into shared memory. In that case we use the same incremental building
trick hot standby uses which is either

1. wait till further +XLOG_RUNNING_XACT+ records have a running->oldestRunningXid
after the initial xl_runnign_xacts->nextXid
2. wait for a further +XLOG_RUNNING_XACT+ that is not overflowed or
a +XLOG_CHECKPOINT_SHUTDOWN+

.Note on suboverflowed
**************************
We probably don't need to care about ->suboverflowed at all as we only
need to know about committed XIDs and we get enough information about
subtransactions at commit.  More thinking needed.
**************************

When we start building a snapshot we are in the +SNAPBUILD_START+ state. As
soon as we find any visibility information, even if incomplete, we change to
+SNAPBUILD_INITIAL_POINT+.

When we have collected enough information to decode any transaction starting
after that point in time we fall over to +SNAPBUILD_FULL_SNAPSHOT+. If those
transactions commit before the next state is reached, we throw their complete
contents away.

As soon as all transactions that were running when we switched over to
+SNAPBUILD_FULL_SNAPSHOT+ commit, we change state to +SNAPBUILD_CONSISTENT+.
Every transaction that commits from now on gets handed to the output plugin.
When doing the switch to +SNAPBUILD_CONSISTENT+ we optionally export a snapshot
which makes all transactions that committed up to this point visible.  This
exported snapshot can be used to run pg_dump; replaying all changes emitted
by the output plugin on a database restored from such a dump will result in
a consistent clone.

["ditaa",scaling="0.8"]
---------------

+-------------------------+
|SNAPBUILD_START          |-----------------------+
|                         |-----------+           |
+-------------------------+           |           |
             |                        |           |
 XLOG_RUNNING_XACTS suboverflowed     |   saved snapshot
             |                        |           |
             |                        |           |
             |                        |           |
             v                        |           |
+-------------------------+           v           v
|SNAPBUILD_INITIAL        |---------------------->+
|                         |---------->+           |
+-------------------------+           |           |
             |                        |           |
 oldestRunningXid past initialNextXid |           |
             |                        |           |
             |  XLOG_RUNNING_XACTS !suboverflowed |
             v                        |           |
+-------------------------+           |           |
|SNAPBUILD_FULL_SNAPSHOT  |<----------+           v
|                         |---------------------->+
+-------------------------+                       |
             |                                    |
             |                    XLOG_CHECKPOINT_SHUTDOWN
 any running txn's finished                       |
             |                                    |
             v                                    |
+-------------------------+                       |
|SNAPBUILD_CONSISTENT     |<----------------------+
|                         |
+-------------------------+

---------------

== Snapshot Management ==

Whenever a transaction is detected as having started during decoding in
+SNAPBUILD_FULL_SNAPSHOT+ state, we distribute the currently maintained
snapshot to it (i.e. call ApplyCacheAddBaseSnapshot). This serves as its
initial SnapshotNow and SnapshotMVCC. Unless there are concurrent catalog
changes that snapshot won't ever change.

Whenever a transaction-with-catalog-changes commits, we iterate over all
concurrently active transactions and add a new SnapshotNow to it
(ApplyCacheAddBaseSnapshot(current_lsn)). This is required because any row
written from now that point on will have used the changed catalog contents.
(This is possible to occur even with correct locking.)

SnapshotNow's need to be setup globally so the syscache and other pieces access
it transparently. This is done using two new tqual.h functions:
SetupDecodingSnapshots() and RevertFromDecodingSnapshots().

== Catalog/User Table Detection ==

Right now, we assume that every transaction that commits before we reach
+SNAPBUILD_CONSISTENT+ state has made catalog modifications.  (We used to
try to detect which ones didn't, but it turned out not to be reliable enough.)

.XXX  Rewrite this section!
**********************
As far as I can tell, this works by examining the xact and seeing if it has
invalidation messages or has logged HEAP2_NEW_CID or HEAP_INPLACE.
**********************


.XXX
**********************
Note that that syscache is a bit problematic because its not actually
unique because shared/nailed catalogs store a 0 as relfilenode (they are stored
in the relmapper). Those are never looked up though, so it might be
ok. Unfortunately it doesn't seem to be possible to use a partial index (WHERE
relfilenode != 0) here.
**********************

.XXX
**********************
For some usecases it would be useful to treat some user specified tables
as catalog tables
(AFAIK this is already implemented)
**********************

== System Table Rewrite Handling ==

XXX, expand, XXX

NOTES:

- always using newest relmapper, use newest invalidations
- old tuples are preserved across rewrites, thats fine
- REINDEX/CLUSTER pg_class; in a transaction

== mixed DDL/DML transaction handling  ==

When a transactions uses DDL and DML in the same transaction things get a bit
more complicated because we need to handle CommandIds and ComboCids as we need
to use the correct version of the catalog when decoding the individual tuples.

CommandId handling itself is relatively simple, we can figure out the current
CommandId relatively easily by looking at the currently used one in
changes. The problematic part is that those CommandId frequently will not be
actual cmin or cmax values but ComboCids. Those are used to minimize space in
the heap. During normal operation cmin/cmax values are only used within the
backend emitting those rows and only during one toplevel transaction, so
instead of storing cmin/cmax only a reference to an in-memory value is stored
that contains both. Whenever we see a new CommandId we call
ApplyCacheAddNewCommandId.

To resolve this problem during heap_* whenever we generate a new combocid
(detected via an new parameter to HeapTupleHeaderAdjustCmax) in a catalog table
we log the new XLOG_HEAP2_NEW_COMBOCID record containing the mapping. During
decoding this ComboCid is added to the applycache
(ApplyCacheAddNewComboCid). They are only guaranteed to be visible within a
single transaction, so we cannot simply setup all of them globally. Before
calling the output plugin ComboCids are temporarily setup and torn down
afterwards.

All this only needs to happen in the transaction performing the DDL.

== Cache Handling ==

As we allow usage of the normal {sys,cat,rel,..}cache we also need to integrate
cache invalidation. For transactions without DDL thats easy as everything is
already provided by HS. Everytime we read a commit record we apply the sinval
messages contained therein.

For transactions that contain DDL and DML cache invalidation needs to happen
more frequently because we need to all tore down all caches that just got
modified. To do that we simply apply all invalidation messages that got
collected at the end of transaction and apply them after every single change.
At some point this can get optimized by generating new local invalidation
messages, but that seems too complicated for now.

XXX: think/talk about syscache invalidation of relmapper/pg_class changes.

== xmin Horizon Handling ==

Reusing MVCC for timetravel access has one obvious major problem:
VACUUM. Obviously we cannot keep data in the catalog indefinitely. Also
obviously, we want autovacuum/manual vacuum to work as before.

The idea here is to reuse the infrastrcuture built for hot_standby_feedback
which allows us to keep the xmin horizon of a walsender backend artificially
low. We keep it low enough so we can restart decoding from the last location
the client has confirmed to be safely received. The means that we keep it low
enough to contain the last checkpoints oldestXid value.

That also means we need to make that value persist across restarts/crashes in a
very similar manner to twophase.c's. That infrastructure actually also useful
to make hot_standby_feedback work properly across primary restarts.

== Restartable Decoding ==

As we want to generate a consistent stream of changes we need to have the
ability to start from a previously decoded location without going to the whole
multi-phase setup because that would make it very hard to calculate up to where
we need to keep information.

To make that easier everytime a decoding process finds an online checkpoint
record it exlusively takes a global lwlock and checks whether visibility
information has been already been written out for that checkpoint and does so
if not. We only need to do that once as visibility information is the same
between all decoding backends.
