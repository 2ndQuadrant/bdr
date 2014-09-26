# contrib/bdr/Makefile
#
# Please test changes here against USE_PGXS=1 as well
#

MODULE_big = bdr

EXTENSION = bdr
DATA = bdr--0.7.1.sql bdr--0.7--0.7.1.sql
DOCS = bdr.conf.sample README.bdr
SCRIPTS = scripts/bdr_initial_load bdr_init_copy

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

OBJS = \
	bdr.o \
	bdr_apply.o \
	bdr_catalogs.o \
	bdr_conflict_handlers.o \
	bdr_conflict_logging.o \
	bdr_compat.o \
	bdr_commandfilter.o \
	bdr_count.o \
	bdr_executor.o \
	bdr_init_replica.o \
	bdr_locks.o \
	bdr_output.o \
	bdr_relcache.o \
	bdr_seq.o

# Can only be built using pgxs
USE_PGXS=1

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

DATE=$(shell date "+%Y-%m-%d")
GITHASH=$(shell if [ -e .distgitrev ]; then cat .distgitrev; else git rev-parse --short HEAD; fi)
BDR_VERSION=$(shell awk '/^\#define BDR_VERSION / { print $3; }' bdr_version.h.in | cut -d '"' -f 2)

bdr_version.h: bdr_version.h.in
	sed '0,/BDR_VERSION_DATE/s,\(BDR_VERSION_DATE\).*,\1 "$(DATE)",;0,/BDR_VERSION_GITHASH/s,\(BDR_VERSION_GITHASH\).*,\1 "$(GITHASH)",' $< >$@

bdr.o: bdr_version.h

bdr_init_copy: bdr_init_copy.o
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(libpq_pgport) $(LIBS) -o $@$(X)

scripts/bdr_initial_load: scripts/bdr_initial_load.in
	sed -e "s/BDR_VERSION/$(BDR_VERSION)/" -e "s/PG_VERSION/$(VERSION)/" $< > $@

all: all-lib bdr_init_copy

clean: additional-clean

additional-clean:
	rm -f bdr_init_copy$(X) bdr_init_copy.o
	rm -f bdr_version.h
	rm -f .distgitrev

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
installcheck: ;

check: regresscheck isolationcheck

REGRESSCHECKS=init \
	ddl/create ddl/alter_table ddl/extension ddl/function ddl/grant ddl/namespace ddl/sequence ddl/view \
	dml/basic dml/contrib dml/delete_pk dml/extended dml/missing_pk dml/mixed dml/toasted


ISOLATIONCHECKS=\
	isolation/waitforstart \
	isolation/ddlconflict \
	isolation/dmlconflict_ii \
	isolation/dmlconflict_uu \
	isolation/dmlconflict_ud \
	isolation/dmlconflict_dd \
	isolation/alter_table \
	isolation/basic_triple_node
#	this test demonstrates a divergent conflict, so deactivate for now
#	isolation/update_pk_change_conflict

# XXX: Add a check that these are installed
REQUIRED_EXTENSIONS="btree_gist"
REQUIRED_TEST_EXTENSIONS="pg_trgm cube hstore"

regresscheck: all install
	[ -e pg_hba.conf ] || ln -s $(top_srcdir)/contrib/bdr/pg_hba.conf .

	mkdir -p results/ddl
	mkdir -p results/dml

	./run_tests --config bdr_regress.conf \
		--testbinary src/test/regress/pg_regress \
		$(REGRESSCHECKS)

isolationcheck: all install
	mkdir -p results/isolation

	./run_tests --config bdr_isolationregress.conf \
		--testbinary src/test/isolation/pg_isolation_regress \
		--dbname node1,node2,node3 \
		$(ISOLATIONCHECKS)

bdr_pgbench_check: bdr_pgbench_check.sh
	sed -e 's,@bindir@,$(bindir),g' \
	    -e 's,@libdir@,$(libdir),g' \
	    -e 's,@MAKE@,$(MAKE),g' \
	    -e 's,@top_srcdir@,$(top_srcdir),g' \
	  $< >$@
	chmod a+x $@

pgbenchcheck: bdr_pgbench_check
	./bdr_pgbench_check

distdir = bdr-$(BDR_VERSION)

git-dist: clean
	rm -f .distgitrev .distgittag
	if ! git diff-index --quiet HEAD; then echo >&2 "WARNING: git working tree has uncommitted changes to tracked files which were INCLUDED"; fi
	if [ -n "`git ls-files --exclude-standard --others`" ]; then echo >&2 "WARNING: git working tree has unstaged files which were IGNORED!"; fi
	echo $(GITHASH) > .distgitrev
	git name-rev --tags --name-only `cat .distgitrev` > .distgittag
	git ls-tree -r -t --full-tree HEAD --name-only |\
	  tar cjf "${distdir}.tar.bz2" --transform="s|^|${distdir}/|" -T - \
	    .distgitrev .distgittag 
	echo >&2 "Prepared ${distdir}.tar.bz2 for rev=`cat .distgitrev`, tag=`cat .distgittag`"
	rm -f .distgitrev .distgittag

PHONY: submake-regress

# phony target...

.PHONY: all check regresscheck isolationcheck
