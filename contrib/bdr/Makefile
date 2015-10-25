# contrib/bdr/Makefile
#
# Please test changes here against USE_PGXS=1 as well
#

MODULE_big = bdr

EXTENSION = bdr
DATA = bdr--0.7.2.sql bdr--0.7--0.7.2.sql bdr--0.7.1--0.7.2.sql bdr--0.7.3.sql bdr--0.7.2--0.7.3.sql
DOCS = bdr.conf.sample README.bdr
SCRIPTS = scripts/bdr_initial_load bdr_init_copy

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)
SHLIB_PREREQS = submake-libpq
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

ifndef USE_PGXS
subdir = contrib/bdr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

DATE=$(shell date "+%Y-%m-%d")
GITHASH=$(shell git rev-parse --short HEAD)

bdr_version.h: bdr_version.h.in
	sed '0,/BDR_VERSION_DATE/s,\(BDR_VERSION_DATE\).*,\1 "$(DATE)",;0,/BDR_VERSION_GITHASH/s,\(BDR_VERSION_GITHASH\).*,\1 "$(GITHASH)",' $< >$@

bdr.o: bdr_version.h

bdr_init_copy: bdr_init_copy.o | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(libpq_pgport) $(LIBS) -o $@$(X)

scripts/bdr_initial_load: scripts/bdr_initial_load.in
	sed -e "s/VERSION/$(VERSION)/" $< > $@

all: all-lib bdr_init_copy

clean: additional-clean

additional-clean:
	rm -f bdr_init_copy$(X) bdr_init_copy.o
	rm -f bdr_version.h

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
installcheck: ;

check: all | submake-regress submake-btree_gist submake-pg_trgm submake-cube submake-hstore regresscheck isolationcheck

ifndef USE_PGXS
submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress

submake-isolation: | submake-regress
	$(MAKE) -C $(top_builddir)/src/test/isolation

submake-btree_gist:
	$(MAKE) -C $(top_builddir)/contrib/btree_gist

submake-pg_trgm:
	$(MAKE) -C $(top_builddir)/contrib/pg_trgm

submake-cube:
	$(MAKE) -C $(top_builddir)/contrib/cube

submake-hstore:
	$(MAKE) -C $(top_builddir)/contrib/hstore

REGRESSCHECKS=init \
	ddl/create ddl/alter_table ddl/extension ddl/function ddl/grant ddl/namespace ddl/sequence ddl/view \
	dml/basic dml/contrib dml/delete_pk dml/extended dml/missing_pk dml/mixed dml/toasted dml/table_rewrite

regresscheck:
	[ -e pg_hba.conf ] || ln -s $(top_srcdir)/contrib/bdr/pg_hba.conf .
	mkdir -p results/ddl
	mkdir -p results/dml
	$(pg_regress_check) \
	    --temp-config $(top_srcdir)/contrib/bdr/bdr_ddlregress.conf \
	    --temp-install=./tmp_check \
	    --extra-install=contrib/btree_gist \
	    --extra-install=contrib/bdr \
	    --extra-install=contrib/pg_trgm \
	    --extra-install=contrib/cube \
	    --extra-install=contrib/hstore \
	    $(REGRESSCHECKS)

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

isolationcheck: all | submake-isolation submake-btree_gist
	[ -e pg_hba.conf ] || ln -s $(top_srcdir)/contrib/bdr/pg_hba.conf .
	mkdir -p results/isolation
	$(pg_isolation_regress_check) \
	    --dbname node1,node2,node3 \
	    --temp-config $(top_srcdir)/contrib/bdr/bdr_isolationregress.conf \
	    --temp-install=./tmp_check \
	    --extra-install=contrib/btree_gist \
	    --extra-install=contrib/bdr \
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

else #USE_PGXS

error-pgxs:
	@echo "Regression checks require an in-tree build in contrib/bdr"
	@echo "You cannot run \"make check\" using pgxs"
	exit 1

submake-regress: ;

submake-btree_gist: ;

submake-pg_trgm: ;

submake-cube: ;

submake-hstore: ;

bdr_pgbench_check: error-pgxs
	;

regresscheck: error-pgxs
	;

endif #USE_PGXS



PHONY: submake-regress

# phony target...

.PHONY: all check
