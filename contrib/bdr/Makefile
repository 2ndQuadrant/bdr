# contrib/bdr/Makefile

subdir = contrib/bdr
top_builddir = ../..

MODULE_big = bdr

EXTENSION = bdr
DATA = bdr--0.6.sql
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

include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

DATE=$(shell date --iso-8601)
GITHASH=$(shell git rev-parse --short HEAD)

bdr_version.h: bdr_version.h.in
	sed '0,/BDR_VERSION_DATE/s,\(BDR_VERSION_DATE\).*,\1 "$(DATE)",;0,/BDR_VERSION_GITHASH/s,\(BDR_VERSION_GITHASH\).*,\1 "$(GITHASH)",' $< >$@

bdr.o: bdr_version.h

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
installcheck:;

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress

submake-btree_gist:
	$(MAKE) -C $(top_builddir)/contrib/btree_gist

bdr_init_copy: bdr_init_copy.o | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(libpq_pgport) $(LIBS) -o $@$(X)

all: bdr_init_copy

clean: additional-clean

additional-clean:
	rm -f bdr_init_copy$(X) bdr_init_copy.o
	rm -f bdr_version.h

check: all | submake-regress submake-btree_gist regresscheck

REGRESS_DDL_CHECKS=ddl/create ddl/alter_table

regresscheck:
	ln -fs $(top_srcdir)/contrib/bdr/pg_hba.conf .
	mkdir -p results/ddl
	$(pg_regress_check) \
	    --temp-config $(top_srcdir)/contrib/bdr/bdr_ddlregress.conf \
	    --temp-install=./tmp_check \
	    --extra-install=contrib/btree_gist \
	    --extra-install=contrib/bdr \
	    $(REGRESS_DDL_CHECKS)

PHONY: submake-regress


# phony target...

.PHONY: all
