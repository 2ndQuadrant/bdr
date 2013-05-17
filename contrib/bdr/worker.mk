# contrib/bdr/worker.mk

MODULE_big = bdr
OBJS = bdr.o bdr_apply.o bdr_count.o

EXTENSION = bdr
DATA = bdr--0.1.sql

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)
SHLIB_PREREQS = submake-libpq

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bdr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
installcheck:;

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress

check: all | submake-regress
	$(pg_regress_check) \
	    --temp-config $(top_srcdir)/contrib/bdr/bdr.conf \
	    --temp-install=./tmp_check \
	    --extra-install=contrib/bdr \
	    extension

PHONY: submake-regress
