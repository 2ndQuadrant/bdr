MODULE_big = test_logical_decoding
OBJS = test_logical_decoding.o

EXTENSION = test_logical_decoding
DATA = test_logical_decoding--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_logical_decoding
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

test_logical_decoding.o: test_logical_decoding.c

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
installcheck:;

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress

submake-test_decoding:
	$(MAKE) -C $(top_builddir)/contrib/test_decoding

check: all | submake-regress submake-test_decoding
	$(pg_regress_check) --temp-config $(top_srcdir)/contrib/test_logical_decoding/logical.conf \
	    --temp-install=./tmp_check \
	    --extra-install=contrib/test_decoding \
	    --extra-install=contrib/test_logical_decoding \
	    ddl

PHONY: submake-test_decoding submake-regress
