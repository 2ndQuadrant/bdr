subdir = src/test/regress/ddl-deparse
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

all: submake-pg_regress

REGRESS_OPTS := --dlpath=. --extra-install=contrib/pg_stat_statements

check: all
	../pg_regress \
		--inputdir=$(srcdir) \
		$(REGRESS_OPTS) \
		--temp-install=./tmp_check \
		--top-builddir=../../../.. \
		--schedule=$(srcdir)/ddl-schedule

# Same, but enable dummy_seclabel first
check-seclabel: REGRESS_OPTS += --extra-install=src/test/modules/dummy_seclabel --temp-config=$(srcdir)/temp.conf
check-seclabel: all check

submake-pg_regress:
	$(MAKE) -C $(top_builddir)/src/test/regress pg_regress$(X) tablespace-setup
