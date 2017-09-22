bdr_version='3.0.0'
bdr_version_num=30000

MODULE_big = bdr
EXTENSION = bdr
PGFILEDESC = "bdr - async multimaster logical replication"

DATA = bdr--3.0.0.sql

OBJS = \
	bdr_apply.o \
	bdr_catalogs.o \
	bdr_catcache.o \
	bdr_consensus.o \
	bdr_functions.o \
	bdr_join.o \
	bdr_manager.o \
	bdr_messaging.o \
	bdr_msgbroker.o \
	bdr_msgbroker_receive.o \
	bdr_msgbroker_send.o \
	bdr_msgformats.o\
	bdr_output.o \
	bdr_pgl_plugin.o \
	bdr_shmem.o \
	bdr_state.o \
	bdr_sync.o \
	bdr_worker.o

REGRESS = preseed init simple views wait part

EXTRA_CLEAN += 

# For regression checks
# http://www.postgresql.org/message-id/CAB7nPqTsR5o3g-fBi6jbsVdhfPiLFWQ_0cGU5=94Rv_8W3qvFA@mail.gmail.com
# this makes "make check" give a useful error
abs_top_builddir = .
NO_TEMP_INSTALL = yes

PG_CONFIG ?= pg_config

PGVER := $(shell $(PG_CONFIG) --version | sed 's/^[^0-9/]*\([0-9][0-9\.]\+\).*/\1/g' | awk -F . '{ print $$1$$2 }')

PG_CPPFLAGS += -I$(libpq_srcdir) -I$(realpath $(srcdir)/compat$(PGVER)) -I$(includedir)/pglogical -O0 -ggdb
SHLIB_LINK += $(libpq)

OBJS += $(srcdir)/compat$(PGVER)/bdr_compat.o

control_path = $(abspath $(srcdir))/bdr.control
EXTRA_CLEAN += $(control_path)

bdr_version_h = $(abspath $(srcdir))/bdr_version.h
EXTRA_CLEAN += $(bdr_version_h)


PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


# We can't do a normal 'make check' because PGXS doesn't support
# creating a temp install. We don't want to use a normal PGXS
# 'installcheck' though, because it's a pain to set up a temp install
# manually, with the config overrides needed.
#
# We compromise by using the install we're building against, installing
# glogical into it, then making a temp instance. This means that 'check'
# affects the target DB install. Nobody with any sense runs 'make check'
# under a user with write permissions to their production PostgreSQL
# install (right?)
# But this is still not ideal.
regresscheck:
	$(MKDIR_P) regression_output
	$(pg_regress_check) \
	    --temp-config ./regress-postgresql.conf \
	    --temp-instance=./tmp_check \
	    --outputdir=./regression_output \
	    --create-role=logical \
	    $(REGRESS)

check: install regresscheck

# make doesn't figure out that these are the same thing unless we tell it
bdr.control: $(control_path)

$(control_path): bdr.control.in Makefile
	sed 's/__BDR_VERSION__/$(bdr_version)/' $(realpath $(srcdir)/bdr.control.in) > $(control_path)

$(bdr_version_h): bdr_version.h.in Makefile
	sed 's/__BDR_VERSION__/$(bdr_version)/;s/__BDR_VERSION_NUM__/$(bdr_version_num)/;s/__BDR_VERSION_DATE__/$(shell date -I)/;s/__BDR_VERSION_GITHASH__/$(GITHASH)/;' $(realpath $(srcdir)/bdr_version.h.in) > $(bdr_version_h)

$(OBJS): $(bdr_version_h)

all: $(control_path) $(bdr_version_h)

GITHASH=$(shell if [ -e .distgitrev ]; then cat .distgitrev; else git rev-parse --short HEAD; fi)

dist-common: clean
	@if test "$(wanttag)" -eq 1 -a "`git name-rev --tags --name-only $(GITHASH)`" = "undefined"; then echo "cannot 'make dist' on untagged tree; tag it or use make git-dist"; exit 1; fi
	@rm -f .distgitrev .distgittag
	@if ! git diff-index --quiet HEAD; then echo >&2 "WARNING: git working tree has uncommitted changes to tracked files which were INCLUDED"; fi
	@if [ -n "`git ls-files --exclude-standard --others`" ]; then echo >&2 "WARNING: git working tree has unstaged files which were IGNORED!"; fi
	@echo $(GITHASH) > .distgitrev
	@git name-rev --tags --name-only `cat .distgitrev` > .distgittag
	@(git ls-tree -r -t --full-tree HEAD --name-only \
	  && cd bdr_dump\
	  && git ls-tree -r -t --full-tree HEAD --name-only | sed 's/^/bdr_dump\//'\
	 ) |\
	  tar cjf "${distdir}.tar.bz2" --transform="s|^|${distdir}/|" --no-recursion \
	    -T - .distgitrev .distgittag
	@echo >&2 "Prepared ${distdir}.tar.bz2 for rev=`cat .distgitrev`, tag=`cat .distgittag`"
	@rm -f .distgitrev .distgittag
	@md5sum "${distdir}.tar.bz2" > "${distdir}.tar.bz2.md5"
	@if test -n "$(GPGSIGNKEYS)"; then gpg -q -a -b $(shell for x in $(GPGSIGNKEYS); do echo -u $$x; done) "${distdir}.tar.bz2"; else echo "No GPGSIGNKEYS passed, not signing tarball. Pass space separated keyid list as make var to sign."; fi

dist: distdir=bdr-$(bdr_version)
dist: wanttag=1
dist: dist-common

git-dist: distdir=bdr-$(bdr_version)_git$(GITHASH)
git-dist: wanttag=0
git-dist: dist-common


# runs TAP tests

# PGXS doesn't support TAP tests yet.
# Copy perl modules in postgresql_srcdir/src/test/perl
# to postgresql_installdir/lib/pgxs/src/test/perl

$(pgxsdir)/src/test/perl/PostgresNode.pm:
	@[ -e $(pgxsdir)/src/test/perl/PostgresNode.pm ] || ( echo -e "----ERROR----\nCan't run prove_installcheck, copy src/test/perl/* to $(pgxsdir)/src/test/perl/ and retry\n-------------" && exit 1)


prove_installcheck: $(pgxsdir)/src/test/perl/PostgresNode.pm install
	rm -rf $(CURDIR)/tmp_check/
	cd $(srcdir) && PERL5LIB=t/ TESTDIR='$(CURDIR)' PATH="$(shell $(PG_CONFIG) --bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' top_builddir='$(CURDIR)/$(top_builddir)' PG_REGRESS='$(pgxsdir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(or $(PROVE_TESTS),t/*.pl)

check_prove: prove_installcheck

.PHONY: all check regresscheck
