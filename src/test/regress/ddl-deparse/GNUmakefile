top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/Makefile.shlib

all: regress.so

check: all
	rm -rf ./testtablespace && mkdir ./testtablespace
	../pg_regress \
		--extra-install=contrib/pg_stat_statements \
		--temp-install=./tmp_check \
		--top-builddir=../../../.. \
		--dlpath=. \
		--schedule=./ddl-schedule

check-seclabel: all
	rm -rf ./testtablespace && mkdir ./testtablespace
	../pg_regress \
		--extra-install=contrib/pg_stat_statements \
		--extra-install=contrib/dummy_seclabel \
		--temp-install=./tmp_check \
		--temp-config=temp.conf \
		--top-builddir=../../../.. \
		--dlpath=. \
		--schedule=./ddl-schedule



