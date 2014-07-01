# contrib/worker_spi/Makefile

subdir = contrib/bdr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

DATE=$(shell date --iso-8601)
GITHASH=$(shell git rev-parse --short HEAD)

bdr_version.h: bdr_version.h.in
	sed '0,/BDR_VERSION_DATE/s,\(BDR_VERSION_DATE\).*,\1 "$(DATE)",;0,/BDR_VERSION_GITHASH/s,\(BDR_VERSION_GITHASH\).*,\1 "$(GITHASH)",' $< >$@

%%: all

all: bdr_version.h
	$(MAKE) -f $(top_srcdir)/contrib/bdr/output.mk $(MAKECMDGOALS)
	$(MAKE) -f $(top_srcdir)/contrib/bdr/worker.mk $(MAKECMDGOALS)
	$(MAKE) -f $(top_srcdir)/contrib/bdr/bdr_init_copy.mk $(MAKECMDGOALS)

clean: all
	rm -f version.h

check: all


# phony target...

.PHONY: all
