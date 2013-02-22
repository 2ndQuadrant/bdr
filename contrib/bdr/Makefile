# contrib/worker_spi/Makefile

subdir = contrib/bdr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

%%: all

all:
	$(MAKE) -f $(top_srcdir)/contrib/bdr/output.mk $(MAKECMDGOALS)
	$(MAKE) -f $(top_srcdir)/contrib/bdr/worker.mk $(MAKECMDGOALS)

.PHONY: all
