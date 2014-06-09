subdir = contrib/bdr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

OBJS= bdr_init_copy.o
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

all: bdr_init_copy

bdr_init_copy: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(libpq_pgport) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) bdr_init_copy$(X) '$(DESTDIR)$(bindir)/bdr_init_copy$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/bdr_init_copy$(X)'

clean distclean maintainer-clean:
	rm -f bdr_init_copy$(X) $(OBJS)

