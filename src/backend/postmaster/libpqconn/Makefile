#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/postmaster/libpqconn
#
# IDENTIFICATION
#    src/backend/postmaster/libpqconn/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/postmaster/libpqconn
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = libpqconn.o $(WIN32RES)
SHLIB_LINK_INTERNAL = $(libpq)
SHLIB_LINK = $(filter -lintl, $(LIBS))
SHLIB_PREREQS = submake-libpq
PGFILEDESC = "libpqconn - open libpq connection"
NAME = libpqconn

all: all-shared-lib

include $(top_srcdir)/src/Makefile.shlib

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean maintainer-clean: clean-lib
	rm -f $(OBJS)
