#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/postmaster
#
# IDENTIFICATION
#    src/backend/postmaster/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/postmaster
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS :=  $(CPPFLAGS) -I$(top_builddir)/src/port -I$(top_srcdir)/src/port

OBJS = autovacuum.o bgworker.o bgwriter.o checkpointer.o fork_process.o \
	pgarch.o pgstat.o postmaster.o startup.o syslogger.o walwriter.o proxy.o

include $(top_srcdir)/src/backend/common.mk
