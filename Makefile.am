noinst_LIBRARIES =
lib_LTLIBRARIES = libcolumnfile.la
check_PROGRAMS = src/columnfile_test
include_HEADERS = src/columnfile.h src/columnfile-capnp.h

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libcolumnfile.pc

TESTS = $(check_PROGRAMS)

ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  $(CAPNP_CFLAGS) \
  $(LIBLZ4_CFLAGS) \
  $(LIBLZMA_CFLAGS) \
  $(ZLIB_CFLAGS)
AM_LDFLAGS = -lpthread
AM_CXXFLAGS = -Wall -std=c++17

include third_party/gtest/Makefile.am

libcolumnfile_la_SOURCES = \
  src/columnfile-capnp.cc \
  src/columnfile-internal.h \
  src/columnfile-reader.cc \
  src/columnfile-select.cc \
  src/columnfile-writer.cc \
  src/zlib-parallel.cc
libcolumnfile_la_LIBADD = \
  $(CAPNP_LIBS) \
  $(LIBLZ4_LIBS) \
  $(LIBLZMA_LIBS) \
  $(ZLIB_LIBS) \
  -lsnappy
libcolumnfile_la_LDFLAGS = -no-undefined -version-info 0:0:0

src_columnfile_test_SOURCES = \
  src/columnfile_test.cc 
src_columnfile_test_LDADD = \
  libcolumnfile.la \
  third_party/gtest/libgtest.a \
  -lcapnpc \
  $(CAPNP_LIBS)

EXTRA_DIST = \
  LICENSE \
  README.md \
  debian/changelog \
  debian/compat \
  debian/control \
  debian/copyright \
  debian/libcolumnfile-dev.dirs \
  debian/libcolumnfile-dev.install \
  debian/libcolumnfile0.dirs \
  debian/libcolumnfile0.install \
  debian/rules \
  debian/source/format \
  libcolumnfile.pc.in \
  testdata/000002.col testdata/000004.col \
  testdata/000008.col testdata/000010.col \
  testdata/000021.col testdata/000023.col \
  testdata/000025.col testdata/000026.col \
  testdata/000027.col testdata/000028.col \
  testdata/000029.col testdata/000044.col \
  testdata/000046.col testdata/000149.col \
  testdata/000155.col testdata/000156.col \
  testdata/000157.col testdata/000161.col \
  testdata/000407.col testdata/000414.col \
  testdata/000416.col testdata/000422.col \
  testdata/000449.col testdata/000476.col \
  testdata/000707.col testdata/001101.col \
  testdata/001112.col testdata/001132.col \
  testdata/001155.col testdata/001192.col \
  testdata/001196.col testdata/001236.col \
  testdata/001529.col testdata/001934.col \
  testdata/004293.col testdata/005710.col \
  testdata/006019.col testdata/006222.col \
  testdata/006966.col testdata/007437.col \
  testdata/008330.col testdata/009100.col \
  testdata/addressbook.capnp
