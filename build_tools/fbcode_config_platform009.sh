#!/bin/sh
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# Set environment variables so that we can compile rocksdb using
# fbcode settings.  It uses the latest g++ and clang compilers and also
# uses jemalloc
# Environment variables that change the behavior of this script:
# PIC_BUILD -- if true, it will only take pic versions of libraries from fbcode. libraries that don't have pic variant will not be included


BASEDIR=`dirname $BASH_SOURCE`
source "$BASEDIR/dependencies_platform009.sh"

CFLAGS=""

# libgcc
LIBGCC_INCLUDE="$LIBGCC_BASE/include/c++/9.3.0 -I $LIBGCC_BASE/include/c++/9.3.0/backward"
LIBGCC_LIBS=" -L $LIBGCC_BASE/lib"

# glibc
GLIBC_INCLUDE="$GLIBC_BASE/include"
GLIBC_LIBS=" -L $GLIBC_BASE/lib"

if test -z $PIC_BUILD; then
  MAYBE_PIC=
else
  MAYBE_PIC=_pic
fi

if ! test $ROCKSDB_DISABLE_SNAPPY; then
  # snappy
  SNAPPY_INCLUDE=" -I $SNAPPY_BASE/include/"
  SNAPPY_LIBS=" $SNAPPY_BASE/lib/libsnappy${MAYBE_PIC}.a"
  CFLAGS+=" -DSNAPPY"
fi

if ! test $ROCKSDB_DISABLE_ZLIB; then
  # location of zlib headers and libraries
  ZLIB_INCLUDE=" -I $ZLIB_BASE/include/"
  ZLIB_LIBS=" $ZLIB_BASE/lib/libz${MAYBE_PIC}.a"
  CFLAGS+=" -DZLIB"
fi

if ! test $ROCKSDB_DISABLE_BZIP; then
  # location of bzip headers and libraries
  BZIP_INCLUDE=" -I $BZIP2_BASE/include/"
  BZIP_LIBS=" $BZIP2_BASE/lib/libbz2${MAYBE_PIC}.a"
  CFLAGS+=" -DBZIP2"
fi

if ! test $ROCKSDB_DISABLE_LZ4; then
  LZ4_INCLUDE=" -I $LZ4_BASE/include/"
  LZ4_LIBS=" $LZ4_BASE/lib/liblz4${MAYBE_PIC}.a"
  CFLAGS+=" -DLZ4"
fi

if ! test $ROCKSDB_DISABLE_ZSTD; then
  ZSTD_INCLUDE=" -I $ZSTD_BASE/include/"
  ZSTD_LIBS=" $ZSTD_BASE/lib/libzstd${MAYBE_PIC}.a"
  CFLAGS+=" -DZSTD"
fi

# location of gflags headers and libraries
GFLAGS_INCLUDE=" -I $GFLAGS_BASE/include/"
GFLAGS_LIBS=" $GFLAGS_BASE/lib/libgflags${MAYBE_PIC}.a"
CFLAGS+=" -DGFLAGS=gflags"

BENCHMARK_INCLUDE=" -I $BENCHMARK_BASE/include/"
BENCHMARK_LIBS=" $BENCHMARK_BASE/lib/libbenchmark${MAYBE_PIC}.a"

BOOST_INCLUDE=" -I $BOOST_BASE/include/"

GLOG_INCLUDE=" -I $GLOG_BASE/include/"
GLOG_LIBS=" $GLOG_BASE/lib/libglog${MAYBE_PIC}.a"

FMT_INCLUDE=" -I $FMT_BASE/include/"
FMT_LIBS=" $FMT_BASE/lib/libfmt${MAYBE_PIC}.a"

DBL_CONV_INCLUDE=" -I $DBL_CONV_BASE/include/"
DBL_CONV_LIBS=" $DBL_CONV_BASE/lib/libdouble-conversion${MAYBE_PIC}.a"

LIBEVENT_INCLUDE=" -I $LIBEVENT_BASE/include/"
LIBEVENT_LIBS=" $LIBEVENT_BASE/lib/libevent${MAYBE_PIC}.a"

# location of jemalloc
JEMALLOC_INCLUDE=" -I $JEMALLOC_BASE/include/"
JEMALLOC_LIB=" $JEMALLOC_BASE/lib/libjemalloc${MAYBE_PIC}.a"

# location of numa
NUMA_INCLUDE=" -I $NUMA_BASE/include/"
NUMA_LIB=" $NUMA_BASE/lib/libnuma${MAYBE_PIC}.a"
CFLAGS+=" -DNUMA"

# location of libunwind
LIBUNWIND="$LIBUNWIND_BASE/lib/libunwind${MAYBE_PIC}.a"

# location of TBB
TBB_INCLUDE=" -isystem $TBB_BASE/include/"
TBB_LIBS="$TBB_BASE/lib/libtbb${MAYBE_PIC}.a"
CFLAGS+=" -DTBB"

# location of LIBURING
LIBURING_INCLUDE=" -isystem $LIBURING_BASE/include/"
LIBURING_LIBS="$LIBURING_BASE/lib/liburing${MAYBE_PIC}.a"
CFLAGS+=" -DLIBURING"

test "$USE_SSE" || USE_SSE=1
export USE_SSE
test "$PORTABLE" || PORTABLE=1
export PORTABLE

BINUTILS="$BINUTILS_BASE/bin"
AR="$BINUTILS/ar"
AS="$BINUTILS/as"

DEPS_INCLUDE="$SNAPPY_INCLUDE $ZLIB_INCLUDE $BZIP_INCLUDE $LZ4_INCLUDE $ZSTD_INCLUDE $GFLAGS_INCLUDE $NUMA_INCLUDE $TBB_INCLUDE $LIBURING_INCLUDE $BENCHMARK_INCLUDE $BOOST_INCLUDE $GLOG_INCLUDE $FMT_INCLUDE $DBL_CONV_INCLUDE $LIBEVENT_INCLUDE"

STDLIBS="-L $GCC_BASE/lib64"

CLANG_BIN="$CLANG_BASE/bin"
CLANG_LIB="$CLANG_BASE/lib"
CLANG_SRC="$CLANG_BASE/../../src"

CLANG_ANALYZER="$CLANG_BIN/clang++"
CLANG_SCAN_BUILD="$CLANG_SRC/llvm/clang/tools/scan-build/bin/scan-build"

if [ -z "$USE_CLANG" ]; then
  # gcc
  CC="$GCC_BASE/bin/gcc"
  CXX="$GCC_BASE/bin/g++"
  AR="$GCC_BASE/bin/gcc-ar"

  CFLAGS+=" -B$BINUTILS"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  JEMALLOC=1
else
  # clang
  CLANG_INCLUDE="$CLANG_LIB/clang/stable/include"
  CC="$CLANG_BIN/clang"
  CXX="$CLANG_BIN/clang++"
  AR="$CLANG_BIN/llvm-ar"

  KERNEL_HEADERS_INCLUDE="$KERNEL_HEADERS_BASE/include"

  CFLAGS+=" -B$BINUTILS -nostdinc -nostdlib"
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/9.x "
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/9.x/x86_64-facebook-linux "
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
  CFLAGS+=" -isystem $CLANG_INCLUDE"
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE/linux "
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE "
  CFLAGS+=" -Wno-expansion-to-defined "
  CXXFLAGS="-nostdinc++"
fi

CFLAGS+=" $DEPS_INCLUDE"
CFLAGS+=" -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_RANGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT -DHAVE_SSE42 -DROCKSDB_IOURING_PRESENT"
CXXFLAGS+=" $CFLAGS"

EXEC_LDFLAGS=" $SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS $NUMA_LIB $TBB_LIBS $LIBURING_LIBS $BENCHMARK_LIBS"
EXEC_LDFLAGS+=" -Wl,--dynamic-linker,/usr/local/fbcode/platform009/lib/ld.so"
EXEC_LDFLAGS+=" $LIBUNWIND"
EXEC_LDFLAGS+=" -Wl,-rpath=/usr/local/fbcode/platform009/lib"
EXEC_LDFLAGS+=" -Wl,-rpath=$GCC_BASE/lib64"
# required by libtbb
EXEC_LDFLAGS+=" -ldl"

PLATFORM_LDFLAGS="$LIBGCC_LIBS $GLIBC_LIBS $STDLIBS -lgcc -lstdc++"
PLATFORM_LDFLAGS+=" -B$BINUTILS"

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS $TBB_LIBS $LIBURING_LIBS $BENCHMARK_LIBS"

VALGRIND_VER="$VALGRIND_BASE/bin/"

# lua not supported because it's on track for deprecation, I think
LUA_PATH=
LUA_LIB=

export CC CXX AR AS CFLAGS CXXFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED VALGRIND_VER JEMALLOC_LIB JEMALLOC_INCLUDE CLANG_ANALYZER CLANG_SCAN_BUILD LUA_PATH LUA_LIB
