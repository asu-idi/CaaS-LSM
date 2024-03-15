cmake_minimum_required(VERSION 3.4)

# Native Hadoop library is required, and according to
# https://hadoop.apache.org/docs/r3.3.1/hadoop-project-dist/hadoop-common/NativeLibraries.html,
# Windows is not supported.

set(hdfs_SOURCES "env_hdfs.cc" "env_hdfs_impl.cc" PARENT_SCOPE)
set(hdfs_LIBS "hdfs" "dl" "verify" "java" "jvm" PARENT_SCOPE)
set(hdfs_INCLUDE_PATHS "$ENV{JAVA_HOME}/include" "$ENV{JAVA_HOME}/include/linux" "$ENV{HADOOP_HOME}/include" PARENT_SCOPE)
set(hdfs_LINK_PATHS "$ENV{JAVA_HOME}/jre/lib/amd64/server" "$ENV{JAVA_HOME}/jre/lib/amd64" "$ENV{HADOOP_HOME}/lib/native" PARENT_SCOPE)
set(hdfs_CMAKE_EXE_LINKER_FLAGS "-u hdfs_reg" PARENT_SCOPE)
