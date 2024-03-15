hdfs_SOURCES = env_hdfs.cc env_hdfs_impl.cc
hdfs_HEADERS = env_hdfs.h
hdfs_CXXFLAGS = -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I${HADOOP_HOME}/include
hdfs_LDFLAGS = -lhdfs -u hdfs_reg -L${JAVA_HOME}/jre/lib/amd64 -L${HADOOP_HOME}/lib/native -L${JAVA_HOME}/jre/lib/amd64/server -ldl -lverify -ljava -ljvm
hdfs_FUNC = register_HdfsObjects
hdfs_JNI_NATIVE_SOURCES = java/rocksjni/env_hdfs.cc
hdfs_NATIVE_JAVA_CLASSES = org.rocksdb.HdfsEnv
hdfs_JAVA_TESTS = org.rocksdb.HdfsEnvTest
