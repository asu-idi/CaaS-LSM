This directory contains the hdfs extensions needed to make rocksdb store
files in HDFS.

The configuration assumes that packages libhdfs0, libhdfs0-dev are
installed which basically means that hdfs.h is $HADOOP_HOME/include.

The env_hdfs.[cc,h] define a factory function NewHdfsEnv(...) that creates
a HdfsEnv object given a string-baed uri representing the uri of the HDFS.

The env_hdfs_impl.[cc,h] files define the HdfsEnv needed to talk to an
underlying HDFS.

# Build
If you want to compile rocksdb with hdfs support, please set $JAVA_HOME and
$HADOOP_HOME accordingly. On my devserver, I have
```
$ echo $JAVA_HOME
/usr/local/fb-jdk-8/
$ echo $HADOOP_HOME
/home/<username>/hadoop-3.3.1/
```
The hadoop-3.3.1 is the directory resulting from untaring a standard hadoop-3.3.1.tar.gz
downloaded from https://hadoop.apache.org/releases.html.

Next, run the commands listed in setup.sh:
```
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:$HADOOP_HOME/lib/native

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
for f in `find $HADOOP_HOME/share/hadoop/hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find $HADOOP_HOME/share/hadoop | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find $HADOOP_HOME/share/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
```

The code first needs to be linked under RocksDB's "plugin/" directory. In your
RocksDB directory, run:
```
$ pushd ./plugin/
$ git clone https://github.com/riversand963/rocksdb-hdfs-env.git hdfs
```
Note that please do not clone it into a directory whose name contains `-`,
otherwise the Makefile won't pick it up. In this example, we clone it into
a directory called 'hdfs'.

Next, we can build and install RocksDB with this plugin as follows:
```
$ popd
$ make clean && DEBUG_LEVEL=0 ROCKSDB_PLUGINS="hdfs" make -j48 db_bench db_stress install
```

# Tool usage
For RocksDB binaries, e.g. db_bench and db_stress we built earlier, the plugin
can be enabled through configuration. Db_bench and db_stress in particular
takes a -env_uri where we can specify a HDFS. For example
```
$ ./db_stress -env_uri=hdfs://localhost:9000/ -compression_type=none
$ ./db_bench -benchmarks=fillrandom -env_uri=hdfs://localhost:9000/ --compression_type=none
```
