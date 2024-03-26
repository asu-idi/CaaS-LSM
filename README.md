# **CaaS-LSM: Compaction-as-a-Service for LSM-based Key-Value Stores in Storage Disaggregated Infrastructure**

## **Dependencies**

- Linux - Ubuntu
    - Prepare for the dependencies of RocksDB: [https://github.com/facebook/rocksdb/blob/main/INSTALL.md](https://github.com/facebook/rocksdb/blob/main/INSTALL.md)
    - Install and config HDFS server
    - Install gRPC

## Baselines

- Notice that multiple CSAs should bind with one Control Plane.
### **Rocks-Local**
Search the repository for this code and delete it.
```c++
tmp_options.compaction_service = std::make_shared<MyTestCompactionService>(
      dbname, compaction_options, compaction_stats, remote_listeners,
      remote_table_properties_collector_factories);
```


### **CaaS-LSM**

- Config the address of Control Plane, CSA, and HDFS server in `include/rocksdb/options.h`
- Build and compile
- Run Control Plane and CSA

```shell
cd $build
./procp_server #run Control Plane server
./csa_server #run CSA server
```


### **Disaggre-RocksDB**
- Config the address of CSA, and HDFS server in `include/rocksdb/options.h`
- Build and compile
- Run CSA

```
git checkout disaggre-rocksdb
cd $build
./csa_server # The name is the same, but the function of CSA is different with that of CaaS-LSM
```

### **Terark-Local**
- clone repo: https://github.com/bytedance/terarkdb
- Build and compile

### **Terark-Native**
- checkout branch to ```terark-native```
```
sudo apt-get install libaio-dev
```
- Before building, open ```WITH_TOOLS``` and ```WITH_TERARK_ZIP```, it's neccessary for remote compaction mode.
```
./build.sh
```
- Use ```remote_compaction_worker_101```

### **Terark-CaaS**
- Copy the code in ```db/compaction/remote_compaction``` of CaaS-LSM, including ```procp_server.cc```, ```csa_server.cc```, ```utils.h```, ```compaction_service.proto```
- Change ```CompactionArgs``` to ```string```, since TerarkDB uses encoded string in network transmit.
- Use the same way in CaaS-LSM to start.

### To evaluate the baselines
Run db_bench
```
./db_bench --benchmarks="fillrandom" --num=4000000 --statistics --threads=16 --max_background_compactions=8 --db=/xxx/xxx  --statistics
```
#### OPS comparison
![ops](https://github.com/asd1ej9h/CaaS-LSM/assets/113972303/88d00f46-e5a8-4fbc-88a0-6d4d79f5a867)

#### P99 latency comparison
![p99](https://github.com/asd1ej9h/CaaS-LSM/assets/113972303/394b4c10-476a-41c6-983e-6209236a869e)

#### Conclusion
The OPS of CaaS-LSM surpassed Disaggre-RocksDB by up to 61%, and TerarkDB-CaaS surpassed native TerarkDB up to 42%.

## Test CaaS-LSM in distributed applications

### Test Nebula
- Use the branch ```nebula```, follow the tips in https://docs.nebula-graph.io/3.2.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/1.install-nebula-graph-by-compiling-the-source-code/ 
- when compiling, replace ```build/third-party/install/include/rocksdb/``` and ```build/third-party/install/lib/librocksdb.a``` with the header files and libs produced by ```main``` branch of this repo.
- If the `main` branch cannot compile, you can try `rest_rpc` branch

### Test Kvrocks
- Clone `Kvrocks` at https://github.com/apache/incubator-kvrocks
- Before build:
    - modify this part in "cmake/rocksdb.cmake" to switch the branch of the default RocksDB to this repository
     ```
     FetchContent_DeclareGitHubWithMirror(rocksdb
        facebook/rocksdb v7.8.3
        MD5=f0cbf71b1f44ce8f50407415d38b9d44
      )
     ```

- Build: ```./x.py build```
- Single mode:
    - build/kvrocks -c kvrocks.conf 
- Cluster mode:
    - Based on ```kvrocks controller``` https://github.com/KvrocksLabs/kvrocks_controller.git with commit ```df83752849ef41ce91037ca5c9cc6c670a480d56```
    - Dependencies: etcd https://etcd.io/docs/v3.5/install/
    - Build ```kvrocks controller```: make
    - Start controller server: ```./_build/kvrocks-controller-server -c ./config/config.yaml```
    - A fast way to build cluster: ```python scripts/e2e_test.py```
    - Check cluster status: ```./_build/kvrocks-controller-cli -c ./config/kc_cli_config.yaml```
    - modify kvrocks.conf: port(e.g., 30001-30006), cluster-enabled(yes), dir /tmp/kvrocks(/tmp/kvrocks1-6)

### Evaluation Results
#### OPS of Nebula

![nebula_sche_ops](https://github.com/asd1ej9h/CaaS-LSM/assets/113972303/6efd5434-ec76-4127-86cf-7eea1fad5d20)

#### Latency of Nebula
![nebula_sche_latency](https://github.com/asd1ej9h/CaaS-LSM/assets/113972303/d9cbfd85-996b-4af8-a1f6-7607fa9ccf8d)

#### Conclusion
Nebula-Random-Sche has a total OPS of 5,669 and an average latency of 526 ms, which are about 86% lower and 6$X$ higher than Nebula-CaaS-LSM respectively.


#### OPS of Kvrocks
![kvrocks_ops_new_2](https://github.com/asd1ej9h/CaaS-LSM/assets/113972303/ab1610c6-c720-4845-91fa-5a9aa10d0d6e)

#### Latency of Kvrocks
![kvrocks_latency_new_2](https://github.com/asd1ej9h/CaaS-LSM/assets/113972303/dfb30952-bbd8-43aa-b296-19621a83edae)

#### Conclusion
With better scheduling of compaction jobs in Kvrocks-CaaS, the overall OPS is about 20% better than that of Kvrocks-Local, and the average latency improves by 30%. In the cross-datacenter scenario, according to the log file, Kvrocks-Local experiences compaction jobs piled and a severe write slowdown after intensive compaction starts. In contrast, Kvrocks-CaaS runs smoothly and improves the overall OPS by 28% and P99 latency by 65%.
