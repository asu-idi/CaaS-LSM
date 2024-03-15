//
// Created by leipeng on 2019-09-26.
//

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <rocksdb/db.h>

#include <iostream>
#include <sstream>
#include <terark/util/linebuf.hpp>
#include <thread>

#include "compaction_service.pb.h"
#include "rocksdb/compaction_dispatcher.h"

class MyWorker : public TERARKDB_NAMESPACE::RemoteCompactionDispatcher::Worker {
  std::string GenerateOutputFileName(size_t file_index) override {
    // make a file name
    std::ostringstream oss;
    oss << "Worker-" << std::this_thread::get_id() << "-" << file_index;
    return oss.str();
  }

 public:
  using TERARKDB_NAMESPACE::RemoteCompactionDispatcher::Worker::Worker;
  std::string name() const { return "MyWorker"; }
};

class CSAImpl final : public compactionservice::CSAService::Service {
 public:
  grpc::Status ExecuteCompactionTask(
      grpc::ServerContext* context,
      const compactionservice::CompactionArgs* compaction_args,
      compactionservice::CompactionReply* compaction_reply) override {
    std::cout << "ExecuteCompactionTask" << std::endl;
    std::string result = worker_.DoCompaction(compaction_args->input());

    worker_.DebugSerializeCheckResult(result);
    compaction_reply->set_result(result);
    std::cout << "set_result done" << std::endl;
    return grpc::Status::OK;
  }

  MyWorker worker_;
};

int main() {
  TERARKDB_NAMESPACE::EnvOptions env_options;

  // MyWorker worker(env_options, TERARKDB_NAMESPACE::Env::Default());

  // // worker.RegistComparator(const Comparator*);
  // // worker.RegistPrefixExtractor(std::shared_ptr<const SliceTransform>);
  // // worker.RegistTableFactory(const char* Name, CreateTableFactoryCallback);
  // // worker.RegistMergeOperator(CreateMergeOperatorCallback);
  // // worker.RegistCompactionFilter(const CompactionFilter*);
  // // worker.RegistCompactionFilterFactory(
  // //    std::shared_ptr<CompactionFilterFactory>);
  // // worker.RegistTablePropertiesCollectorFactory(
  // //    std::shared_ptr<TablePropertiesCollectorFactory>);

  // terark::LineBuf buf;
  // buf.read_all(stdin);
  // std::cout << worker.DoCompaction(TERARKDB_NAMESPACE::Slice(buf.p, buf.n));

  CSAImpl service;
  std::string server_address("127.0.0.1:8010");
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}

// a shell script calling this program:
// ----------------------------------------------
// env TerarkZipTable_localTempDir=/tmp remote_compaction_worker_101
// ----------------------------------------------
