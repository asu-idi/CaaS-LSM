#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <string>

#include "csa.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#ifdef HDFS
#include "plugin/hdfs/env_hdfs.h"
#endif

ROCKSDB_NAMESPACE::OpenAndCompactOptions compaction_service_options;

std::atomic<int64_t> local_task_nums_ = std::atomic<int64_t>(0);
int64_t max_task_nums_ = compaction_service_options.csa_max_concurrent_tasks;

class CSAImpl final : public csa::CSAService::Service {
 public:
  grpc::Status ExecuteCompactionTask(
      grpc::ServerContext* context, const csa::CompactionArgs* compaction_args,
      csa::CompactionReply* compaction_reply) override {
    local_task_nums_ += 1;
    while (local_task_nums_ > static_cast<int>(max_task_nums_)) {
      std::cout << "Wait" << std::endl;
      sleep(1);
    }
    std::cout << "CSA ExecuteCompactionTask() currency: " << local_task_nums_
              << std::endl;
    std::string compaction_service_result;
    rocksdb::CompactionServiceOptionsOverride options_override;
    rocksdb::OpenAndCompactOptions open_and_compact_options;
    ROCKSDB_NAMESPACE::Options options_;
#ifdef HDFS
    std::unique_ptr<rocksdb::Env> hdfs;
    rocksdb::NewHdfsEnv(open_and_compact_options.hdfs_address, &hdfs);
    options_override.env = hdfs.get();
#endif
    options_override.file_checksum_gen_factory =
        options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
        options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory = options_.sst_partitioner_factory;
    options_override.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    rocksdb::Status s = ROCKSDB_NAMESPACE::DB::OpenAndCompact(
        compaction_args->name(), compaction_args->output_directory(),
        compaction_args->input(), &compaction_service_result, options_override);
    compaction_reply->set_code(s.code());
    compaction_reply->set_result(std::move(compaction_service_result));
    local_task_nums_ -= 1;
    return ::grpc::Status::OK;
  }
};

int main() {
  std::string server_address(compaction_service_options.csa_address);
  CSAImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}