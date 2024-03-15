#include <grpc/grpc.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <string>
#include <thread>

#include "compaction_service.grpc.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "utils.h"

#ifdef HDFS
#include "plugin/hdfs/env_hdfs.h"
#endif

ROCKSDB_NAMESPACE::OpenAndCompactOptions compaction_service_options;

class CSAImpl final : public compactionservice::CSAService::Service {
 public:
  grpc::Status ExecuteCompactionTask(
      grpc::ServerContext* context,
      const compactionservice::CompactionTaskArgs* request,
      google::protobuf::Empty* response) override {
    local_task_nums_ += 1;
    std::cout << GetTime() << "Receive compaction task (" << request->task_id()
              << ") success " << std::endl;
    compactionservice::CompactionTaskArgs compaction_task_args = *request;
    std::thread thd1([compaction_task_args, this] {
      while (local_task_nums_ > static_cast<int>(max_task_nums_)) {
        std::cout << "Wait" << std::endl;
        sleep(1);
      }
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
      options_override.sst_partitioner_factory =
          options_.sst_partitioner_factory;
      options_override.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      const compactionservice::CompactionArgs& compaction_args =
          compaction_task_args.compaction_args();
      uint64_t start_time;
      uint64_t open_db_latency;
      rocksdb::Status s = ROCKSDB_NAMESPACE::DB::OpenAndCompact(
          compaction_args.name(), compaction_args.output_directory(),
          compaction_args.input(), &compaction_service_result, &start_time,
          &open_db_latency, options_override);
      compactionservice::CompactionReply compaction_reply;
      uint64_t process_latency =
          start_time - compaction_task_args.compaction_args().trigger_ms();
      compaction_reply.set_process_latency(process_latency);
      compaction_reply.set_code(s.code());
      compaction_reply.set_open_db_latency(open_db_latency);
      if (s.ok()) {
        std::cout << GetTime() << "Compaction task ("
                  << compaction_task_args.task_id() << ") finish" << std::endl;
      } else {
        std::cout << GetTime() << "Compaction task ("
                  << compaction_task_args.task_id() << ") failed" << std::endl;
      }
      compaction_reply.set_result(std::move(compaction_service_result));
      local_task_nums_--;
      grpc::ClientContext context2;
      compactionservice::SubmitTaskArgs submit_task_args;
      submit_task_args.mutable_compaction_reply()->CopyFrom(compaction_reply);
      submit_task_args.set_task_id(compaction_task_args.task_id());
      google::protobuf::Empty response2;
      grpc::Status status =
          stub_->SubmitTask(&context2, submit_task_args, &response2);
      if (!status.ok()) {
        std::cout << GetTime() << "Submit compaction task ("
                  << submit_task_args.task_id() << ") failed" << std::endl;
      }
    });
    thd1.detach();
    return grpc::Status::OK;
  }

  grpc::Status CheckCSAStatus(
      grpc::ServerContext* context, const google::protobuf::Empty* request,
      compactionservice::CSAStatus* csa_status) override {
    csa_status->set_address(address_);
    csa_status->set_local_task_nums(local_task_nums_);
    csa_status->set_max_task_nums(max_task_nums_);
    csa_status->set_memory_usage(get_memoccupy());
    return ::grpc::Status::OK;
  }

  void RegisterCSA() {
    compactionservice::CSAStatus csa_status;
    csa_status.set_address(address_);
    csa_status.set_local_task_nums(local_task_nums_);
    csa_status.set_max_task_nums(max_task_nums_);
    csa_status.set_memory_usage(get_memoccupy());
    grpc::ClientContext context;
    google::protobuf::Empty response;
    grpc::Status status = stub_->RegisterCSA(&context, csa_status, &response);
    if (!status.ok()) {
      std::cout << GetTime() << "RegisterCSA failed" << std::endl;
    } else {
      std::cout << GetTime() << "RegisterCSA success" << std::endl;
    }
  }

  std::unique_ptr<compactionservice::ProCPService::Stub> stub_;

 private:
  std::atomic<int64_t> local_task_nums_ = std::atomic<int64_t>(0);
  int64_t max_task_nums_ = compaction_service_options.csa_max_concurrent_tasks;
  std::string address_ = compaction_service_options.csa_address;
};

int main() {
  std::string server_address(compaction_service_options.csa_address);
  CSAImpl service;
  service.stub_ = compactionservice::ProCPService::NewStub(
      grpc::CreateChannel(compaction_service_options.pro_cp_address,
                          grpc::InsecureChannelCredentials()));

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << GetTime() << "Server listening on " << server_address
            << std::endl;
  service.RegisterCSA();
  server->Wait();
}