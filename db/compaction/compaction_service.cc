#include "compaction_service.h"

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "csa.grpc.pb.h"
#include "rocksdb/db.h"

class CSAClient {
 public:
  CSAClient(const std::shared_ptr<grpc::Channel>& channel)
      : stub_(csa::CSAService::NewStub(channel)) {}

  ROCKSDB_NAMESPACE::Status OpenAndCompact(
      const ROCKSDB_NAMESPACE::OpenAndCompactOptions& options,
      const std::string& name, const std::string& output_directory,
      const std::string& input, std::string* output,
      const ROCKSDB_NAMESPACE::CompactionServiceOptionsOverride&
          override_options) {
    csa::CompactionArgs compaction_args;
    compaction_args.set_name(name);
    compaction_args.set_output_directory(output_directory);
    compaction_args.set_input(input);
    csa::CompactionReply compaction_reply;
    grpc::ClientContext context;
    grpc::Status status = stub_->ExecuteCompactionTask(
        &context, compaction_args, &compaction_reply);
    output->assign(compaction_reply.result());
    if (status.ok()) {
      return ROCKSDB_NAMESPACE::Status::OK();
    } else {
      return ROCKSDB_NAMESPACE::Status::IOError(status.error_message());
    }
  };

 private:
  std::unique_ptr<csa::CSAService::Stub> stub_;
};

namespace ROCKSDB_NAMESPACE {
CompactionServiceJobStatus MyTestCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  InstrumentedMutexLock l(&mutex_);
  start_info_ = info;
  assert(info.db_name == db_path_);
  jobs_.emplace(info.job_id, compaction_service_input);
  CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
  if (is_override_start_status_) {
    return override_start_status_;
  }
  return s;
}

CompactionServiceJobStatus MyTestCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) {
  std::string compaction_input;
  assert(info.db_name == db_path_);
  {
    InstrumentedMutexLock l(&mutex_);
    wait_info_ = info;
    auto i = jobs_.find(info.job_id);
    if (i == jobs_.end()) {
      return CompactionServiceJobStatus::kFailure;
    }
    compaction_input = std::move(i->second);
    jobs_.erase(i);
  }

  if (is_override_wait_status_) {
    return override_wait_status_;
  }

  CompactionServiceOptionsOverride options_override;
  options_override.env = options_.env;
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
  options_override.statistics = statistics_;
  if (!listeners_.empty()) {
    options_override.listeners = listeners_;
  }

  if (!table_properties_collector_factories_.empty()) {
    options_override.table_properties_collector_factories =
        table_properties_collector_factories_;
  }

  OpenAndCompactOptions options;
  options.canceled = &canceled_;

  //  Status s = DB::OpenAndCompact(
  //      options, db_path_, db_path_ + "/" + std::to_string(info.job_id),
  //      compaction_input, compaction_service_result, options_override);
  CSAClient csa_client(grpc::CreateChannel(options.csa_address,
                                           grpc::InsecureChannelCredentials()));
  Status s = csa_client.OpenAndCompact(
      options, db_path_, db_path_ + "/" + std::to_string(info.job_id),
      compaction_input, compaction_service_result, options_override);

  if (is_override_wait_result_) {
    *compaction_service_result = override_wait_result_;
  }
  compaction_num_.fetch_add(1);
  if (s.ok()) {
    return CompactionServiceJobStatus::kSuccess;
  } else {
    return CompactionServiceJobStatus::kUseLocal;
  }
}
};  // namespace ROCKSDB_NAMESPACE
