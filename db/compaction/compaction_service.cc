#ifndef ROCKSDB_LITE

#include "db/compaction/compaction_service.h"

#include "db/compaction/remote_compaction/primary_db.h"

namespace ROCKSDB_NAMESPACE {

MyTestCompactionService::MyTestCompactionService(
    std::string db_path, Options& options,
    std::shared_ptr<Statistics>& statistics,
    std::vector<std::shared_ptr<EventListener>>& listeners,
    std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
        table_properties_collector_factories)
    : db_path_(std::move(db_path)),
      options_(options),
      statistics_(statistics),
      start_info_("na", "na", "na", 0, Env::TOTAL),
      wait_info_("na", "na", "na", 0, Env::TOTAL),
      listeners_(listeners),
      table_properties_collector_factories_(
          std::move(table_properties_collector_factories)) {}

const char* MyTestCompactionService::kClassName() {
  return "MyTestCompactionService";
}

const char* MyTestCompactionService::Name() const { return kClassName(); }

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
  return CompactionServiceJobStatus::kUseLocal;
}

CompactionServiceJobStatus MyTestCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    const CompactionAdditionInfo& compaction_addition_info,
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

  //      Status s = DB::OpenAndCompact(
  //          db_path_, db_path_ + "/" +
  //          ROCKSDB_NAMESPACE::to_string(info.job_id), compaction_input,
  //          compaction_service_result, options_override);
  //  if (compaction_addition_info.start_level == 0) {
  //    return CompactionServiceJobStatus::kUseLocal;
  //  }

  Status s = RPC_NAMESPACE::PrimaryDB::CallOpenAndCompact(
      db_path_, db_path_ + "/" + std::to_string(info.job_id), compaction_input,
      compaction_addition_info, compaction_service_result);

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

int MyTestCompactionService::GetCompactionNum() {
  return compaction_num_.load();
}

CompactionServiceJobInfo MyTestCompactionService::GetCompactionInfoForStart() {
  return start_info_;
}
CompactionServiceJobInfo MyTestCompactionService::GetCompactionInfoForWait() {
  return wait_info_;
}

void MyTestCompactionService::OverrideStartStatus(
    CompactionServiceJobStatus s) {
  is_override_start_status_ = true;
  override_start_status_ = s;
}

void MyTestCompactionService::OverrideWaitStatus(CompactionServiceJobStatus s) {
  is_override_wait_status_ = true;
  override_wait_status_ = s;
}

void MyTestCompactionService::OverrideWaitResult(std::string str) {
  is_override_wait_result_ = true;
  override_wait_result_ = std::move(str);
}

void MyTestCompactionService::ResetOverride() {
  is_override_wait_result_ = false;
  is_override_start_status_ = false;
  is_override_wait_status_ = false;
}

void MyTestCompactionService::SetCanceled(bool canceled) {
  canceled_ = canceled;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE