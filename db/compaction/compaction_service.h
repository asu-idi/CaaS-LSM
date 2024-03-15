//
// Created by Qiaolin Yu on 2022/4/8.
//

#pragma once

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
class MyTestCompactionService : public CompactionService {
 public:
  MyTestCompactionService(
      std::string db_path, Options& options,
      std::shared_ptr<Statistics>& statistics,
      std::vector<std::shared_ptr<EventListener>>& listeners,
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
          table_properties_collector_factories);

  static const char* kClassName();

  const char* Name() const override;

  CompactionServiceJobStatus StartV2(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override;

  CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& info,
      std::string* compaction_service_result) override;

  CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& info,
      const CompactionAdditionInfo& compaction_addition_info,
      std::string* compaction_service_result) override;

  int GetCompactionNum();

  CompactionServiceJobInfo GetCompactionInfoForStart();

  CompactionServiceJobInfo GetCompactionInfoForWait();

  void OverrideStartStatus(CompactionServiceJobStatus s);

  void OverrideWaitStatus(CompactionServiceJobStatus s);

  void OverrideWaitResult(std::string str);

  void ResetOverride();

  void SetCanceled(bool canceled);

 private:
  InstrumentedMutex mutex_;
  std::atomic_int compaction_num_{0};
  std::map<uint64_t, std::string> jobs_;
  const std::string db_path_;
  Options options_;
  std::shared_ptr<Statistics> statistics_;
  CompactionServiceJobInfo start_info_;
  CompactionServiceJobInfo wait_info_;
  bool is_override_start_status_ = false;
  CompactionServiceJobStatus override_start_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_status_ = false;
  CompactionServiceJobStatus override_wait_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_result_ = false;
  std::string override_wait_result_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories_;
  std::atomic_bool canceled_{false};
};
}  // namespace ROCKSDB_NAMESPACE