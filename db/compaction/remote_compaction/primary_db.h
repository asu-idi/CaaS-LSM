#pragma once
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rpc_structs.h"

namespace RPC_NAMESPACE {

class PrimaryDB {
 public:
  static size_t AddTask(const CompactionArgs& compaction_args,
                        const CompactionAdditionInfo& compaction_addition_info);

  static CompactionReply CheckTask(size_t task_id);

  static ROCKSDB_NAMESPACE::Status CallOpenAndCompact(
      const std::string& name, const std::string& output_directory,
      const std::string& input,
      const ROCKSDB_NAMESPACE::CompactionAdditionInfo& compaction_addition_info,
      std::string* result);
};
}  // namespace RPC_NAMESPACE
