#pragma once

#include <third-party/rest_rpc/include/rest_rpc.hpp>

#include "rpc_structs.h"
namespace RPC_NAMESPACE {
class Coordinator {
 public:
  static size_t AddTaskHandler(
      rpc_conn conn, const CompactionArgs& compaction_args,
      const CompactionAdditionInfo& compaction_addition_info);

  static CompactionReply CheckTaskHandler(rpc_conn conn, const size_t& task_id);

  static std::mutex task_latch;

  static std::mutex monitor_latch;

  static std::mutex reply_latch;

  static void SubmitTaskHandler(rpc_conn conn,
                                const SubmitTaskArgs& submit_task_args);
};
}  // namespace RPC_NAMESPACE
