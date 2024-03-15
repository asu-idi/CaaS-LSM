#pragma once
#include <string>

#include "resource_utils.h"
#include "rpc_structs.h"

namespace RPC_NAMESPACE {

class WorkerAgent {
 public:
  static void WorkerAgentHandler(rpc_conn conn,
                                 const CompactTaskArgs& compact_task_args);

  static void RegisterWorker();

  static WorkerStatus CheckWorkerStatusHandler(rpc_conn conn);

  static std::string ip_;

  static uint32_t port_;

  static std::atomic<size_t> local_task_nums_;

  static size_t max_task_nums_;
};
}  // namespace RPC_NAMESPACE