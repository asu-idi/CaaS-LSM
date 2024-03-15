#pragma once

#include <unordered_map>

#include "rpc_structs.h"

namespace RPC_NAMESPACE {

class Monitor {
 public:
  [[noreturn]] static void UpdateWorkerStatus();
  static void UpdateOneWorkerStatus(const std::string& ip,
                                    const uint32_t& port);

  static void RegisterWorkerHandler(rpc_conn conn,
                                    const WorkerStatus& worker_status);

  static std::unordered_map<std::string, WorkerStatus> worker_status_map;

  static std::unordered_map<std::string, std::vector<size_t>> worker_task_list;
};
}  // namespace RPC_NAMESPACE
