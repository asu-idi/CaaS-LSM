#pragma once
#include "rocksdb/status.h"
#include "rpc_structs.h"

namespace RPC_NAMESPACE {
class JobDistributor {
 public:
  static void DistributeCompactionJob(const CompactTaskArgs& compact_task_args,
                                      const HostAddress& host_address);
};
}  // namespace RPC_NAMESPACE