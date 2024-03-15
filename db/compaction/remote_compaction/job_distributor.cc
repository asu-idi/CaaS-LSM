#include "job_distributor.h"

namespace RPC_NAMESPACE {

void JobDistributor::DistributeCompactionJob(
    const RPC_NAMESPACE::CompactTaskArgs& compact_task_args,
    const RPC_NAMESPACE::HostAddress& host_address) {
  try {
    rest_rpc::rpc_client client(host_address.ip, host_address.port);
    bool r = client.connect();
    if (!r) {
      std::cout << "connect timeout" << std::endl;
      throw std::exception();
    }
    client.call<1000000000>("WorkerAgentHandler", compact_task_args);
    std::cout << GetTime() << " "
              << "Distribute " << compact_task_args.task_id << " to "
              << host_address.ip << ":" << host_address.port << std::endl;
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }
}

}  // namespace RPC_NAMESPACE
