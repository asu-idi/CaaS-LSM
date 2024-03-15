#include "monitor.h"

#include "coordinator.h"
#include "scheduler.h"

namespace RPC_NAMESPACE {

std::unordered_map<std::string, WorkerStatus> Monitor::worker_status_map;

std::unordered_map<std::string, std::vector<size_t>> Monitor::worker_task_list;

void Monitor::RegisterWorkerHandler(rpc_conn conn,
                                    const WorkerStatus& worker_status) {
  std::string worker_key =
      RPC_NAMESPACE::AddressToKey(worker_status.ip, worker_status.port);
  Monitor::worker_status_map[worker_key] = worker_status;
}

[[noreturn]] void Monitor::UpdateWorkerStatus() {
  while (true) {
    sleep(5);
    for (const auto& iter : worker_status_map) {
      UpdateOneWorkerStatus(iter.second.ip, iter.second.port);
    }
  }
}

void Monitor::UpdateOneWorkerStatus(const std::string& ip,
                                    const uint32_t& port) {
  try {
    if (port == 0) {
      throw std::exception();
    }
    rest_rpc::rpc_client client(ip, port);
    bool r = client.connect();
    if (!r) {
      std::cout << GetTime() << " "
                << "Worker " << ip << " has failed" << std::endl;
      std::string key = RPC_NAMESPACE::AddressToKey(ip, port);
      Coordinator::task_latch.lock();
      worker_status_map.erase(key);
      auto task_list = worker_task_list[key];
      for (size_t i : task_list) {
        if (Scheduler::task_map.count(i) > 0) {
          Scheduler::task_queue.push(i);
          std::cout << GetTime() << " Reschedule " << i << std::endl;
          std::cout << GetTime() << " task queue size "
                    << Scheduler::task_queue.size() << std::endl;
        }
      }
      worker_task_list.erase(key);
      Coordinator::task_latch.unlock();
      throw std::exception();
    }
    auto worker_status =
        client.call<1000000000, WorkerStatus>("UpdateWorkerStatusHandler");
    std::string worker_key = RPC_NAMESPACE::AddressToKey(ip, port);
    Coordinator::monitor_latch.lock();
    Monitor::worker_status_map[worker_key] = worker_status;
    Coordinator::monitor_latch.unlock();
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }
}

}  // namespace RPC_NAMESPACE