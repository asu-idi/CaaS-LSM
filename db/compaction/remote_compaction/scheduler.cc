#include "scheduler.h"

#include "coordinator.h"
#include "job_distributor.h"
#include "monitor.h"

namespace RPC_NAMESPACE {
std::unordered_map<size_t, CompactionJobInfo> Scheduler::task_map;
std::unordered_map<size_t, CompactionReply> Scheduler::task_reply_map;
std::size_t Scheduler::next_task_id = 0;
std::priority_queue<size_t, std::vector<size_t>, Scheduler::TaskCmp>
    Scheduler::task_queue;

HostAddress Scheduler::ScheduleWorker(
    const CompactionAdditionInfo& compaction_addition_info) {
  std::lock_guard<std::mutex> lock(Coordinator::monitor_latch);
  if (Monitor::worker_status_map.empty()) {
    return {};
  }
  size_t min_task_nums =
      Monitor::worker_status_map.begin()->second.local_task_nums;
  auto best_worker = Monitor::worker_status_map.begin()->first;
  for (const auto& iter : Monitor::worker_status_map) {
    if (iter.second.local_task_nums < min_task_nums &&
        iter.second.local_task_nums < iter.second.max_task_nums) {
      best_worker = iter.first;
      min_task_nums = iter.second.local_task_nums;
    }
  }
  if (Monitor::worker_status_map[best_worker].local_task_nums >=
          Monitor::worker_status_map[best_worker].max_task_nums ||
      Monitor::worker_status_map[best_worker].memory_usage > 0.95) {
    return {};
  }
  return KeyToAddress(best_worker);
}

[[noreturn]] void Scheduler::ConsumeTask() {
  while (true) {
    Coordinator::task_latch.lock();
    if (task_queue.empty()) {
      Coordinator::task_latch.unlock();
      std::cout << GetTime() << " No task" << std::endl;
      sleep(1);
      continue;
    }
    auto task_id = task_queue.top();
    task_queue.pop();
    std::cout << GetTime() << " Schedule " << task_id << std::endl;
    auto compaction_job_info = task_map[task_id];
    if (Monitor::worker_status_map.empty()) {
      CompactionReply compactionReply;
      compactionReply.code = 1;
      Scheduler::task_reply_map[task_id] = compactionReply;
      Scheduler::task_map.erase(task_id);
      Coordinator::task_latch.unlock();
      std::cout << GetTime() << " Fallback " << task_id << std::endl;
      continue;
    }
    auto worker_address =
        ScheduleWorker(compaction_job_info.compaction_addition_info);
    if (worker_address.ip.empty()) {
      std::cout << GetTime() << " Workers are all busy, reschedule " << task_id
                << std::endl;
      task_queue.push(task_id);
      Coordinator::task_latch.unlock();
      sleep(1);
      continue;
    }
    auto key = AddressToKey(worker_address.ip, worker_address.port);
    if (Monitor::worker_task_list.count(key) == 0) {
      std::vector<size_t> temp;
      Monitor::worker_task_list[key] = temp;
    }
    Monitor::worker_task_list[key].emplace_back(task_id);
    Coordinator::task_latch.unlock();
    Coordinator::monitor_latch.lock();
    std::cout << GetTime() << " memory usage is "
              << Monitor::worker_status_map[key].memory_usage << std::endl;
    Monitor::worker_status_map[key].local_task_nums++;
    Coordinator::monitor_latch.unlock();
    CompactionReply compactionReply;
    if (worker_address.ip.empty() || worker_address.port == 0) {
      Coordinator::task_latch.lock();
      task_queue.push(task_id);
      Coordinator::task_latch.unlock();
    } else {
      CompactTaskArgs compact_task_args = {task_id,
                                           compaction_job_info.compaction_args};
      try {
        JobDistributor::DistributeCompactionJob(compact_task_args,
                                                worker_address);
      } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
        Coordinator::task_latch.lock();
        task_queue.push(task_id);
        Coordinator::task_latch.unlock();
        continue;
      }
    }
    Coordinator::monitor_latch.lock();
    Coordinator::monitor_latch.unlock();
  }
}
}  // namespace RPC_NAMESPACE
