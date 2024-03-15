#include "coordinator.h"

#include "monitor.h"
#include "scheduler.h"

namespace RPC_NAMESPACE {

std::mutex Coordinator::task_latch;
std::mutex Coordinator::reply_latch;
std::mutex Coordinator::monitor_latch;

size_t Coordinator::AddTaskHandler(
    rpc_conn conn, const CompactionArgs& compaction_args,
    const CompactionAdditionInfo& compaction_addition_info) {
  std::lock_guard<std::mutex> lock(task_latch);
  Scheduler::task_map[Scheduler::next_task_id] = {compaction_args,
                                                  compaction_addition_info};
  Scheduler::task_queue.push(Scheduler::next_task_id);
  ++Scheduler::next_task_id;
  return Scheduler::next_task_id - 1;
}

void Coordinator::SubmitTaskHandler(rpc_conn conn,
                                    const SubmitTaskArgs& submit_task_args) {
  std::lock_guard<std::mutex> reply_lock(reply_latch);
  std::lock_guard<std::mutex> task_lock(task_latch);
  if (Scheduler::task_map.count(submit_task_args.task_id) == 0) {
    return;
  }
  if (submit_task_args.compaction_reply.code != 0) {
    std::cout << GetTime() << " " << submit_task_args.task_id << " failed"
              << std::endl;
    Scheduler::task_queue.push(submit_task_args.task_id);
    return;
  }
  std::cout << GetTime() << " " << submit_task_args.task_id << " success"
            << std::endl;
  Scheduler::task_reply_map[submit_task_args.task_id] =
      submit_task_args.compaction_reply;
  Scheduler::task_map.erase(submit_task_args.task_id);
}

CompactionReply Coordinator::CheckTaskHandler(rpc_conn conn,
                                              const size_t& task_id) {
  std::lock_guard<std::mutex> lock(reply_latch);
  if (Scheduler::task_reply_map.count(task_id) == 0) {
    std::cout << GetTime() << " Check" << task_id << " Wait " << std::endl;
    return {};
  }
  auto res = Scheduler::task_reply_map[task_id];
  Scheduler::task_reply_map.erase(task_id);
  std::cout << GetTime() << " Check" << task_id << " " << res.code << std::endl;
  return res;
}

}  // namespace RPC_NAMESPACE

int main() {
  rpc_server server(8000, 500);
  server.register_handler("AddTaskHandler",
                          RPC_NAMESPACE::Coordinator::AddTaskHandler);
  server.register_handler("CheckTaskHandler",
                          RPC_NAMESPACE::Coordinator::CheckTaskHandler);
  server.register_handler("SubmitTaskHandler",
                          RPC_NAMESPACE::Coordinator::SubmitTaskHandler);
  server.register_handler("RegisterWorkerHandler",
                          RPC_NAMESPACE::Monitor::RegisterWorkerHandler);
  std::thread scheduler(RPC_NAMESPACE::Scheduler::ConsumeTask);
  std::thread monitor(RPC_NAMESPACE::Monitor::UpdateWorkerStatus);
  server.run();
  scheduler.join();
  monitor.join();
}
