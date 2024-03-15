#pragma once
#include <queue>

#include "rpc_structs.h"

namespace RPC_NAMESPACE {

class Scheduler {
 public:
  static HostAddress ScheduleWorker(
      const CompactionAdditionInfo &compaction_addition_info);

  [[noreturn]] static void ConsumeTask();

  static std::unordered_map<size_t, CompactionJobInfo> task_map;

  static std::size_t next_task_id;

  static std::unordered_map<size_t, CompactionReply> task_reply_map;

  // TODO: avoid starvation
  struct TaskCmp {
    bool operator()(const size_t &task1, const size_t &task2) const {
      if (Scheduler::task_map[task1].compaction_addition_info.start_level !=
          Scheduler::task_map[task2].compaction_addition_info.start_level) {
        return Scheduler::task_map[task1].compaction_addition_info.start_level >
               Scheduler::task_map[task2].compaction_addition_info.start_level;
      }
      if (Scheduler::task_map[task1].compaction_addition_info.score < 0 &&
          Scheduler::task_map[task2].compaction_addition_info.score < 0) {
        return task1 > task2;
      }
      if (Scheduler::task_map[task1].compaction_addition_info.score < 0 ||
          Scheduler::task_map[task2].compaction_addition_info.score < 0) {
        return Scheduler::task_map[task1].compaction_addition_info.score >
               Scheduler::task_map[task2].compaction_addition_info.score;
      }
      if (Scheduler::task_map[task1].compaction_addition_info.score !=
          Scheduler::task_map[task2].compaction_addition_info.score) {
        return Scheduler::task_map[task1].compaction_addition_info.score <
               Scheduler::task_map[task2].compaction_addition_info.score;
      }
      return task1 > task2;
    }
  };

  static std::priority_queue<size_t, std::vector<size_t>, TaskCmp> task_queue;
};

}  // namespace RPC_NAMESPACE
