#pragma once
#include <rest_rpc.hpp>
#include <utility>

#include "rocksdb/status.h"

namespace RPC_NAMESPACE {

struct CompactionAdditionInfo {
  double score;
  uint64_t num_entries;
  uint64_t num_deletions;
  uint64_t compensated_file_size;
  int output_level;
  int start_level;
  CompactionAdditionInfo(double _score, uint64_t _num_entries,
                         uint64_t _num_deletions,
                         uint64_t _compensated_file_size, int _output_level,
                         int _start_level)
      : score(_score),
        num_entries(_num_entries),
        num_deletions(_num_deletions),
        compensated_file_size(_compensated_file_size),
        output_level(_output_level),
        start_level(_start_level) {}
  CompactionAdditionInfo() = default;
  MSGPACK_DEFINE(score, num_entries, num_deletions, compensated_file_size,
                 output_level, start_level);
};

struct CompactionReply {
  int code = 99;
  std::string result;
  CompactionReply(int _code, std::string _result)
      : code(_code), result(std::move(_result)) {}
  CompactionReply() = default;
  MSGPACK_DEFINE(code, result);
};

struct CompactionArgs {
  std::string name;
  std::string output_directory;
  std::string input;
  CompactionArgs(std::string _name, std::string _output_directory,
                 std::string _input)
      : name(std::move(_name)),
        output_directory(std::move(_output_directory)),
        input(std::move(_input)) {}
  CompactionArgs() = default;
  MSGPACK_DEFINE(name, output_directory, input);
};

struct CompactTaskArgs {
  size_t task_id{};
  CompactionArgs compaction_args;
  CompactTaskArgs(size_t _task_id, CompactionArgs _compaction_args)
      : task_id(_task_id), compaction_args(std::move(_compaction_args)) {}
  CompactTaskArgs() = default;
  MSGPACK_DEFINE(task_id, compaction_args);
};

struct SubmitTaskArgs {
  size_t task_id{};
  CompactionReply compaction_reply;
  SubmitTaskArgs(size_t _task_id, CompactionReply _compaction_reply)
      : task_id(_task_id), compaction_reply(std::move(_compaction_reply)) {}
  SubmitTaskArgs() = default;
  MSGPACK_DEFINE(task_id, compaction_reply);
};

struct HostAddress {
  std::string ip;
  uint32_t port;
};

inline std::string AddressToKey(const std::string& ip, uint32_t port) {
  return (ip + ":" + std::to_string(port));
}

inline HostAddress KeyToAddress(const std::string& key) {
  size_t position = key.find(':');
  std::string ip = key.substr(0, position);
  uint32_t port =
      std::stoi(key.substr(position + 1, key.length() - position - 1));
  return {ip, port};
}

inline char* GetTime() {
  time_t now = time(0);

  char* dt = ctime(&now);

  tm* gmtm = gmtime(&now);
  dt = asctime(gmtm);
  return dt;
}

struct WorkerStatus {
  std::string ip;
  uint32_t port;
  size_t local_task_nums;
  size_t max_task_nums;
  double memory_usage;
  MSGPACK_DEFINE(ip, port, local_task_nums, max_task_nums, memory_usage);
};

struct CompactionJobInfo {
  CompactionArgs compaction_args;
  CompactionAdditionInfo compaction_addition_info{};
};

}  // namespace RPC_NAMESPACE