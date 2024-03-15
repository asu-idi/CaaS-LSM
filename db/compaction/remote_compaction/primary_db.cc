#include "primary_db.h"

#include <iostream>
#include <third-party/rest_rpc/include/rest_rpc.hpp>

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rpc_config.h"
#include "rpc_structs.h"

namespace RPC_NAMESPACE {

size_t PrimaryDB::AddTask(
    const CompactionArgs& compaction_args,
    const CompactionAdditionInfo& compaction_addition_info) {
  rest_rpc::rpc_client client(RPC_NAMESPACE::coordinator_ip,
                              RPC_NAMESPACE::coordinator_port);
  bool r = client.connect();
  if (!r) {
    std::cout << "connect timeout" << std::endl;
    throw std::exception();
  }
  auto task_id = client.call<1000000000, size_t>(
      "AddTaskHandler", compaction_args, compaction_addition_info);
  return task_id;
}

CompactionReply PrimaryDB::CheckTask(size_t task_id) {
  rest_rpc::rpc_client client(RPC_NAMESPACE::coordinator_ip,
                              RPC_NAMESPACE::coordinator_port);
  bool r = client.connect();
  if (!r) {
    std::cout << "connect timeout" << std::endl;
    throw std::exception();
  }
  auto compaction_reply =
      client.call<1000000000, CompactionReply>("CheckTaskHandler", task_id);
  return compaction_reply;
}

ROCKSDB_NAMESPACE::Status PrimaryDB::CallOpenAndCompact(
    const std::string& name, const std::string& output_directory,
    const std::string& input,
    const ROCKSDB_NAMESPACE::CompactionAdditionInfo& compaction_addition_info,
    std::string* result) {
  CompactionArgs compaction_args(name, output_directory, input);
  RPC_NAMESPACE::CompactionAdditionInfo compaction_addition_info_rpc{
      compaction_addition_info.score,
      compaction_addition_info.num_entries,
      compaction_addition_info.num_deletions,
      compaction_addition_info.compensated_file_size,
      compaction_addition_info.output_level,
      compaction_addition_info.start_level};
  try {
    auto task_id = AddTask(compaction_args, compaction_addition_info_rpc);
    CompactionReply compaction_reply;
    do {
      sleep(RPC_NAMESPACE::check_time_interval);
      compaction_reply = CheckTask(task_id);
    } while (compaction_reply.code == 99);
    if (compaction_reply.code != 0) {
      return ROCKSDB_NAMESPACE::Status::Aborted();
    }
    *result = compaction_reply.result;
    return ROCKSDB_NAMESPACE::Status::OK();
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
    return ROCKSDB_NAMESPACE::Status::IOError();
  }
}
}  // namespace RPC_NAMESPACE