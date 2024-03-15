#include "worker_agent.h"

#include <third-party/rest_rpc/include/rest_rpc.hpp>

#include "plugin/hdfs/env_hdfs.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rpc_config.h"

namespace RPC_NAMESPACE {
std::string WorkerAgent::ip_ = RPC_NAMESPACE::worker_agent_ip;
uint32_t WorkerAgent::port_ = RPC_NAMESPACE::worker_agent_port;
std::atomic<size_t> WorkerAgent::local_task_nums_ = std::atomic<size_t>(0);
// size_t WorkerAgent::max_task_nums_ = std::thread::hardware_concurrency();
size_t WorkerAgent::max_task_nums_ = 5;
bool isDone = false;

void WorkerAgent::RegisterWorker() {
  rest_rpc::rpc_client client(RPC_NAMESPACE::coordinator_ip,
                              RPC_NAMESPACE::coordinator_port);
  WorkerStatus worker_status;
  worker_status.ip = WorkerAgent::ip_;
  worker_status.port = WorkerAgent::port_;
  worker_status.local_task_nums = local_task_nums_;
  bool r = client.connect();
  if (!r) {
    std::cout << "Worker agent register timeout" << std::endl;
    throw std::exception();
  }
  client.call<1000000000, CompactionReply>("RegisterWorkerHandler",
                                           worker_status);
  std::cout << "Worker agent register success" << std::endl;
}

void WorkerAgent::WorkerAgentHandler(rpc_conn conn,
                                     const CompactTaskArgs& compact_task_args) {
  ++local_task_nums_;
  std::cout << GetTime() << " "
            << "Receive" << compact_task_args.task_id << std::endl;
  std::thread thd1([compact_task_args] {
    std::string compaction_service_result;
    rocksdb::CompactionServiceOptionsOverride options_override;
    ROCKSDB_NAMESPACE::Options options_;
    std::unique_ptr<rocksdb::Env> hdfs;
    rocksdb::NewHdfsEnv(RPC_NAMESPACE::hdfs_address, &hdfs);
    options_override.env = hdfs.get();
    options_override.file_checksum_gen_factory =
        options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
        options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory = options_.sst_partitioner_factory;
    options_override.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    CompactionArgs compaction_args = compact_task_args.compaction_args;
    CompactionReply compaction_reply;
    rocksdb::Status s = ROCKSDB_NAMESPACE::DB::OpenAndCompact(
        compaction_args.name, compaction_args.output_directory,
        compaction_args.input, &compaction_service_result, options_override);
    compaction_reply = {s.code(), std::move(compaction_service_result)};
    --local_task_nums_;
    try {
      rest_rpc::rpc_client client(RPC_NAMESPACE::coordinator_ip,
                                  RPC_NAMESPACE::coordinator_port);
      bool r = client.connect();
      if (!r) {
        std::cout << "Worker agent register timeout" << std::endl;
        throw std::exception();
      }
      SubmitTaskArgs submitTaskArgs = {compact_task_args.task_id,
                                       compaction_reply};
      client.call<1000000000>("SubmitTaskHandler", submitTaskArgs);
      std::cout << GetTime() << " "
                << "End" << compact_task_args.task_id << std::endl;
    } catch (const std::exception& e) {
      std::cout << e.what() << std::endl;
    }
  });
  thd1.detach();
}

WorkerStatus WorkerAgent::CheckWorkerStatusHandler(rpc_conn conn) {
  double memory_usage = get_memoccupy();
  return {ip_, port_, local_task_nums_, WorkerAgent::max_task_nums_,
          memory_usage};
}

}  // namespace RPC_NAMESPACE

int main() {
  rpc_server server(RPC_NAMESPACE::worker_agent_port, 50);
  server.register_handler("WorkerAgentHandler",
                          RPC_NAMESPACE::WorkerAgent::WorkerAgentHandler);
  server.register_handler("UpdateWorkerStatusHandler",
                          RPC_NAMESPACE::WorkerAgent::CheckWorkerStatusHandler);
  RPC_NAMESPACE::WorkerAgent::RegisterWorker();
  server.run();
}