#include <grpc/grpc.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "compaction_service.grpc.pb.h"
#include "queue"
#include "rocksdb/options.h"
#include "thread"
#include "utils.h"

ROCKSDB_NAMESPACE::OpenAndCompactOptions compaction_service_options;
std::unordered_map<uint64_t, compactionservice::AddTaskArgs> task_args_map_;
struct TaskCmp {
  bool operator()(const uint64_t& task1, const uint64_t& task2) const {
    if (task_args_map_[task1].compaction_addition_info().start_level() !=
        task_args_map_[task2].compaction_addition_info().start_level()) {
      return task_args_map_[task1].compaction_addition_info().start_level() >
             task_args_map_[task2].compaction_addition_info().start_level();
    }
    if (task_args_map_[task1].compaction_addition_info().score() < 0 &&
        task_args_map_[task2].compaction_addition_info().score() < 0) {
      return task1 > task2;
    }
    if (task_args_map_[task1].compaction_addition_info().score() < 0 ||
        task_args_map_[task2].compaction_addition_info().score() < 0) {
      return task_args_map_[task1].compaction_addition_info().score() >
             task_args_map_[task2].compaction_addition_info().score();
    }
    if (task_args_map_[task1].compaction_addition_info().score() !=
        task_args_map_[task2].compaction_addition_info().score()) {
      return task_args_map_[task1].compaction_addition_info().score() <
             task_args_map_[task2].compaction_addition_info().score();
    }
    return task1 > task2;
  }
};

std::unordered_map<std::string, compactionservice::CSAStatus> csa_status_map_;
std::unordered_map<std::string, std::vector<uint64_t>> csa_task_list_;
std::unordered_map<std::string,
                   std::unique_ptr<compactionservice::CSAService::Stub>>
    csa_client_map_;
std::unordered_map<uint64_t, compactionservice::CompactionReply>
    task_reply_map_;
std::atomic<uint64_t> next_task_id_ = 0;
std::unordered_map<uint64_t, uint64_t> reschedule_num;
std::priority_queue<uint64_t, std::vector<uint64_t>, TaskCmp>
    task_priority_queue_;
std::mutex monitor_latch_;
std::mutex scheduler_latch_;

class ProCPImpl final : public compactionservice::ProCPService::Service {
 public:
  bool JudgeFallback(const compactionservice::AddTaskArgs* request) {
    return false;
  }

  grpc::Status AddTask(grpc::ServerContext* context,
                       const compactionservice::AddTaskArgs* request,
                       compactionservice::TaskId* reply) override {
    std::lock_guard<std::mutex> lock(scheduler_latch_);
    if (JudgeFallback(request)) {
      return grpc::Status::CANCELLED;
    }
    task_args_map_[next_task_id_].mutable_compaction_args()->CopyFrom(
        request->compaction_args());
    task_args_map_[next_task_id_].mutable_compaction_addition_info()->CopyFrom(
        request->compaction_addition_info());
    task_priority_queue_.push(next_task_id_);
    ++next_task_id_;
    reply->set_task_id(next_task_id_ - 1);
    std::cout << "Add compaction task: (" << reply->task_id() << ")"
              << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status SubmitTask(grpc::ServerContext* context,
                          const compactionservice::SubmitTaskArgs* request,
                          google::protobuf::Empty* response) override {
    std::cout << GetTime() << "Submit compaction task: (" << request->task_id()
              << ")" << std::endl;
    std::lock_guard<std::mutex> lock(scheduler_latch_);
    if (task_args_map_.count(request->task_id()) == 0) {
      return grpc::Status::OK;
    }
    if (request->compaction_reply().code() != 0) {
      std::cout << GetTime() << "Compaction task (" << request->task_id()
                << ") failed" << std::endl;
      task_priority_queue_.push(request->task_id());
      return grpc::Status::OK;
    }
    std::cout << GetTime() << "Compaction task (" << request->task_id()
              << ") success" << std::endl;
    task_reply_map_[request->task_id()] = request->compaction_reply();
    task_args_map_.erase(request->task_id());
    return grpc::Status::OK;
  }

  grpc::Status CheckTask(grpc::ServerContext* context,
                         const compactionservice::TaskId* request,
                         compactionservice::CompactionReply* reply) override {
    std::lock_guard<std::mutex> lock(scheduler_latch_);
    if (task_reply_map_.count(request->task_id()) == 0) {
      std::cout << GetTime() << "Check compaction task (" << request->task_id()
                << "): Not finished" << std::endl;
      reply->set_code(99);
      return grpc::Status::OK;
    }
    auto res = task_reply_map_[request->task_id()];
    task_reply_map_.erase(request->task_id());
    std::cout << GetTime() << "Check compaction task (" << request->task_id()
              << "): Finished " << std::endl;
    reply->set_code(res.code());
    reply->mutable_result()->assign(res.result());
    reply->set_process_latency(res.process_latency());
    reply->set_open_db_latency(res.open_db_latency());
    return grpc::Status::OK;
  }

  grpc::Status RegisterCSA(grpc::ServerContext* context,
                           const compactionservice::CSAStatus* request,
                           google::protobuf::Empty* response) override {
    csa_status_map_[request->address()] = *request;
    csa_client_map_[request->address()] =
        compactionservice::CSAService::NewStub(grpc::CreateChannel(
            request->address(), grpc::InsecureChannelCredentials()));
    std::vector<uint64_t> temp;
    csa_task_list_[request->address()] = temp;
    std::cout << GetTime() << "Register CSA (" << request->address() << ")"
              << std::endl;
    return grpc::Status::OK;
  }
};

grpc::Status DistributeCompactionJob(
    const compactionservice::CompactionTaskArgs& compact_task_args,
    const std::string& csa_address) {
  grpc::ClientContext context;
  google::protobuf::Empty response;
  grpc::Status status = csa_client_map_[csa_address]->ExecuteCompactionTask(
      &context, compact_task_args, &response);
  return status;
}

void UpdateOneCSAStatus(const std::string& address) {
  auto& stub = csa_client_map_[address];
  grpc::ClientContext context;
  google::protobuf::Empty request;
  compactionservice::CSAStatus csa_status;
  grpc::Status status = stub->CheckCSAStatus(&context, request, &csa_status);
  monitor_latch_.lock();
  if (status.ok()) {
    csa_status_map_[address] = csa_status;
  } else {
    std::cout << GetTime() << "CSA (" << address << ") Offline" << std::endl;
    scheduler_latch_.lock();
    csa_status_map_.erase(address);
    csa_client_map_.erase(address);
    auto task_list = csa_task_list_[address];
    for (uint64_t i : task_list) {
      if (task_args_map_.count(i) > 0) {
        task_priority_queue_.push(i);
      }
    }
    csa_task_list_.erase(address);
    scheduler_latch_.unlock();
  }
  monitor_latch_.unlock();
}

[[noreturn]] void UpdateCSAStatus() {
  while (true) {
    sleep(5);
    for (auto iter = csa_status_map_.begin(); iter != csa_status_map_.end();) {
      UpdateOneCSAStatus(iter++->first);
    }
  }
}

std::string ScheduleCSA(
    const compactionservice::CompactionAdditionInfo& compaction_addition_info) {
  std::lock_guard<std::mutex> lock(monitor_latch_);
  if (csa_status_map_.empty()) {
    return "";
  }
  size_t min_task_nums = csa_status_map_.begin()->second.local_task_nums();
  auto best_worker = csa_status_map_.begin()->first;
  for (const auto& iter : csa_status_map_) {
    if (iter.second.local_task_nums() < min_task_nums &&
        iter.second.local_task_nums() < iter.second.max_task_nums()) {
      best_worker = iter.first;
      min_task_nums = iter.second.local_task_nums();
    }
  }
  if (csa_status_map_[best_worker].local_task_nums() >=
          csa_status_map_[best_worker].max_task_nums() ||
      csa_status_map_[best_worker].memory_usage() < 0.3) {
    return "";
  }
  return best_worker;
}

[[noreturn]] void ConsumeTask() {
  while (true) {
    scheduler_latch_.lock();
    if (task_priority_queue_.empty()) {
      scheduler_latch_.unlock();
      //      std::cout << GetTime() << "No task" << std::endl;
      sleep(1);
      continue;
    }
    auto task_id = task_priority_queue_.top();
    task_priority_queue_.pop();
    std::cout << GetTime() << "Schedule compaction task (" << task_id << ")"
              << std::endl;
    auto compaction_job_info = task_args_map_[task_id];
    if (csa_status_map_.empty() ||
        task_priority_queue_.size() >
            compaction_service_options.max_accumulation_in_procp ||
        reschedule_num[task_id] > compaction_service_options.max_reschedule) {
      compactionservice::CompactionReply compactionReply;
      compactionReply.set_code(1);
      task_reply_map_[task_id] = compactionReply;
      task_args_map_.erase(task_id);
      scheduler_latch_.unlock();
      std::cout << GetTime() << "Fallback compaction task (" << task_id << ")"
                << std::endl;
      continue;
    }
    auto worker_address =
        ScheduleCSA(compaction_job_info.compaction_addition_info());
    if (worker_address.empty()) {
      reschedule_num[task_id]++;
      std::cout << GetTime() << "Workers are all busy, reschedule " << task_id
                << std::endl;
      task_priority_queue_.push(task_id);
      scheduler_latch_.unlock();
      sleep(1);
      continue;
    }
    csa_task_list_[worker_address].emplace_back(task_id);
    scheduler_latch_.unlock();
    monitor_latch_.lock();
    std::cout << GetTime() << "Memory usage is "
              << csa_status_map_[worker_address].memory_usage() << std::endl;
    csa_status_map_[worker_address].set_local_task_nums(
        csa_status_map_[worker_address].local_task_nums() + 1);
    monitor_latch_.unlock();
    compactionservice::CompactionTaskArgs compaction_task_args;
    compaction_task_args.set_task_id(task_id);
    compaction_task_args.mutable_compaction_args()->CopyFrom(
        compaction_job_info.compaction_args());
    grpc::Status status =
        DistributeCompactionJob(compaction_task_args, worker_address);
    if (!status.ok()) {
      std::cout << GetTime()
                << " Failed to send compaction task to CSA: " << worker_address
                << std::endl;
      scheduler_latch_.lock();
      task_priority_queue_.push(task_id);
      scheduler_latch_.unlock();
    }
  }
}

int main() {
  std::string server_address(compaction_service_options.pro_cp_address);
  ProCPImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << GetTime() << "Server listening on " << server_address
            << std::endl;
  std::thread scheduler(ConsumeTask);
  std::thread monitor(UpdateCSAStatus);
  scheduler.join();
  monitor.join();
  server->Wait();
}