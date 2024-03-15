#pragma once
#include <string>

namespace RPC_NAMESPACE {
static std::string hdfs_address = "hdfs://10.128.0.11:9000/";
static std::string coordinator_ip = "10.128.0.9";
static unsigned short coordinator_port = 8000;

static std::string worker_agent_ip = "10.128.0.9";
static unsigned short worker_agent_port = 8010;

static int check_time_interval = 1;

}  // namespace RPC_NAMESPACE