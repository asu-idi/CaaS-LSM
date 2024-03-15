// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <functional>
#include <future>
#include <iostream>
#include <sstream>
#include <string>

#include "compaction_service.grpc.pb.h"
#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"
#include "utilities/util/terark_boost.hpp"

namespace TERARKDB_NAMESPACE {
// using boost::noncopyable;
struct CompactionWorkerContext;
struct CompactionWorkerResult;

class CompactionDispatcher : boost::noncopyable {
 public:
  virtual ~CompactionDispatcher() = default;

  virtual std::function<CompactionWorkerResult()> StartCompaction(
      const CompactionWorkerContext& context) = 0;

  virtual const char* Name() const = 0;
};

class RemoteCompactionDispatcher : public CompactionDispatcher {
 public:
  virtual std::function<CompactionWorkerResult()> StartCompaction(
      const CompactionWorkerContext& context) override;

  virtual const char* Name() const override;

  virtual std::future<std::string> DoCompaction(std::string data);
  class Worker : boost::noncopyable {
   public:
    Worker(EnvOptions env_options, Env* env);
    Worker();
    virtual ~Worker();
    virtual std::string GenerateOutputFileName(size_t file_index) = 0;
    std::string DoCompaction(Slice data);
    static void DebugSerializeCheckResult(Slice data);

   protected:
    struct Rep;
    Rep* rep_;
  };
  std::unique_ptr<compactionservice::CSAService::Stub> stub_;

  RemoteCompactionDispatcher(const std::shared_ptr<grpc::Channel>& channel)
      : stub_(compactionservice::CSAService::NewStub(channel)) {}
};

class LocalWorker
    : public TERARKDB_NAMESPACE::RemoteCompactionDispatcher::Worker {
  std::string GenerateOutputFileName(size_t file_index) override {
    // make a file name
    std::ostringstream oss;
    oss << "Worker-" << std::this_thread::get_id() << "-" << file_index;
    return oss.str();
  }

 public:
  using TERARKDB_NAMESPACE::RemoteCompactionDispatcher::Worker::Worker;

  LocalWorker()
      : TERARKDB_NAMESPACE::RemoteCompactionDispatcher::Worker() {}
};

}  // namespace TERARKDB_NAMESPACE
