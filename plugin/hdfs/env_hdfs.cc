//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env_hdfs.h"

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE
int register_HdfsObjects(ObjectLibrary& library, const std::string&) {
  // HDFS FileSystem and Env objects can be registered as either:
  // hdfs://
  // hdfs://server:port
  // hdfs://server:port/dir

  library.AddFactory<FileSystem>(
      ObjectLibrary::PatternEntry(HdfsFileSystem::kNickName(), false)
          .AddSeparator("://", false),
      [](const std::string& uri, std::unique_ptr<FileSystem>* guard,
         std::string* errmsg) {
        Status s = HdfsFileSystem::Create(FileSystem::Default(), uri, guard);
        if (!s.ok()) {
          *errmsg = "Failed to connect to default server";
        }
        return guard->get();
      });
  library.AddFactory<Env>(
      ObjectLibrary::PatternEntry(HdfsFileSystem::kNickName(), false)
          .AddSeparator("://", false),
      [](const std::string& uri, std::unique_ptr<Env>* guard,
         std::string* errmsg) {
        Status s = NewHdfsEnv(uri, guard);
        if (!s.ok()) {
          *errmsg = "Failed to connect to default server";
        }
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // ROCKSDB_LITE
void hdfs_reg() {
#ifndef ROCKSDB_LITE
  auto lib = ObjectRegistry::Default()->AddLibrary("hdfs");
  register_HdfsObjects(*lib, "hdfs");
#endif  // ROCKSDB_LITE
}

// The factory method for creating an HDFS Env
Status NewHdfsFileSystem(const std::string& uri,
                         std::shared_ptr<FileSystem>* fs) {
  std::unique_ptr<FileSystem> hdfs;
  Status s = HdfsFileSystem::Create(FileSystem::Default(), uri, &hdfs);
  if (s.ok()) {
    fs->reset(hdfs.release());
  }
  return s;
}

Status NewHdfsEnv(const std::string& uri, std::unique_ptr<Env>* hdfs) {
  std::shared_ptr<FileSystem> fs;
  Status s = NewHdfsFileSystem(uri, &fs);
  if (s.ok()) {
    *hdfs = NewCompositeEnv(fs);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
