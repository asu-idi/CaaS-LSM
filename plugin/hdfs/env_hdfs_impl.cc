//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <stdio.h>
#include <sys/time.h>
#include <time.h>

#include <algorithm>
#include <iostream>
#include <sstream>

#include "env_hdfs.h"
#include "logging/logging.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

#define HDFS_EXISTS 0
#define HDFS_DOESNT_EXIST -1
#define HDFS_SUCCESS 0

//
// This file defines an HDFS environment for rocksdb. It uses the libhdfs
// api to access HDFS. All HDFS files created by one instance of rocksdb
// will reside on the same HDFS cluster.
//
namespace ROCKSDB_NAMESPACE {
namespace {
// Thrown during execution when there is an issue with the supplied
// arguments.
class HdfsUsageException : public std::exception {};

// A simple exception that indicates something went wrong that is not
// recoverable.  The intention is for the message to be printed (with
// nothing else) and the process terminate.
class HdfsFatalException : public std::exception {
 public:
  explicit HdfsFatalException(const std::string& s) : what_(s) {}
  virtual ~HdfsFatalException() throw() {}
  virtual const char* what() const throw() { return what_.c_str(); }

 private:
  const std::string what_;
};

// Log error message
static IOStatus IOError(const std::string& context, int err_number) {
  if (err_number == ENOSPC) {
    return IOStatus::NoSpace(context, errnoStr(err_number).c_str());
  } else if (err_number == ENOENT) {
    return IOStatus::PathNotFound(context, errnoStr(err_number).c_str());
  } else {
    return IOStatus::IOError(context, errnoStr(err_number).c_str());
  }
}

// assume that there is one global logger for now. It is not thread-safe,
// but need not be because the logger is initialized at db-open time.
static Logger* mylog = nullptr;

// Used for reading a file from HDFS. It implements both sequential-read
// access methods as well as random read access methods.
class HdfsReadableFile : virtual public FSSequentialFile,
                         virtual public FSRandomAccessFile {
 private:
  hdfsFS fileSys_;
  std::string filename_;
  hdfsFile hfile_;

 public:
  HdfsReadableFile(hdfsFS fileSys, const std::string& fname)
      : fileSys_(fileSys), filename_(fname), hfile_(nullptr) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile opening file %s\n",
                    filename_.c_str());
    hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), O_RDONLY, 0, 0, 0);
    ROCKS_LOG_DEBUG(mylog,
                    "[hdfs] HdfsReadableFile opened file %s hfile_=0x%p\n",
                    filename_.c_str(), hfile_);
  }

  ~HdfsReadableFile() override {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile closing file %s\n",
                    filename_.c_str());
    hdfsCloseFile(fileSys_, hfile_);
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile closed file %s\n",
                    filename_.c_str());
    hfile_ = nullptr;
  }

  bool isValid() { return hfile_ != nullptr; }

  // sequential access, read data at current offset in file
  IOStatus Read(size_t n, const IOOptions& /*options*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    IOStatus s;
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile reading %s %ld\n",
                    filename_.c_str(), n);

    char* buffer = scratch;
    size_t total_bytes_read = 0;
    tSize bytes_read = 0;
    tSize remaining_bytes = (tSize)n;

    // Read a total of n bytes repeatedly until we hit error or eof
    while (remaining_bytes > 0) {
      bytes_read = hdfsRead(fileSys_, hfile_, buffer, remaining_bytes);
      if (bytes_read <= 0) {
        break;
      }
      assert(bytes_read <= remaining_bytes);

      total_bytes_read += bytes_read;
      remaining_bytes -= bytes_read;
      buffer += bytes_read;
    }
    assert(total_bytes_read <= n);

    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile read %s\n",
                    filename_.c_str());

    if (bytes_read < 0) {
      s = IOError(filename_, errno);
    } else {
      *result = Slice(scratch, total_bytes_read);
    }

    return s;
  }

  // random access, read data from specified offset in file
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    IOStatus s;
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile preading %s\n",
                    filename_.c_str());
    tSize bytes_read =
        hdfsPread(fileSys_, hfile_, offset, static_cast<void*>(scratch),
                  static_cast<tSize>(n));
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile pread %s\n",
                    filename_.c_str());
    *result = Slice(scratch, (bytes_read < 0) ? 0 : bytes_read);
    if (bytes_read < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }

  IOStatus Skip(uint64_t n) override {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile skip %s\n",
                    filename_.c_str());
    // get current offset from file
    tOffset current = hdfsTell(fileSys_, hfile_);
    if (current < 0) {
      return IOError(filename_, errno);
    }
    // seek to new offset in file
    tOffset newoffset = current + n;
    int val = hdfsSeek(fileSys_, hfile_, newoffset);
    if (val < 0) {
      return IOError(filename_, errno);
    }
    return IOStatus::OK();
  }

 private:
  // returns true if we are at the end of file, false otherwise
  bool feof() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile feof %s\n",
                    filename_.c_str());
    if (hdfsTell(fileSys_, hfile_) == fileSize()) {
      return true;
    }
    return false;
  }

  // the current size of the file
  tOffset fileSize() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsReadableFile fileSize %s\n",
                    filename_.c_str());
    hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, filename_.c_str());
    tOffset size = 0L;
    if (pFileInfo != nullptr) {
      size = pFileInfo->mSize;
      hdfsFreeFileInfo(pFileInfo, 1);
    } else {
      throw HdfsFatalException("fileSize on unknown file " + filename_);
    }
    return size;
  }
};

// Appends to an existing file in HDFS.
class HdfsWritableFile : public FSWritableFile {
 private:
  hdfsFS fileSys_;
  std::string filename_;
  hdfsFile hfile_;

 public:
  HdfsWritableFile(hdfsFS fileSys, const std::string& fname,
                   const FileOptions& options)
      : FSWritableFile(options),
        fileSys_(fileSys),
        filename_(fname),
        hfile_(nullptr) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile opening %s\n",
                    filename_.c_str());
    hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), O_WRONLY, 0, 0, 0);
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile opened %s\n",
                    filename_.c_str());
    assert(hfile_ != nullptr);
  }
  ~HdfsWritableFile() override {
    if (hfile_ != nullptr) {
      ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile closing %s\n",
                      filename_.c_str());
      hdfsCloseFile(fileSys_, hfile_);
      ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile closed %s\n",
                      filename_.c_str());
      hfile_ = nullptr;
    }
  }

  using FSWritableFile::Append;

  // If the file was successfully created, then this returns true.
  // Otherwise returns false.
  bool isValid() { return hfile_ != nullptr; }

  // The name of the file, mostly needed for debug logging.
  const std::string& getName() { return filename_; }

  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile Append %s\n",
                    filename_.c_str());
    const char* src = data.data();
    size_t left = data.size();
    size_t ret = hdfsWrite(fileSys_, hfile_, src, static_cast<tSize>(left));
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile Appended %s\n",
                    filename_.c_str());
    if (ret != left) {
      return IOError(filename_, errno);
    }
    return IOStatus::OK();
  }

  // This is used by HdfsLogger to write data to the debug log file
  IOStatus Append(const char* src, size_t size) {
    if (hdfsWrite(fileSys_, hfile_, src, static_cast<tSize>(size)) !=
        static_cast<tSize>(size)) {
      return IOError(filename_, errno);
    }
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile Sync %s\n",
                    filename_.c_str());
    if (hdfsFlush(fileSys_, hfile_) == -1) {
      return IOError(filename_, errno);
    }
    if (hdfsHSync(fileSys_, hfile_) == -1) {
      return IOError(filename_, errno);
    }
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile Synced %s\n",
                    filename_.c_str());
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile closing %s\n",
                    filename_.c_str());
    if (hdfsCloseFile(fileSys_, hfile_) != 0) {
      return IOError(filename_, errno);
    }
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsWritableFile closed %s\n",
                    filename_.c_str());
    hfile_ = nullptr;
    return IOStatus::OK();
  }
};

// The object that implements the debug logs to reside in HDFS.
class HdfsLogger : public Logger {
 private:
  HdfsWritableFile* file_;

  Status HdfsCloseHelper() {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsLogger closed %s\n",
                    file_->getName().c_str());
    if (mylog != nullptr && mylog == this) {
      mylog = nullptr;
    }
    return Status::OK();
  }

 protected:
  virtual Status CloseImpl() override { return HdfsCloseHelper(); }

 public:
  HdfsLogger(HdfsWritableFile* f) : file_(f) {
    ROCKS_LOG_DEBUG(mylog, "[hdfs] HdfsLogger opened %s\n",
                    file_->getName().c_str());
  }

  ~HdfsLogger() override {
    if (!closed_) {
      closed_ = true;
      HdfsCloseHelper().PermitUncheckedError();
    }
  }

  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    const uint64_t thread_id = Env::Default()->GetThreadID();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, nullptr);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                    t.tm_min, t.tm_sec, static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;  // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      file_->Append(base, p - base).PermitUncheckedError();
      file_->Flush(IOOptions(), nullptr).PermitUncheckedError();
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};

class HdfsDirectory : public FSDirectory {
 public:
  explicit HdfsDirectory(int fd) : fd_(fd) {}
  ~HdfsDirectory() {}

  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  int GetFd() const { return fd_; }

 private:
  int fd_;
};

}  // namespace

// Finally, the HdfsFileSystem
HdfsFileSystem::HdfsFileSystem(const std::shared_ptr<FileSystem>& base,
                               const std::string& fsname, hdfsFS fileSys)
    : FileSystemWrapper(base), fsname_(fsname), fileSys_(fileSys) {}

HdfsFileSystem::~HdfsFileSystem() {
  if (fileSys_ != nullptr) {
    fprintf(stderr, "Destroying HdfsFileSystem(%s)\n", fsname_.c_str());
    hdfsDisconnect(fileSys_);
  }
}

std::string HdfsFileSystem::GetId() const {
  if (fsname_.empty()) {
    return kProto;
  } else if (StartsWith(fsname_, kProto)) {
    return fsname_;
  } else {
    std::string id = kProto;
    return id.append("default:0").append(fsname_);
  }
}

Status HdfsFileSystem::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (fileSys_ != nullptr) {
    return FileSystemWrapper::ValidateOptions(db_opts, cf_opts);
  } else {
    return Status::InvalidArgument("Failed to connect to hdfs ", fsname_);
  }
}

// open a file for sequential reading
IOStatus HdfsFileSystem::NewSequentialFile(
    const std::string& fname, const FileOptions& /*options*/,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  HdfsReadableFile* f = new HdfsReadableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(f);
  return IOStatus::OK();
}

// open a file for random reading
IOStatus HdfsFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& /*options*/,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  HdfsReadableFile* f = new HdfsReadableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(f);
  return IOStatus::OK();
}

// create a new file for writing
IOStatus HdfsFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  HdfsWritableFile* f = new HdfsWritableFile(fileSys_, fname, options);
  if (f == nullptr || !f->isValid()) {
    delete f;
    return IOError(fname, errno);
  }
  result->reset(f);
  return IOStatus::OK();
}

IOStatus HdfsFileSystem::NewDirectory(const std::string& name,
                                      const IOOptions& options,
                                      std::unique_ptr<FSDirectory>* result,
                                      IODebugContext* dbg) {
  IOStatus s = FileExists(name, options, dbg);
  if (s.ok()) {
    result->reset(new HdfsDirectory(0));
  } else {
    ROCKS_LOG_FATAL(mylog, "NewDirectory hdfsExists call failed");
  }
  return s;
}

IOStatus HdfsFileSystem::FileExists(const std::string& fname,
                                    const IOOptions& /*options*/,
                                    IODebugContext* /*dbg*/) {
  int value = hdfsExists(fileSys_, fname.c_str());
  switch (value) {
    case HDFS_EXISTS:
      return IOStatus::OK();
    case HDFS_DOESNT_EXIST:
      return IOStatus::NotFound();
    default:  // anything else should be an error
      ROCKS_LOG_FATAL(mylog, "FileExists hdfsExists call failed");
      return IOStatus::IOError(
          "hdfsExists call failed with error <Need to fix ToString() here> on "
          "path " +
          fname + ".\n");
  }
}

IOStatus HdfsFileSystem::GetChildren(const std::string& path,
                                     const IOOptions& options,
                                     std::vector<std::string>* result,
                                     IODebugContext* dbg) {
  IOStatus s = FileExists(path, options, dbg);
  if (s.ok()) {
    int numEntries = 0;
    hdfsFileInfo* pHdfsFileInfo = 0;
    pHdfsFileInfo = hdfsListDirectory(fileSys_, path.c_str(), &numEntries);
    if (numEntries >= 0) {
      for (int i = 0; i < numEntries; i++) {
        std::string pathname(pHdfsFileInfo[i].mName);
        size_t pos = pathname.rfind("/");
        if (std::string::npos != pos) {
          result->push_back(pathname.substr(pos + 1));
        }
      }
      if (pHdfsFileInfo != nullptr) {
        hdfsFreeFileInfo(pHdfsFileInfo, numEntries);
      }
    } else {
      // numEntries < 0 indicates error
      ROCKS_LOG_FATAL(mylog, "hdfsListDirectory call failed with error ");
      s = IOStatus::IOError("hdfsListDirectory call failed negative error.");
    }
  }
  return s;
}

IOStatus HdfsFileSystem::DeleteFile(const std::string& fname,
                                    const IOOptions& /*options*/,
                                    IODebugContext* /*dbg*/) {
  if (hdfsDelete(fileSys_, fname.c_str(), 1) == 0) {
    return IOStatus::OK();
  }
  return IOError(fname, errno);
}

IOStatus HdfsFileSystem::CreateDir(const std::string& name,
                                   const IOOptions& /*options*/,
                                   IODebugContext* /*dbg*/) {
  if (hdfsCreateDirectory(fileSys_, name.c_str()) == 0) {
    return IOStatus::OK();
  }
  return IOError(name, errno);
}

IOStatus HdfsFileSystem::CreateDirIfMissing(const std::string& name,
                                            const IOOptions& options,
                                            IODebugContext* dbg) {
  IOStatus s = FileExists(name, options, dbg);
  if (s.IsNotFound()) {
    //  Not atomic. state might change b/w hdfsExists and CreateDir.
    s = CreateDir(name, options, dbg);
  }
  return s;
}

IOStatus HdfsFileSystem::DeleteDir(const std::string& name,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  return DeleteFile(name, options, dbg);
};

IOStatus HdfsFileSystem::GetFileSize(const std::string& fname,
                                     const IOOptions& /*options*/,
                                     uint64_t* size, IODebugContext* /*dbg*/) {
  *size = 0L;
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, fname.c_str());
  if (pFileInfo != nullptr) {
    *size = pFileInfo->mSize;
    hdfsFreeFileInfo(pFileInfo, 1);
    return IOStatus::OK();
  }
  return IOError(fname, errno);
}

IOStatus HdfsFileSystem::GetFileModificationTime(const std::string& fname,
                                                 const IOOptions& /*options*/,
                                                 uint64_t* time,
                                                 IODebugContext* /*dbg*/) {
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, fname.c_str());
  if (pFileInfo != nullptr) {
    *time = static_cast<uint64_t>(pFileInfo->mLastMod);
    hdfsFreeFileInfo(pFileInfo, 1);
    return IOStatus::OK();
  }
  return IOError(fname, errno);
}

// The rename is not atomic. HDFS does not allow a renaming if the
// target already exists. So, we delete the target before attempting the
// rename.
IOStatus HdfsFileSystem::RenameFile(const std::string& src,
                                    const std::string& target,
                                    const IOOptions& /*options*/,
                                    IODebugContext* /*dbg*/) {
  hdfsDelete(fileSys_, target.c_str(), 1);
  if (hdfsRename(fileSys_, src.c_str(), target.c_str()) == 0) {
    return IOStatus::OK();
  }
  return IOError(src, errno);
}

IOStatus HdfsFileSystem::LockFile(const std::string& /*fname*/,
                                  const IOOptions& /*options*/, FileLock** lock,
                                  IODebugContext* /*dbg*/) {
  // there isn's a very good way to atomically check and create
  // a file via libhdfs
  *lock = nullptr;
  return IOStatus::OK();
}

IOStatus HdfsFileSystem::UnlockFile(FileLock* /*lock*/,
                                    const IOOptions& /*options*/,
                                    IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus HdfsFileSystem::NewLogger(const std::string& fname,
                                   const IOOptions& /*options*/,
                                   std::shared_ptr<Logger>* result,
                                   IODebugContext* /*dbg*/) {
  // EnvOptions is used exclusively for its `strict_bytes_per_sync` value. That
  // option is only intended for WAL/flush/compaction writes, so turn it off in
  // the logger.
  EnvOptions options;
  options.strict_bytes_per_sync = false;
  HdfsWritableFile* f = new HdfsWritableFile(fileSys_, fname, options);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  HdfsLogger* h = new HdfsLogger(f);
  result->reset(h);
  if (mylog == nullptr) {
    // mylog = h; // uncomment this for detailed logging
  }
  return IOStatus::OK();
}

IOStatus HdfsFileSystem::IsDirectory(const std::string& path,
                                     const IOOptions& /*options*/, bool* is_dir,
                                     IODebugContext* /*dbg*/) {
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, path.c_str());
  if (pFileInfo != nullptr) {
    if (is_dir != nullptr) {
      *is_dir = (pFileInfo->mKind == kObjectKindDirectory);
    }
    hdfsFreeFileInfo(pFileInfo, 1);
    return IOStatus::OK();
  }
  return IOError(path, errno);
}

Status HdfsFileSystem::Create(const std::shared_ptr<FileSystem>& base,
                              const std::string& uri,
                              std::unique_ptr<FileSystem>* result) {
  result->reset();
  std::string cwd;
  hdfsFS fs;
  if (uri.empty() || uri == HdfsFileSystem::kProto) {
    // Either empty or hdfs://
    fs = hdfsConnectNewInstance("default", 0);
  } else if (!StartsWith(uri, HdfsFileSystem::kProto)) {
    // Does not start with hdfs://, so its just a path
    cwd = uri;
    fs = hdfsConnectNewInstance("default", 0);
  } else {
    auto start = strlen(HdfsFileSystem::kProto);
    auto colon = uri.find(':', 1 + start);
    if (colon == std::string::npos || colon == uri.size() - 1) {
      return Status::InvalidArgument("Bad host-port for hdfs ", uri);
    } else {
      std::string host = uri.substr(start, colon);
      std::string rest = uri.substr(colon + 1);
      auto slash = rest.find('/');
      if (slash != std::string::npos) {
        cwd = rest.substr(slash);
        rest = rest.substr(0, slash);
      }
      char* end = nullptr;
      auto port = strtol(rest.c_str(), &end, 10);
      if (static_cast<size_t>(end - rest.data()) != rest.length()) {
        return Status::InvalidArgument("Bad host-port for hdfs ", uri);
      } else if (port == 0) {
        return Status::InvalidArgument("Bad host-port0 for hdfs ", uri);
      } else {
        fs = hdfsConnectNewInstance(host.c_str(), static_cast<tPort>(port));
      }
    }
  }
  if (fs == nullptr) {
  } else if (!cwd.empty()) {
    int code = hdfsSetWorkingDirectory(fs, cwd.c_str());
    if (code < 0) {
      hdfsDisconnect(fs);
      fs = nullptr;
      return Status::InvalidArgument("Bad working directory for hdfs ", uri);
    }
  }
  result->reset(new HdfsFileSystem(base, uri, fs));
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
