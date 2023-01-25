#include "env_cfs.h"

#include <rocksdb/env.h>

#include <stdio.h>
#include <sys/time.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include "logging/logging.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

#define CFS_EXISTS 0
#define CFS_DOESNT_EXIST -1
#define CFS_SUCCESS 0

//
// This file defines an cfs environment for rocksdb. It uses the libCfs
// api to access cfs. All Cfs files created by one instance of rocksdb
// will reside on the same cfs cluster.
//
namespace ROCKSDB_NAMESPACE {
namespace {
// Thrown during execution when there is an issue with the supplied
// arguments.
class CfsUsageException : public std::exception { };

// A simple exception that indicates something went wrong that is not
// recoverable.  The intention is for the message to be printed (with
// nothing else) and the process terminate.
class CfsFatalException : public std::exception {
public:
  explicit CfsFatalException(const std::string& s) : what_(s) { }
  virtual ~CfsFatalException() throw() { }
  virtual const char* what() const throw() {
    return what_.c_str();
  }
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

// Used for reading a file from cfs. It implements both sequential-read
// access methods as well as random read access methods.
class CfsReadableFile : virtual public FSSequentialFile,
                         virtual public FSRandomAccessFile {
 private:
  std::string filename_;

 public:
  CfsReadableFile(const std::string& fname)
      : filename_(fname) {
    fprintf(stdout, "[cfs] cfsReadableFile ocreated (%s)\n", filename_.c_str());
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsReadableFile opening file %s\n",
                    filename_.c_str());
    // openFile(filename_.c_str(), O_RDONLY)
  }

  ~CfsReadableFile() override {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsReadableFile closing file %s\n",
                    filename_.c_str());
    // CloseFile()
  }

  // sequential access, read data at current offset in file
  IOStatus Read(size_t n, const IOOptions& /*options*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    IOStatus s;
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsReadableFile read %s %ld\n",
                    filename_.c_str(), n);
    return IOStatus::OK();
  }

  // random access, read data from specified offset in file
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsReadableFile preading %s\n",
                    filename_.c_str());

    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsReadableFile skip %s\n",
                    filename_.c_str());
    return IOStatus::OK();
  }

  bool isValid() {
    return true;
  }

 private:

  // returns true if we are at the end of file, false otherwise
  bool feof() {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsReadableFile feof %s\n",
                    filename_.c_str());
   return true;
  }
};

// Appends to an existing file in cfs.
class CfsWritableFile: public FSWritableFile {
 private:
    std::string filename_;

 public:
  CfsWritableFile(const std::string& fname,
                   const FileOptions& options)
      : FSWritableFile(options),
        filename_(fname){
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile opening %s\n",
                    filename_.c_str());
  }
  ~CfsWritableFile() override {
      ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile closing %s\n",
                      filename_.c_str());
//      cfsCloseFile(fileSys_, hfile_);
    }

  using FSWritableFile::Append;

  // The name of the file, mostly needed for debug logging.
  const std::string& getName() {
    return filename_;
  }

  bool isValid(){
    return true;
  }

  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile Append %s\n",
                    filename_.c_str());
    const char* src = data.data();
    size_t left = data.size();
//    size_t ret = cfsWrite(fileSys_, hfile_, src, static_cast<tSize>(left));
     return IOStatus::OK();
  }

  // This is used by cfsLogger to write data to the debug log file
  IOStatus Append(const char* src, size_t size) {
     return IOStatus::OK();
  }
  
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile Sync %s\n",
                    filename_.c_str());
     return IOStatus::OK();
  }


  IOStatus Close(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile closing %s\n",
                    filename_.c_str());
     return IOStatus::OK();
  }
};

// The object that implements the debug logs to reside in cfs.
class CfsLogger : public Logger {
 private:
  CfsWritableFile* file_;

  Status CfsCloseHelper() {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsLogger closed %s\n",
                    file_->getName().c_str());
    if (mylog != nullptr && mylog == this) {
      mylog = nullptr;
    }
    return Status::OK();
  }

 protected:
  virtual Status CloseImpl() override { return CfsCloseHelper(); }

 public:
  CfsLogger(CfsWritableFile* f)
      : file_(f) {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsLogger opened %s\n",
                    file_->getName().c_str());
  }

  ~CfsLogger() override {
    if (!closed_) {
      closed_ = true;
      CfsCloseHelper().PermitUncheckedError();
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
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
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
          continue;       // Try again with larger buffer
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

class CfsDirectory : public FSDirectory {
 public:
  explicit CfsDirectory(int fd) : fd_(fd) {}
  ~CfsDirectory() {}

  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  int GetFd() const { return fd_; }

 private:
  int fd_;
};
}  // namespace

// Finally, the cfsFileSystem
CfsFileSystem::CfsFileSystem(const std::shared_ptr<FileSystem>& base, const std::string& fsname)
  : FileSystemWrapper(base), fsname_(fsname){
    fprintf(stdout, "Creating cfsFileSystem(%s)\n", fsname_.c_str());
}
  
CfsFileSystem::~CfsFileSystem() {
    fprintf(stderr, "Destroying cfsFileSystem(%s)\n", fsname_.c_str());
  }
  
std::string CfsFileSystem::GetId() const {
  if (fsname_.empty()) {
    return kProto;
  } else if (StartsWith(fsname_, kProto)) {
    return fsname_;
  } else {
    std::string id = kProto;
    return id.append("default:0").append(fsname_);
  }
}
  
Status CfsFileSystem::ValidateOptions(const DBOptions& db_opts,
                                       const ColumnFamilyOptions& cf_opts) const {
//  if (fileSys_ != nullptr) {
    return FileSystemWrapper::ValidateOptions(db_opts, cf_opts);
 /* } else {
    return Status::InvalidArgument("Failed to connect to cfs ", fsname_);
  }
  */
}
  
// open a file for sequential reading
IOStatus CfsFileSystem::NewSequentialFile(const std::string& fname,
                                               const FileOptions& /*options*/,
                                               std::unique_ptr<FSSequentialFile>* result,
                                               IODebugContext* /*dbg*/) {
  result->reset();
  CfsReadableFile* f = new CfsReadableFile(fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(f);
  return IOStatus::OK();
}

// open a file for random reading
IOStatus CfsFileSystem::NewRandomAccessFile(const std::string& fname,
                                                 const FileOptions& /*options*/,
                                                 std::unique_ptr<FSRandomAccessFile>* result,
                                                 IODebugContext* /*dbg*/) {
  result->reset();
  CfsReadableFile* f = new CfsReadableFile(fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(f);
  return IOStatus::OK();
}

// create a new file for writing
IOStatus CfsFileSystem::NewWritableFile(const std::string& fname,
                                             const FileOptions& options,
                                             std::unique_ptr<FSWritableFile>* result,
                                             IODebugContext* /*dbg*/) {
  result->reset();
  CfsWritableFile* f = new CfsWritableFile(fname, options);
  if (f == nullptr || !f->isValid()) {
    delete f;
    return IOError(fname, errno);
  }
  result->reset(f);
  return IOStatus::OK();
}

IOStatus CfsFileSystem::NewDirectory(const std::string& name,
                                          const IOOptions& options,
                                          std::unique_ptr<FSDirectory>* result,
                                          IODebugContext* dbg) {
  IOStatus s = FileExists(name, options, dbg);
  if (s.ok()) {
    result->reset(new CfsDirectory(0));
  } else {
    ROCKS_LOG_FATAL(mylog, "NewDirectory cfsExists call failed");
  }
  return s;
}

int cfsExists(const std::string &path) {
  return CFS_EXISTS;
}

IOStatus CfsFileSystem::FileExists(const std::string& fname,
                                        const IOOptions& /*options*/,
                                        IODebugContext* /*dbg*/) {
  int value = cfsExists(fname.c_str());
  switch (value) {
    case CFS_EXISTS:
      return IOStatus::OK();
    case CFS_DOESNT_EXIST:
      return IOStatus::NotFound();
    default:  // anything else should be an error
      ROCKS_LOG_FATAL(mylog, "FileExists cfsExists call failed");
      return IOStatus::IOError("cfsExists call failed with error " ,
                               " on path " + fname + ".\n");
  }
}

IOStatus CfsFileSystem::GetChildren(const std::string& path,
                                         const IOOptions& options,
                                         std::vector<std::string>* result, 
                                        IODebugContext* dbg) {
  IOStatus s = FileExists(path, options, dbg);
  return s;
}

int cfsDelete(const std::string &path) {
  return 0;
}

int cfsCreateDirectory(const std::string &path) {
  return 0;
}


IOStatus CfsFileSystem::DeleteFile(const std::string& fname,
                                        const IOOptions& /*options*/,
                                        IODebugContext* /*dbg*/) {
  if (cfsDelete(fname.c_str()) == 0) {
    return IOStatus::OK();
  }
  return IOError(fname, errno);
}

IOStatus CfsFileSystem::CreateDir(const std::string& name,
                                       const IOOptions& /*options*/,
                                       IODebugContext* /*dbg*/) {
  if (cfsCreateDirectory(name.c_str()) == 0) {
    return IOStatus::OK();
  }
  return IOError(name, errno);
}

IOStatus CfsFileSystem::CreateDirIfMissing(const std::string& name,
                                                const IOOptions& options,
                                                IODebugContext* dbg) {
  IOStatus s = FileExists(name, options, dbg);
  if (s.IsNotFound()) {
  //  Not atomic. state might change b/w cfsExists and CreateDir.
    s = CreateDir(name, options, dbg);
  }
  return s;
}
  
IOStatus CfsFileSystem::DeleteDir(const std::string& name, 
                                       const IOOptions& options,
                                       IODebugContext* dbg) {
  return DeleteFile(name, options, dbg);
};

IOStatus CfsFileSystem::GetFileSize(const std::string& fname,
                                         const IOOptions& /*options*/,
                                         uint64_t* size,
                                         IODebugContext* /*dbg*/) {
  *size = 0L;
  return IOStatus::OK();
}

IOStatus CfsFileSystem::GetFileModificationTime(const std::string& fname, 
                                                     const IOOptions& /*options*/,
                                                     uint64_t* time, 
                                                     IODebugContext* /*dbg*/) {
  return IOStatus::OK();

}

// The rename is not atomic. cfs does not allow a renaming if the
// target already exists. So, we delete the target before attempting the
// rename.

int cfsRename(const std::string &source, const std::string &dest) {
  return 0;
}

IOStatus CfsFileSystem::RenameFile(const std::string& src, const std::string& target,
                                        const IOOptions& /*options*/, IODebugContext* /*dbg*/) {
  cfsDelete(target.c_str());
  if (cfsRename(src.c_str(), target.c_str()) == 0) {
    return IOStatus::OK();
  }
  return IOError(src, errno);
}

IOStatus CfsFileSystem::LockFile(const std::string& /*fname*/,
                                      const IOOptions& /*options*/,
                                      FileLock** lock,
                                      IODebugContext* /*dbg*/) {
  // there isn's a very good way to atomically check and create
  // a file via libcfs
  *lock = nullptr;
  return IOStatus::OK();
}

IOStatus CfsFileSystem::UnlockFile(FileLock* /*lock*/,
                                        const IOOptions& /*options*/,
                                        IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus CfsFileSystem::NewLogger(const std::string &fname,
                                       const IOOptions& /*options*/,
                                       std::shared_ptr<Logger>* result,
                                       IODebugContext* /*dbg*/) {
  // EnvOptions is used exclusively for its `strict_bytes_per_sync` value. That
  // option is only intended for WAL/flush/compaction writes, so turn it off in
  // the logger.
  EnvOptions options;
  options.strict_bytes_per_sync = false;
  CfsWritableFile* f = new CfsWritableFile(fname, options);
  
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  CfsLogger* h = new CfsLogger(f);
  result->reset(h);
  if (mylog == nullptr) {
    mylog = h; 
  }
  return IOStatus::OK();
}

IOStatus CfsFileSystem::IsDirectory(const std::string& path,
                                         const IOOptions& /*options*/, 
                                         bool* is_dir,
                                         IODebugContext* /*dbg*/) {
                                         
    return IOStatus::OK();
}

Status CfsFileSystem::Create(const std::shared_ptr<FileSystem>& base, const std::string& uri, std::unique_ptr<FileSystem>* result) {
  result->reset();
  result->reset(new CfsFileSystem(base, uri));
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE

