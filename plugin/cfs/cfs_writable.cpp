#include "cfs_writable.h"

namespace ROCKSDB_NAMESPACE {
  namespace {

static Logger* myLog = nullptr;

  IOStatus CfsWritableFile::Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile Append %s\n",
                    filename_.c_str());
    const char* src = data.data();
    size_t left = data.size();
//    size_t ret = cfsWrite(fileSys_, hfile_, src, static_cast<tSize>(left));
     return IOStatus::OK();
  }

  // This is used by cfsLogger to write data to the debug log file
  IOStatus CfsWritableFile::Append(const char* src, size_t size) {
     return IOStatus::OK();
  }
  
  IOStatus CfsWritableFile::Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) {
    return IOStatus::OK();
  }

  IOStatus CfsWritableFile::Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile Sync %s\n",
                    filename_.c_str());
     return IOStatus::OK();
  }

  IOStatus CfsWritableFile::Close(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsWritableFile closing %s\n",
                    filename_.c_str());
     return IOStatus::OK();
  }

  }
}

