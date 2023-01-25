#include "rocksdb/file_system.h"
#include "logging/logging.h"

// Appends to an existing file in cfs.
namespace ROCKSDB_NAMESPACE {
  namespace {

extern Logger* mylog; 

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
                  IODebugContext* /*dbg*/) override;  // This is used by cfsLogger to write data to the debug log file
                                                      
  IOStatus Append(const char* src, size_t size); 
  
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override;

  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override; 

  IOStatus Close(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override ;
};


  }
}

