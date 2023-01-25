#include "cfs_writable.h"
//
// The object that implements the debug logs to reside in cfs.
namespace ROCKSDB_NAMESPACE {
  namespace {

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
  CfsLogger(CfsWritableFile* f);

  ~CfsLogger() override;

    using Logger::Logv;
  void Logv(const char* format, va_list ap) override ;
};

  }
}

