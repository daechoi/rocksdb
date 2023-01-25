#include <rocksdb/file_system.h>

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


