#include "cfs_logger.h"
#include <sys/time.h>

namespace ROCKSDB_NAMESPACE {
  namespace {
    CfsLogger::CfsLogger(CfsWritableFile* f)
      : file_(f) {
    ROCKS_LOG_DEBUG(mylog, "[cfs] CfsLogger opened %s\n",
                    file_->getName().c_str());
  }

    CfsLogger::~CfsLogger() {
    if (!closed_) {
      closed_ = true;
      CfsCloseHelper().PermitUncheckedError();
    }
  }

  void CfsLogger::Logv(const char* format, va_list ap) {
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


  }
}

