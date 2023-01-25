#pragma once

#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/status.h>
#include "rocksdb/utilities/object_registry.h"


namespace ROCKSDB_NAMESPACE {
  
class CfsFileSystem : public FileSystemWrapper {
 public:
  static const char* kClassName() { return "CfsFileSystem"; }
  const char* Name() const override { return kClassName(); }
  static const char* kNickName() { return "cfs"; }
  static constexpr const char* kProto = "cfs://"; 
  
  const char* NickName() const override { return kNickName(); }
  static Status Create(const std::shared_ptr<FileSystem>& base, const std::string& fsname, std::unique_ptr<FileSystem>* fs);

  explicit CfsFileSystem(const std::shared_ptr<FileSystem>& base, const std::string& fsname);
  ~CfsFileSystem() override;

  std::string GetId() const override;
  
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;
  
  IOStatus NewSequentialFile(const std::string& /*fname*/,
                             const FileOptions& /*options*/,
                             std::unique_ptr<FSSequentialFile>* /*result*/,
                             IODebugContext* /*dbg*/) override;
  IOStatus NewRandomAccessFile(const std::string& /*fname*/,
                               const FileOptions& /*options*/,
                               std::unique_ptr<FSRandomAccessFile>* /*result*/,
                               IODebugContext* /*dbg*/) override;
  IOStatus NewWritableFile(const std::string& /*fname*/,
                           const FileOptions& /*options*/,
                           std::unique_ptr<FSWritableFile>* /*result*/,
                           IODebugContext* /*dbg*/) override;
  IOStatus NewDirectory(const std::string& /*name*/,
                        const IOOptions& /*options*/,
                        std::unique_ptr<FSDirectory>* /*result*/,
                        IODebugContext* /*dbg*/) override;
  IOStatus FileExists(const std::string& /*fname*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus GetChildren(const std::string& /*path*/,
                       const IOOptions& /*options*/,
                       std::vector<std::string>* /*result*/,
                       IODebugContext* /*dbg*/) override;
  IOStatus DeleteFile(const std::string& /*fname*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus CreateDir(const std::string& /*name*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override;
  IOStatus CreateDirIfMissing(const std::string& /*name*/,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override;
  IOStatus DeleteDir(const std::string& /*name*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override;
  IOStatus GetFileSize(const std::string& /*fname*/,
                       const IOOptions& /*options*/, uint64_t* /*size*/,
                       IODebugContext* /*dbg*/) override;
  IOStatus GetFileModificationTime(const std::string& /*fname*/,
                                   const IOOptions& /*options*/,
                                   uint64_t* /*time*/,
                                   IODebugContext* /*dbg*/) override;
  IOStatus RenameFile(const std::string& /*src*/, const std::string& /*target*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus LockFile(const std::string& /*fname*/, const IOOptions& /*options*/,
                    FileLock** /*lock*/, IODebugContext* /*dbg*/) override;
  IOStatus UnlockFile(FileLock* /*lock*/, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus NewLogger(const std::string& /*fname*/, const IOOptions& /*options*/,
                     std::shared_ptr<Logger>* /*result*/,
                     IODebugContext* /*dbg*/) override;
  IOStatus IsDirectory(const std::string& /*path*/,
                       const IOOptions& /*options*/, bool* /*is_dir*/,
                       IODebugContext* /*dbg*/) override;
 private:
  std::string fsname_;  // string of the form "cfs://hostname:port/dira"
//  crystal::cfs_client filesys_; // 
};

// Returns a `FileSystem` that hashes file contents when naming files, thus
// deduping them. RocksDB however expects files to be identified based on a
// monotonically increasing counter, so a mapping of RocksDB's name to content
// hash is needed. This mapping is stored in a separate RocksDB instance.
Status NewCfsEnv(const std::string& fsname, std::unique_ptr<Env>* env);
Status NewCfsFileSystem(const std::string& fsname, std::shared_ptr<FileSystem>* fs);
}  // namespace ROCKSDB_NAMESPACE
