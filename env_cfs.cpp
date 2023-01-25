#include "env_cfs.h"

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"


namespace ROCKSDB_NAMESPACE {

/*extern "C" FactoryFunc<FileSystem> cfs_reg;

FactoryFunc<FileSystem> cfs_reg = 
  ObjectLibrary::Default()->AddFactory<FileSystem>(
      ObjectLibrary::PatternEntry("cfs", false).AddSeparator("://"),
      [] (const std::string& uri, std::unique_ptr<FileSystem>* f,
        std::string* errmsg) {
      FileSystem* fs= nullptr;
      Status s = CfsFileSystem::Create(FileSystem::Default(), uri, f);
      if (!s.ok()) {
          *errmsg = "Failed to connect to default server";
        }
        return f->get();
      }
    );

    */
extern "C" int register_CfsObjects(ROCKSDB_NAMESPACE::ObjectLibrary&, const std::string&);
extern "C" void cfs_reg();

void cfs_reg() {
#ifndef ROCKSDB_LITE
  fprintf(stdout, "Registering CFS ***********\n");
  fflush(stdout);
  auto lib = ObjectRegistry::Default()->AddLibrary("cfs");
  register_CfsObjects(*lib, "cfs");
#endif // ROCKSDB_LITE
}

 

int register_CfsObjects(ROCKSDB_NAMESPACE::ObjectLibrary& library, const std::string&) {
  // cfs FileSystem and Env objects can be registered as either:
  // cfs://
  // cfs://server:port
  // cfs://server:port/dir
  
  library.AddFactory<FileSystem>(ObjectLibrary::PatternEntry(CfsFileSystem::kNickName(), false)
                                 .AddSeparator("://", false),
      [](const std::string& uri, std::unique_ptr<FileSystem>* guard,
         std::string* errmsg) {
        Status s = CfsFileSystem::Create(FileSystem::Default(), uri, guard);
        if (!s.ok()) {
          *errmsg = "Failed to connect to default server";
        }
        return guard->get();
      });
  library.AddFactory<Env>(ObjectLibrary::PatternEntry(CfsFileSystem::kNickName(), false)
                          .AddSeparator("://", false),
      [](const std::string& uri, std::unique_ptr<Env>* guard,
         std::string* errmsg) {
        Status s = NewCfsEnv(uri, guard);
        if (!s.ok()) {
          *errmsg = "Failed to connect to default server";
        }
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
  
}

// The factory method for creating an Cfs Env
Status NewCfsFileSystem(const std::string& uri, std::shared_ptr<FileSystem>* fs) {
  std::unique_ptr<FileSystem> cfs;
  Status s = CfsFileSystem::Create(FileSystem::Default(), uri, &cfs);
  if (s.ok()) {
    fs->reset(cfs.release());
  }
  return s;
}

Status NewCfsEnv(const std::string& uri, std::unique_ptr<Env>* cfs) {
  std::shared_ptr<FileSystem> fs;
  Status s = NewCfsFileSystem(uri, &fs);
  if (s.ok()) {
    *cfs =  NewCompositeEnv(fs);
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
