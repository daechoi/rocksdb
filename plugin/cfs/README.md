#Mock CFS

`Mock CFS` is to demonstrate how to integrate CFS with RocksDB.  
It provides a factory function in a header file to show integration with RocksDB header includes. 

env_cfs.[cc,h] define a factory function NewCfsEnv(...) that creates a Cfs object given a string based uri representing the uri of the CFS.

env_cfs_impl.[cc,h] define CfsEnv needed to talk to an underlying CFS.

## Build

The code first need to be linked under RocksDB's "plugin/" directory.  Go to the base root directory of rocksdb source.

```
$ pushd ./plugin/
$ git clone htts://github.com/crystalcld/mock_cfs.git cfs
```

Next, we can build and install RocksDB with this plugin as follows:

```
$ popd
$ cd build/
$ cmake .. -DROCKSDB_PLUGINS="cfs"
$ make clean && sudo DEBUG_LEVEL=0 ROCKSDB_PLUGINS="cfs" make install
```

# Tool usage
For RocksDB binaries, e.g. db_bench and db_stress we built earlier, the plugin
can be enabled through configuration. Db_bench and db_stress in particular
takes a -env_uri where we can specify a cfs. For example
```
$ ./db_stress -env_uri=cfs://localhost:9000/ -compression_type=none
$ ./db_bench -benchmarks=fillrandom -env_uri=cfs://localhost:9000/ --compression_type=none
```

