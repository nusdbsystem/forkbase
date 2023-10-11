# Dependencies

## Compulsory

### Core

* cmake >= 2.8
* g++ >= 4.9
* protobuf >= 2.6.1
* boost >= 1.66.0
* czmq >= 4.0.2
* libzmq >= 4.2.1
* gflags >= 2.1.0

### Performance Monitor

* libpcap (1.8.1)
* dbus (1.0)
* cpp-netlib == 0.11.0
* python >= 2.7

## Optional

### For RDMA

* libibverbs >= 1.0
* libboost_thread >= 1.5.4 (Thread)

### For Cryptographic Hashing

* libcrypto++
```
Download from https://github.com/weidai11/cryptopp/releases
```

### For Storage

* LevelDB
```
Install from https://github.com/google/leveldb
Export `$LEVELDB_ROOT` variable to point to the installed directory.
Include `$LEVELDB_ROOT` to `$CPLUS_INCLUDE_PATH` so that cmake can find the header files.
```

* Snappy
```
Install from https://github.com/google/snappy
Export `$SNAPPY_ROOT_DIR` variable to point to the installed directory.
```

* RocksDB >= 5.8 ([download](https://github.com/facebook/rocksdb/releases))

Install RocksDB
```
$ make shared_lib -j "${NCORES}" USE_RTTI=1 DISABLE_WARNING_AS_ERROR=ON && make install-shared INSTALL_PATH="${LIB_HOME}/rocksdb"
```
Set RocksDB environment variables
```
# RocksDB
export ROCKSDB_ROOT="${LIB_HOME}/rocksdb"
export CPATH="${ROCKSDB_ROOT}/include:${CPATH}"
export LD_LIBRARY_PATH="${ROCKSDB_ROOT}/lib:${LD_LIBRARY_PATH}"
export LIBRARY_PATH="${ROCKSDB_ROOT}/lib:${LIBRARY_PATH}"
export CMAKE_INCLUDE_PATH="${ROCKSDB_ROOT}/include:${CMAKE_INCLUDE_PATH}"
export CMAKE_LIBRARY_PATH="${ROCKSDB_ROOT}/lib:${CMAKE_LIBRARY_PATH}"
```



