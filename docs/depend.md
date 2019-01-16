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


