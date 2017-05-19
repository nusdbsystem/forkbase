# Dependencies
## Compulsory
### Core
1. cmake >= 2.8
2. g++ >= 4.8 (At least support c11 standard) 
3. protobuf >= 2.6.1 
4. boost >= 1.5.4 (file system)
5. czmq >= 4.0.2
6. libzmq >= 4.2.1
7. gflags >= 2.1.0


### Performance Monitor
1. libpcap (1.8.1)
2. dbus (1.0)
3. boost >= 1.5.4 (cpp-netlib) 
4. python >= 2.7

## Optional
### For RDMA
1. libibverbs >= 1.0
2. libboost_thread >= 1.5.4 (Thread)

### Other
1. LevelDB
    + Install from https://github.com/google/leveldb 
    + Export `$LEVELDB_ROOT` variable to point to the installed directory.
    + Include `$LEVELDB_ROOT` to `$CPLUS_INCLUDE_PATH` so that cmake can find the header files.

2. Snappy
    + Install from https://github.com/google/snappy
    + Export `$SNAPPY_ROOT_DIR` variable to point to the installed directory.


