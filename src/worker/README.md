# UStore Worker #

## Dependency Resolution ##

### RocksDB ###

Since [RocksDB v5.8](http://rocksdb.org/blog/2017/09/28/rocksdb-5-8-released.html), the library compiled with release mode is built with `-fno-rtti`, whereas that with debug mode is without it. However, abandoning the RTTI information with release build may cause the "Undefined Reference to Typeinfo" error. Therefore, when building the RocksDB library, the**`USE_RTTI`** environmental variable should be set to 1 prior to launch the `make` command. For example, 

    $ USE_RTTI=1 bash -c 'make shared_lib' && sudo make install-shared

this is to build the RocksDB shared library and install it to the default path. For building/installing the static library, use the command below: 

    $ USE_RTTI=1 bash -c 'make static_lib' && sudo make install-static

At times, you may prefer customized install path: 

    $ USE_RTTI=1 bash -c 'make -j shared_lib' && INSTALL_PATH=/path/to/install bash -c 'make install-shared'

## Compilation ##

To enable RocksDB-based head version table, `USE_SIMPLE_HEAD_VERSION=OFF` and `USE_ROCKSDB=ON` should be passed to the `cmake` command. For example, 

    $ cmake -DUSE_SIMPLE_HEAD_VERSION=OFF -DUSE_ROCKSDB=ON ..
