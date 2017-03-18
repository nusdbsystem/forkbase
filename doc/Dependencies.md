# Dependencies

1. LevelDB

    + Install from https://github.com/google/leveldb 
    + Export `$LEVELDB_ROOT` variable to point to the installed directory.
    + Include `$LEVELDB_ROOT` to `$CPLUS_INCLUDE_PATH` so that cmake can find the header files.

2. Snappy
    + Install from https://github.com/google/snappy
    + Export `$SNAPPY_ROOT_DIR` variable to point to the installed directory.
