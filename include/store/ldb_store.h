// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_LDB_STORE_H_
#define USTORE_STORE_LDB_STORE_H_

#include <string>
#include <memory>
#include "leveldb/db.h"
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "store/chunk_store.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class LDBStore : public ChunkStore, private Noncopyable {
 public:
  LDBStore();
  explicit LDBStore(const std::string& dbpath);
  ~LDBStore();
  /*
   * store allocates the returned chunck,
   * caller need to release memory after use.
   */
  Chunk* Get(const Hash& key) override;
  bool Put(const Hash& key, const Chunk& chunk) override;

 private:
  leveldb::DB* db_;
  leveldb::Options opt_;
  leveldb::WriteOptions wr_opt_;
  leveldb::ReadOptions rd_opt_;
};
}  // namespace ustore

#endif  // USTORE_STORE_LDB_STORE_H_
