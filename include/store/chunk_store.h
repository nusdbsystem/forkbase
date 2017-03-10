// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_CHUNK_STORE_H_
#define USTORE_STORE_CHUNK_STORE_H_

#include <string>
#include "chunk/chunk.h"
#include "types/type.h"

namespace ustore {

class ChunkStore {
 public:
  /*
   * store allocates the returned chunck and rely on caller to release memory
   */
  Chunk* virtual Get(const Hash& key) = 0;
  bool virtual Put(const Hash& key, const Chunk& chunk) = 0;
};

}  // namespace ustore
#endif  // USTORE_CHUNK_STORE_H_
