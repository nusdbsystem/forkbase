// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_CHUNK_LOADER_H_
#define USTORE_STORE_CHUNK_LOADER_H_

#include <map>
#include <string>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "store/chunk_store.h"

namespace ustore {
/** ChunkLoader is responsible to load chunks and cache them.
*/
class ChunkLoader {
 public:
  ChunkLoader();
  // let ChunkLoader call chunkStore internally
  // explicit ChunkLoader(ChunkStore* cs);
  //  Delete all chunks
  ~ChunkLoader();

  const Chunk* Load(const Hash& key);

 private:
  std::map<Hash, Chunk> cache_;
  ChunkStore* cs_;
};
}  // namespace ustore

#endif  // USTORE_STORE_CHUNK_LOADER_H_
