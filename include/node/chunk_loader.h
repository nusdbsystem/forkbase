// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_CHUNK_LOADER_H_
#define USTORE_NODE_CHUNK_LOADER_H_

#include "hash/hash.h"
#include "chunk/chunk.h"

namespace ustore {
class ChunkLoader {
/* ChunkLoader is responsible to load chunks and cache them.
*/
 public:
  ChunkLoader();

  //  Delete all chunks
  ~ChunkLoader();

  const Chunk* Load(const Hash& key);
};
}  // namespace ustore

#endif  // USTORE_NODE_CHUNK_LOADER_H_
