// Copyright (c) 2017 The Ustore Authors.

#include "store/chunk_loader.h"

namespace ustore {

ChunkLoader::ChunkLoader() : cs_{store::GetChunkStore()} {}
// ChunkLoader::ChunkLoader(ChunkStore* cs) : cs_{cs} {}

ChunkLoader::~ChunkLoader() {
  for (auto& t : cache_) {
    delete t.second;
  }
}

const Chunk* ChunkLoader::Load(const Hash& key) {
  auto it = cache_.find(key);
  if (it != cache_.end()) {
    return it->second;
  } else {
    const Chunk* c = cs_->Get(key);
    cache_[c->hash()] = c;
    return c;
  }
}

}  // namespace ustore
