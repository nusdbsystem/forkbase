// Copyright (c) 2017 The Ustore Authors.

#include "store/chunk_loader.h"

namespace ustore {

ChunkLoader::ChunkLoader() : cs_{store::GetChunkStore()} {}

ChunkLoader::~ChunkLoader() {}

const Chunk* ChunkLoader::Load(const Hash& key) {
  auto it = cache_.find(key);
  if (it != cache_.end()) return &(it->second);
  cache_.emplace(key.Clone(), cs_->Get(key));
  return &cache_[key];
}

}  // namespace ustore
