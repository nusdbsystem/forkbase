// Copyright (c) 2017 The Ustore Authors.

#include "chunk/chunk_loader.h"

namespace ustore {

const Chunk* ChunkLoader::Load(const Hash& key) {
  CHECK(!key.empty());
  auto it = cache_.find(key);
  if (it != cache_.end()) return &(it->second);
  cache_.emplace(key.Clone(), GetChunk(key));
  return &cache_[key];
}

Chunk ServerChunkLoader::GetChunk(const Hash& key) {
  return cs_->Get(key);
}

Chunk ClientChunkLoader::GetChunk(const Hash& key) {
  return db_->GetChunk(Slice(key_), key);
}

}  // namespace ustore
