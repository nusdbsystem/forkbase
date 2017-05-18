// Copyright (c) 2017 The Ustore Authors.

#include "store/chunk_loader.h"

namespace ustore {

ServerChunkLoader::ServerChunkLoader() : cs_{store::GetChunkStore()} {}

ServerChunkLoader::~ServerChunkLoader() {}

const Chunk* ServerChunkLoader::Load(const Hash& key) {
  auto it = cache_.find(key);
  if (it != cache_.end()) return &(it->second);
  cache_.emplace(key.Clone(), cs_->Get(key));
  return &cache_[key];
}

}  // namespace ustore
