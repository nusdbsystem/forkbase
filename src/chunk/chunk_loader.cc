// Copyright (c) 2017 The Ustore Authors.

#include "chunk/chunk_loader.h"
#include "cluster/partitioner.h"
#include "spec/db.h"

namespace ustore {

const Chunk* ChunkLoader::Load(const Hash& key) {
  CHECK(!key.empty());
  auto it = cache_.find(key);
  if (it != cache_.end()) return &(it->second);
  cache_.emplace(key.Clone(), GetChunk(key));
  return &cache_[key];
}

Chunk LocalChunkLoader::GetChunk(const Hash& key) {
  return cs_->Get(key);
}

Chunk PartitionedChunkLoader::GetChunk(const Hash& key) {
  int id = ptt_->GetWorkerId(key);
  if (id == ptt_->id()) {
    return cs_->Get(key);
  } else {
    // TODO(anh): need to fetch that chunk from remote node
  }
  LOG(FATAL) << "Failed to load chunk";
  return Chunk();
}

Chunk ClientChunkLoader::GetChunk(const Hash& key) {
  Chunk chunk;
  ErrorCode code = db_->GetChunk(Slice(key_), key, &chunk);
  if (code != ErrorCode::kOK)
    LOG(WARNING) << "Failed to fetch chunk, error code: "
                 << static_cast<int>(code);
  return chunk;
}

}  // namespace ustore
