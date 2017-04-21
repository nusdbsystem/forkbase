// Copyright (c) 2017 The Ustore Authors.

#include "types/ustring.h"

#include <cstring>

#include "node/node_builder.h"
#include "store/chunk_store.h"
#include "utils/logging.h"

namespace ustore {
bool UString::SetNodeForHash(const Hash& hash) {
  const Chunk* chunk = chunk_loader_->Load(hash);
  if (chunk == nullptr) return false;

  if (chunk->type() == ChunkType::kString) {
    node_.reset(new StringNode(chunk));
    return true;
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UString";
  }
  return false;
}

SString::SString(const Slice& data) noexcept :
    UString(std::make_shared<ChunkLoader>()) {
  const Chunk* chunk = StringNode::NewChunk(
                           reinterpret_cast<const byte_t*>(data.data()),
                           data.len());
  store::GetChunkStore()->Put(chunk->hash(), *chunk);
  SetNodeForHash(chunk->hash());
}

SString::SString(const Hash& hash) noexcept :
    UString(std::make_shared<ChunkLoader>()) {
  // ustring do not need chunk loader, as it has only one chunk
  SetNodeForHash(hash);
}
}  // namespace ustore
