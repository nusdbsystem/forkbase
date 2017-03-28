// Copyright (c) 2017 The Ustore Authors.

#include "types/ustring.h"

#include <cstring>
#include "node/node_builder.h"
#include "store/chunk_store.h"
#include "utils/logging.h"

namespace ustore {

const UString* UString::Create(const byte_t* data, size_t num_bytes) {
  const Chunk* chunk = StringNode::NewChunk(data, num_bytes);
  store::GetChunkStore()->Put(chunk->hash(), *chunk);
  return new UString(chunk);
}

const UString* UString::Load(const Hash& hash) {
  // ustring do not need chunk loader, as it has only one chunk
  const Chunk* chunk = store::GetChunkStore()->Get(hash);
  return new UString(chunk);
}

UString::UString(const Chunk* chunk) {
  if (chunk->type() == ChunkType::kString) {
    node_ = new StringNode(chunk);
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UString";
  }
}

}  // namespace ustore
