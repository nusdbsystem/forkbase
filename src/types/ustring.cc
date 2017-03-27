// Copyright (c) 2017 The Ustore Authors.

#include "types/ustring.h"

#include <cstring>
#include "node/node_builder.h"
#include "utils/logging.h"

namespace ustore {

const UString* UString::Create(const byte_t* data, size_t num_bytes) {
  ustore::ChunkStore* cs = ustore::GetChunkStore();
  const Chunk* chunk = StringNode::NewChunk(data, num_bytes);
  cs->Put(chunk->hash(), *chunk);
  return new UString(chunk);
}

const UString* UString::Load(const Hash& hash) {
  ustore::ChunkStore* cs = ustore::GetChunkStore();
  const Chunk* chunk = cs->Get(hash);
  return new UString(chunk);
}

UString::UString(const Chunk* chunk) {
  if (chunk->type() == kStringChunk) {
    node_ = new StringNode(chunk);
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UString";
  }
}

}  // namespace ustore
