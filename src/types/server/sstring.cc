// Copyright (c) 2017 The Ustore Authors.

#include "types/server/sstring.h"

#include "node/string_node.h"
#include "node/node_builder.h"

namespace ustore {

SString::SString(const Slice& data) noexcept :
    UString(std::make_shared<ChunkLoader>()) {
  Chunk chunk = StringNode::NewChunk(
                reinterpret_cast<const byte_t*>(data.data()), data.len());
  store::GetChunkStore()->Put(chunk.hash(), chunk);
  SetNodeForHash(chunk.hash());
}

SString::SString(const Hash& hash) noexcept :
    UString(std::make_shared<ChunkLoader>()) {
  SetNodeForHash(hash);
}

}  // namespace ustore
