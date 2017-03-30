// Copyright (c) 2017 The Ustore Authors.

#include "node/cell_node.h"

#include <cstdint>
#include <cstring>  // for memcpy
#include "utils/logging.h"

namespace ustore {

const Chunk* CellNode::NewChunk(const UType type, const Hash& dataHash,
                                const Hash& preHash) {
  return NewChunk(type, dataHash, preHash, Hash());
}

const Chunk* CellNode::NewChunk(const UType type, const Hash& dataHash,
                                const Hash& preHash1, const Hash& preHash2) {
  // Check the first hash can not be empty
  CHECK(!preHash1.empty());
  size_t chunk_len = kChunkLength1PreHash;
  bool merged = false;
  if (!preHash2.empty()) {
    chunk_len = kChunkLength2PreHash;
    merged = true;
  }
  Chunk* chunk = new Chunk(ChunkType::kCell, chunk_len);
  *reinterpret_cast<UType*>(chunk->m_data() + kUTypeOffset) = type;
  std::memcpy(chunk->m_data() + kMergedOffset, &merged, sizeof(bool));
  std::memcpy(chunk->m_data() + kDataHashOffset, dataHash.value(),
              Hash::kByteLength);
  std::memcpy(chunk->m_data() + kPreHash1Offset, preHash1.value(),
              Hash::kByteLength);
  if (!preHash2.empty()) {
    std::memcpy(chunk->m_data() + kPreHash2Offset, preHash2.value(),
                Hash::kByteLength);
  }
  return chunk;
}

const Hash CellNode::preHash(bool second) const {
  if (!second) return Hash(chunk_->data() + kPreHash1Offset);
  if (merged()) {
    CHECK_EQ(chunk_->capacity(), kChunkLength2PreHash);
    return Hash(chunk_->data() + kPreHash2Offset);
  }
  return Hash();
}

}  // namespace ustore
