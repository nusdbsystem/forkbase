// Copyright (c) 2017 The Ustore Authors.

#include "node/cell_node.h"

#include <cstdint>
#include <cstring>  // for memcpy
#include "utils/logging.h"

namespace ustore {

const Chunk* CellNode::NewChunk(const UType type, const Slice& key,
                                const Hash& dataHash, const Hash& preHash) {
  return NewChunk(type, key, dataHash, preHash, Hash());
}

const Chunk* CellNode::NewChunk(const UType type, const Slice& key,
                                const Hash& dataHash, const Hash& preHash1,
                                const Hash& preHash2) {
  // Check the first hash can not be empty
  CHECK(!preHash1.empty());
  bool merged = !preHash2.empty();
  size_t chunk_len = kChunkLen(merged, key.len());
  Chunk* chunk = new Chunk(ChunkType::kCell, chunk_len);
  *reinterpret_cast<UType*>(chunk->m_data() + kUTypeOffset) = type;
  std::memcpy(chunk->m_data() + kMergedOffset, &merged, sizeof(bool));
  std::memcpy(chunk->m_data() + kDataHashOffset, dataHash.value(),
              Hash::kByteLength);
  std::memcpy(chunk->m_data() + kPreHash1Offset, preHash1.value(),
              Hash::kByteLength);
  if (merged) {
    std::memcpy(chunk->m_data() + kPreHash2Offset, preHash2.value(),
                Hash::kByteLength);
  }
  cell_key_size_t key_len = static_cast<cell_key_size_t>(key.len());
  std::memcpy(chunk->m_data() + kCellKeyLenOffset(merged), &key_len,
              sizeof(cell_key_size_t));
  std::memcpy(chunk->m_data() + kCellKeyOffset(merged), key.data(), key.len());
  return chunk;
}

Hash CellNode::preHash(bool second) const {
  if (!second) return Hash(chunk_->data() + kPreHash1Offset);
  if (merged()) {
    return Hash(chunk_->data() + kPreHash2Offset);
  }
  return Hash();
}

}  // namespace ustore
