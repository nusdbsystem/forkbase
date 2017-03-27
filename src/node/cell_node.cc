// Copyright (c) 2017 The Ustore Authors.

#include "node/cell_node.h"

#include <cstdint>
#include <cstring>  // for memcpy
#include "utils/logging.h"

namespace ustore {

const Chunk* CellNode::NewChunk(const UType type, const Hash& dataHash) {
  return NewChunk(type, dataHash, Hash::NULL_HASH, Hash());
}

const Chunk* CellNode::NewChunk(const UType type, const Hash& dataHash,
                                const Hash& preHash1, const Hash& preHash2) {
  // Check the first hash can not be empty
  CHECK(!preHash1.empty());
  size_t chunk_len = PRE_HASH_2_OFFSET;
  bool merged = false;
  if (!preHash2.empty()) {
    chunk_len = PRE_HASH_2_OFFSET + HASH_BYTE_LEN;
    merged = true;
  }
  Chunk* chunk = new Chunk(kCellChunk, chunk_len);
  *reinterpret_cast<UType*>(chunk->m_data() + UTYPE_OFFSET) = type;
  memcpy(chunk->m_data() + MERGED_OFFSET, &merged, sizeof(bool));
  memcpy(chunk->m_data() + DATA_HASH_OFFSET, dataHash.value(), HASH_BYTE_LEN);
  memcpy(chunk->m_data() + PRE_HASH_1_OFFSET, preHash1.value(), HASH_BYTE_LEN);
  if (!preHash2.empty()) {
    memcpy(chunk->m_data() + PRE_HASH_2_OFFSET, preHash2.value(),
           HASH_BYTE_LEN);
  }
  return chunk;
}

const Hash CellNode::preHash(bool second) const {
  if (!second)
    return Hash(chunk_->data() + PRE_HASH_1_OFFSET);
  if (merged()) {
    CHECK_EQ(chunk_->capacity(), CELL_CHUNK_LENGTH_2_PRE_HASHS);
    return Hash(chunk_->data() + PRE_HASH_2_OFFSET);
  }
  return Hash();
}

}  // namespace ustore
