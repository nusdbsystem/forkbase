// Copyright (c) 2017 The Ustore Authors.

#include "node/string_node.h"

#include <cstdint>
#include <cstring>  // for memcpy

namespace ustore {

const Chunk* StringNode::NewChunk(const byte_t* data, size_t num_bytes) {
  Chunk* chunk = new Chunk(ustore::kStringChunk, sizeof(uint32_t) + num_bytes);
  *reinterpret_cast<uint32_t*>(chunk->m_data()) = num_bytes;
  std::memcpy(chunk->m_data() + sizeof(uint32_t), data, num_bytes);
  return chunk;
}

size_t StringNode::len() const {
  // read the first 4 bytes of chunk as uint32_t
  uint32_t len = *(reinterpret_cast<const uint32_t*>(chunk_->data()));
  return len;
}

size_t StringNode::Copy(byte_t* buffer) const {
  size_t byte_offset = sizeof(uint32_t);  // across str_len field (4 Bytes)
  size_t str_len = len();
  memcpy(buffer, chunk_->data() + byte_offset, str_len);
  return str_len;
}

}  // namespace ustore
