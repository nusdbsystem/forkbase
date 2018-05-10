// Copyright (c) 2017 The Ustore Authors.

#include "node/cell_node.h"

#include <cstdint>
#include <cstring>  // for memcpy

namespace ustore {

Chunk CellNode::NewChunk(const UType type, const Slice& key, const Slice& data,
    const Slice& ctx, const Hash& preHash1, const Hash& preHash2) {
  // First hash can not be empty
  CHECK(!preHash1.empty());
  // key/data/ctx size can not be larger than 1<<16
  CHECK(key.len() < kFieldMaxLength) << "Key length ("<< key.len() <<") exceeds limit";  // NOLINT
  CHECK(data.len() < kFieldMaxLength) << "Data length ("<< data.len() <<") exceeds limit";  // NOLINT
  CHECK(ctx.len() < kFieldMaxLength) << "Context length ("<< ctx.len() <<") exceeds limit";  // NOLINT
  size_t num_pre_hash = preHash2.empty() ? 1 : 2;
  size_t key_offset = ComputeKeyOffset(num_pre_hash);
  size_t data_offset = key_offset + key.len();
  size_t ctx_offset = data_offset + data.len();
  size_t chunk_len = ctx_offset + ctx.len();
  Chunk chunk(ChunkType::kCell, chunk_len);
  // mete fields
  *reinterpret_cast<UType*>(chunk.m_data() + kUTypePos) = type;
  *reinterpret_cast<uint8_t*>(chunk.m_data() + kNumPreHashPos) = num_pre_hash;
  *reinterpret_cast<uint16_t*>(chunk.m_data() + kKeyLengthPos) = key.len();
  *reinterpret_cast<int16_t*>(chunk.m_data() + kDataLengthPos) = data.len();
  *reinterpret_cast<int16_t*>(chunk.m_data() + kCtxLengthPos) = ctx.len();
  // pre hash
  std::memcpy(chunk.m_data() + ComputePreHashOffset(0), preHash1.value(),
              Hash::kByteLength);
  if (num_pre_hash == 2)
    std::memcpy(chunk.m_data() + ComputePreHashOffset(1), preHash2.value(),
                Hash::kByteLength);
  // key
  std::memcpy(chunk.m_data() + key_offset, key.data(), key.len());
  // data
  std::memcpy(chunk.m_data() + data_offset, data.data(), data.len());
  // context
  std::memcpy(chunk.m_data() + ctx_offset, ctx.data(), ctx.len());
  return chunk;
}

}  // namespace ustore
