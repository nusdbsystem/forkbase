// Copyright (c) 2017 The Ustore Authors.

#include "chunk/chunk.h"

#include <utility>

namespace ustore {

Chunk::Chunk(ChunkType type, uint32_t capacity)
    : own_(new byte_t[kMetaLength + capacity]) {
  head_ = own_.get();
  *reinterpret_cast<uint32_t*>(&own_[kNumBytesOffset]) = kMetaLength + capacity;
  *reinterpret_cast<ChunkType*>(&own_[kChunkTypeOffset]) = type;
}

Chunk::Chunk(std::unique_ptr<byte_t[]> head) noexcept : own_(std::move(head)) {
  head_ = own_.get();
}

}  // namespace ustore
