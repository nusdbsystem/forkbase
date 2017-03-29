// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_H_
#define USTORE_CHUNK_CHUNK_H_

#include <cstdint>
#include <string>
#include "types/type.h"
#include "hash/hash.h"
#include "utils/noncopyable.h"

namespace ustore {

class Chunk : private Noncopyable {
 public:
  /*
   * Chunk format:
   * | uint32_t  | ChunkType | Bytes |
   * | num_bytes | type      | data  |
   */
  static const size_t NUM_BYTES_OFFSET = 0;
  static const size_t CHUNK_TYPE_OFFSET = NUM_BYTES_OFFSET + sizeof(uint32_t);
  static const size_t META_SIZE = CHUNK_TYPE_OFFSET + sizeof(ChunkType);

  // allocate a new chunk with usable capacity (excluding meta data)
  Chunk(ChunkType type, uint32_t capacity, byte_t* data) 
      : own_(true), head_(new byte_t[META_SIZE + capacity]) {
    *reinterpret_cast<uint32_t*>(head_ + NUM_BYTES_OFFSET) =
        META_SIZE + capacity;
    *reinterpret_cast<ChunkType*>(head_ + CHUNK_TYPE_OFFSET) = type;
    std::copy(data, data + capacity, head_);
  }

  Chunk(const byte_t* head, bool own = false) noexcept : own_(own), head_(head) {}

  ~Chunk() {
    if (own_) delete[] head_;
  }

  // total number of bytes
  inline uint32_t numBytes() const noexcept{
    return *reinterpret_cast<uint32_t*>(head_ + NUM_BYTES_OFFSET);
  }
  // type of the chunk
  inline ChunkType type() const noexcept{
    return *reinterpret_cast<ChunkType*>(head_ + CHUNK_TYPE_OFFSET);
  }
  // number of bytes used to store actual data
  inline uint32_t capacity() const noexcept{ return numBytes() - META_SIZE; }
  // pointer to the chunk
  inline const byte_t* head() const noexcept{ return head_; }
  // pointer to actual data
  inline const byte_t* data() const noexcept{ return head_ + META_SIZE; }
  // chunk hash
  inline const Hash& hash() const {
    return hash_.empty() ? forceHash() : hash_;
  }
  // force to re-compute chunk hash
  inline const Hash& forceHash() const {
    hash_.Compute(head_, numBytes());
    return hash_;
  }

 private:
  bool own_ = false;
  byte_t* head_ = nullptr;
  mutable Hash hash_;
};

}  // namespace ustore
#endif  // USTORE_CHUNK_CHUNK_H_
