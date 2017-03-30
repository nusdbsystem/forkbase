// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_H_
#define USTORE_CHUNK_CHUNK_H_

#include <cstdint>
#include <memory>
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
  static constexpr size_t kNumBytesOffset = 0;
  static constexpr size_t kChunkTypeOffset = kNumBytesOffset + sizeof(uint32_t);
  static constexpr size_t kMetaLength = kChunkTypeOffset + sizeof(ChunkType);

  // allocate a new chunk with usable capacity (excluding meta data)
  explicit Chunk(ChunkType type, uint32_t capacity);
  // create chunk but not own the data
  explicit Chunk(const byte_t* head) : head_(head) {}
  // create chunk and let it own the data
  explicit Chunk(std::unique_ptr<byte_t[]>* head);
  ~Chunk() {}

  // total number of bytes
  inline uint32_t numBytes() const {
    return *reinterpret_cast<const uint32_t*>(head_ + kNumBytesOffset);
  }
  // type of the chunk
  inline ChunkType type() const {
    return *reinterpret_cast<const ChunkType*>(head_ + kChunkTypeOffset);
  }
  // number of bytes used to store actual data
  inline uint32_t capacity() const { return numBytes() - kMetaLength; }
  // pointer to the chunk
  inline const byte_t* head() const { return head_; }
  // pointer to actual data
  inline const byte_t* data() const { return head_ + kMetaLength; }
  // pointer to mutable data
  byte_t* m_data() const;
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
  // own the chunk if created by itself
  std::unique_ptr<byte_t[]> own_;
  // read-only chunk if passed from chunk storage
  const byte_t* head_ = nullptr;
  mutable Hash hash_;
};

}  // namespace ustore
#endif  // USTORE_CHUNK_CHUNK_H_
