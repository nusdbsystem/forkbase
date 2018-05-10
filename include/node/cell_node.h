// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_CELL_NODE_H_
#define USTORE_NODE_CELL_NODE_H_

#include <memory>
#include <utility>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "types/type.h"

namespace ustore {

class CellNode {
  /* CellNode contains a UCell
    | Type  | Num Pre Hash |
    | Utype | uint8_t      |
    | Key Length | Data Length | Context Length |
    | uint16_t   | uint16_t    | uint16_t       |
    | Pre Hash 1 | Pre Hash 2 | ... | Pre Hash k |
    | Hash::kByteLength * Num Pre Hash           |
    | Key        | Data        | Context        |
    | Key Length | Data Length | Context Length |
  */
 public:
  static Chunk NewChunk(UType type, const Slice& key, const Slice& data,
      const Slice& ctx, const Hash& preHash1, const Hash& preHash2);

  explicit CellNode(Chunk&& chunk) : chunk_(std::move(chunk)) {
    key_offset_ = ComputeKeyOffset(numPreHash());
    data_offset_ = key_offset_ + keyLength();
    ctx_offset_ = data_offset_ + dataLength();
  }
  ~CellNode() = default;

  inline UType type() const {
    return *reinterpret_cast<const UType*>(chunk_.data() + kUTypePos);
  }
  inline size_t numPreHash() const {
    return *reinterpret_cast<const uint8_t*>(chunk_.data() + kNumPreHashPos);
  }
  inline size_t keyLength() const {
    return *reinterpret_cast<const uint16_t*>(chunk_.data() + kKeyLengthPos);
  }
  inline size_t dataLength() const {
    return *reinterpret_cast<const int16_t*>(chunk_.data() + kDataLengthPos);
  }
  inline size_t ctxLength() const {
    return *reinterpret_cast<const int16_t*>(chunk_.data() + kCtxLengthPos);
  }
  inline size_t keyOffset() const { return key_offset_; }
  inline size_t dataOffset() const { return data_offset_; }
  inline size_t ctxOffset() const { return ctx_offset_; }

  inline Hash preHash(size_t idx) const {
    if (idx >= numPreHash()) return Hash();
    return Hash(chunk_.data() + ComputePreHashOffset(idx));
  }
  inline const byte_t* key() const {
    return keyLength() ? chunk_.data() + keyOffset() : nullptr;
  }
  inline const byte_t* data() const {
    return dataLength() ? chunk_.data() + dataOffset() : nullptr;
  }
  inline const byte_t* ctx() const {
    return ctxLength() ? chunk_.data() + ctxOffset() : nullptr;
  }

  // hash of this node
  inline Hash hash() const { return chunk_.hash(); }
  inline const Chunk& chunk() const { return chunk_; }

 private:
  static constexpr size_t kFieldMaxLength = 1<<16;
  static constexpr size_t kUTypePos = 0;
  static constexpr size_t kNumPreHashPos = kUTypePos + sizeof(UType);
  static constexpr size_t kKeyLengthPos = kNumPreHashPos + sizeof(uint8_t);
  static constexpr size_t kDataLengthPos = kKeyLengthPos + sizeof(uint16_t);
  static constexpr size_t kCtxLengthPos = kDataLengthPos + sizeof(uint16_t);
  static constexpr size_t kPreHashPos = kCtxLengthPos + sizeof(uint16_t);

  inline static size_t ComputePreHashOffset(size_t idx) {
    return kPreHashPos + Hash::kByteLength * idx;
  }
  inline static size_t ComputeKeyOffset(size_t num_pre_hash) {
    return ComputePreHashOffset(num_pre_hash);
  }

  size_t key_offset_;
  size_t data_offset_;
  size_t ctx_offset_;
  Chunk chunk_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CELL_NODE_H_
