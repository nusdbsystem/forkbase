// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_CELL_NODE_H_
#define USTORE_NODE_CELL_NODE_H_

#include <cstddef>
#include <memory>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "types/type.h"

namespace ustore {

class CellNode {
  /* CellNode contains a UCell

    Encoding Scheme:
    | type  | Merged |     Data Hash     |
    | Utype |  bool  | Hash::kByteLength |
    | Previous Hash 1   |     Previous Hash 2         |
    | Hash::kByteLength | Hash::kByteLength(optional) |
    | Length of UCell Key |            UCell Key          |
    | uint16_t (2 bytes)  | specified by the second field |
  */
 public:
  // Create new chunk contains a new cell,
  // preHash2 is empty hash (Hash()) in this cell.
  static const Chunk* NewChunk(const UType type, const Slice& key,
                               const Hash& dataHash, const Hash& preHash);
  // Create new chunk contains a new cell based on
  // two previous versions, both previous versions are not empty.
  // Used for merge operation.
  static const Chunk* NewChunk(const UType type, const Slice& key,
                               const Hash& dataHash, const Hash& preHash1,
                               const Hash& preHash2);

  explicit CellNode(const Chunk* chunk) : chunk_(chunk) {}
  // cell node does not have chunkloader, need to delete chunk
  ~CellNode() {}

  inline UType type() const {
    return *reinterpret_cast<const UType*>(chunk_->data() + kUTypeOffset);
  }
  inline bool merged() const {
    return *reinterpret_cast<const bool*>(chunk_->data() + kMergedOffset);
  }
  inline Hash dataHash() const {
    return Hash(chunk_->data() + kDataHashOffset);
  }
  // return empty hash (Hash()) if
  // the request second prehash does not exist
  Hash preHash(bool second = false) const;

  inline size_t cellKeyLen() const {
    return static_cast<size_t>(
        *reinterpret_cast<const cell_key_size_t*>(chunk_->data() +
                                                  kCellKeyLenOffset(merged())));
  }

  inline Slice cellKey() const {
    return Slice(reinterpret_cast<const char*>(chunk_->data() +
                                               kCellKeyOffset(merged())),
                 cellKeyLen());
  }

  // hash of this node
  inline Hash hash() const { return chunk_->hash(); }

 private:
  static constexpr size_t kUTypeOffset = 0;
  static constexpr size_t kMergedOffset = kUTypeOffset + sizeof(UType);
  static constexpr size_t kDataHashOffset = kMergedOffset + sizeof(bool);
  static constexpr size_t kPreHash1Offset = kDataHashOffset + Hash::kByteLength;
  static constexpr size_t kPreHash2Offset = kPreHash1Offset + Hash::kByteLength;

  inline static size_t kCellKeyLenOffset(bool merged) {
    return Hash::kByteLength + (merged ? kPreHash2Offset : kPreHash1Offset);
  }

  inline static size_t kCellKeyOffset(bool merged) {
    return kCellKeyLenOffset(merged) + sizeof(cell_key_size_t);
  }

  inline static size_t kChunkLen(bool merged, size_t key_len) {
    return kCellKeyOffset(merged) + key_len;
  }

  std::unique_ptr<const Chunk> chunk_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CELL_NODE_H_
