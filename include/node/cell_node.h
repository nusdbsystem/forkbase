// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_CELL_NODE_H_
#define USTORE_NODE_CELL_NODE_H_

#include <cstddef>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "types/type.h"

namespace ustore {

class CellNode {
  /* CellNode contains a UCell

    Encoding Scheme:
    | type  | Merged | Data Hash         |  Previous Hash 1  |
    | Utype | bool   | Hash::kByteLength | Hash::kByteLength |
    |Previous Hash 2             |
    |Hash::kByteLength(optional) |
  */
 public:
  // Create new chunk contains a new cell,
  // preHash2 is empty hash (Hash()) in this cell.
  static const Chunk* NewChunk(const UType type, const Hash& dataHash,
                               const Hash& preHash);
  // Create new chunk contains a new cell based on
  // two previous versions, both previous versions are not empty.
  // Used for merge operation.
  static const Chunk* NewChunk(const UType type, const Hash& dataHash,
                               const Hash& preHash1, const Hash& preHash2);

  explicit CellNode(const Chunk* chunk) : chunk_(chunk) {}
  // cell node does not have chunkloader, need to delete chunk
  ~CellNode() { delete chunk_; }

  inline UType type() const {
    return *reinterpret_cast<const UType*>(chunk_->data() + kUTypeOffset);
  }
  inline bool merged() const {
    return *reinterpret_cast<const bool*>(chunk_->data() + kMergedOffset);
  }
  inline const Hash dataHash() const {
    return Hash(chunk_->data() + kDataHashOffset);
  }
  // return empty hash (Hash()) if
  // the request second prehash does not exist
  const Hash preHash(bool second = false) const;

  // hash of this node
  inline const Hash hash() const { return chunk_->hash(); }

 private:
  static constexpr size_t kUTypeOffset = 0;
  static constexpr size_t kMergedOffset = kUTypeOffset + sizeof(UType);
  static constexpr size_t kDataHashOffset = kMergedOffset + sizeof(bool);
  static constexpr size_t kPreHash1Offset = kDataHashOffset + Hash::kByteLength;
  static constexpr size_t kPreHash2Offset = kPreHash1Offset + Hash::kByteLength;
  static constexpr size_t kChunkLength1PreHash = kPreHash2Offset;
  static constexpr size_t kChunkLength2PreHash =
      kPreHash2Offset + Hash::kByteLength;

  const Chunk* chunk_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CELL_NODE_H_
