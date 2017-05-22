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
  static Chunk NewChunk(const UType type, const Slice& key,
                        const Hash& dataHash, const Hash& preHash);
  // Create new chunk contains a new cell based on
  // two previous versions, both previous versions are not empty.
  // Used for merge operation.
  static Chunk NewChunk(const UType type, const Slice& key,
                        const Hash& dataHash, const Hash& preHash1,
                        const Hash& preHash2);

  explicit CellNode(Chunk&& chunk) : chunk_(std::move(chunk)) {}
  // cell node does not have chunkloader, need to delete chunk
  ~CellNode() {}

  inline UType type() const {
    return *reinterpret_cast<const UType*>(chunk_.data() + kUTypeOffset);
  }
  inline bool merged() const {
    return *reinterpret_cast<const bool*>(chunk_.data() + kMergedOffset);
  }
  inline Hash dataHash() const {
    return Hash(chunk_.data() + kDataHashOffset);
  }
  // return empty hash (Hash()) if
  // the request second prehash does not exist
  Hash preHash(bool second = false) const;
  inline size_t keyLength() const {
    return static_cast<size_t>(*reinterpret_cast<const key_size_t*>(
          chunk_.data() + kKeyLenOffset(merged())));
  }
  inline Slice key() const {
    return Slice(chunk_.data() + kKeyOffset(merged()), keyLength());
  }
  // hash of this node
  inline Hash hash() const { return chunk_.hash(); }
  inline const Chunk& chunk() const { return chunk_; }

 private:
  static constexpr size_t kUTypeOffset = 0;
  static constexpr size_t kMergedOffset = kUTypeOffset + sizeof(UType);
  static constexpr size_t kDataHashOffset = kMergedOffset + sizeof(bool);
  static constexpr size_t kPreHash1Offset = kDataHashOffset + Hash::kByteLength;
  static constexpr size_t kPreHash2Offset = kPreHash1Offset + Hash::kByteLength;

  inline static size_t kKeyLenOffset(bool merged) {
    return Hash::kByteLength + (merged ? kPreHash2Offset : kPreHash1Offset);
  }

  inline static size_t kKeyOffset(bool merged) {
    return kKeyLenOffset(merged) + sizeof(key_size_t);
  }

  inline static size_t kChunkLength(bool merged, size_t key_len) {
    return kKeyOffset(merged) + key_len;
  }

  Chunk chunk_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CELL_NODE_H_
