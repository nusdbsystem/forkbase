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
    | type  | Merged |     Data Hash |  Previous Hash 1 | Previous Hash 2 |
    | Utype | bool   | HASH_BYTE_LEN | HASH_BYTE_LEN    |
    HASH_BYTE_LEN(optional)   |
  */
 public:
  // Create new chunk contains a new cell, preHash1 is Hash::NULL_HASH,
  // preHash2 is empty hash (Hash())
  static const Chunk* NewChunk(const UType type, const Hash& dataHash);
  // Create new chunk contains a new cell based on at least one pre-version,
  // preHash 2 is empty hash (Hash()) if non-exist
  static const Chunk* NewChunk(const UType type, const Hash& dataHash,
                               const Hash& preHash1, const Hash& preHash2);

  explicit CellNode(const Chunk* chunk) : chunk_(chunk) {}
  // cell node does not have chunkloader, need to delete chunk
  ~CellNode() { delete chunk_; }

  inline UType type() const {
    return *reinterpret_cast<const UType*>(chunk_->data() + UTYPE_OFFSET);
  }
  inline bool merged() const {
    return *reinterpret_cast<const bool*>(chunk_->data() + MERGED_OFFSET);
  }
  inline const Hash dataHash() const {
    return Hash(chunk_->data() + DATA_HASH_OFFSET);
  }
  // return empty hash (Hash()) if
  // the request second prehash does not exist
  const Hash preHash(bool second = false) const;

 private:
  static const size_t UTYPE_OFFSET = 0;
  static const size_t MERGED_OFFSET = UTYPE_OFFSET + sizeof(UType);
  static const size_t DATA_HASH_OFFSET = MERGED_OFFSET + sizeof(bool);
  static const size_t PRE_HASH_1_OFFSET = DATA_HASH_OFFSET + HASH_BYTE_LEN;
  static const size_t PRE_HASH_2_OFFSET = PRE_HASH_1_OFFSET + HASH_BYTE_LEN;
  static const size_t CELL_CHUNK_LENGTH_1_PRE_HASH = PRE_HASH_2_OFFSET;
  static const size_t CELL_CHUNK_LENGTH_2_PRE_HASHS =
      PRE_HASH_2_OFFSET + HASH_BYTE_LEN;

  const Chunk* chunk_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CELL_NODE_H_
