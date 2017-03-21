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
  | type  | Data Hash     | Merged | Previous Hash 1 | Previous Hash 2 |
  | Utype | HASH_BYTE_LEN | bool   | HASH_BYTE_LEN   | HASH_BYTE_LEN   |
*/
 public:
  static const size_t UTYPE_OFFSET = 0;
  static const size_t MERGED_OFFSET = UTYPE_OFFSET + sizeof(Utype);
  static const size_t DATA_HASH_OFFSET = MERGED_OFFSET + sizeof(bool);
  static const size_t PRE_HASH_1_OFFSET = DATA_HASH_OFFSET + HASH_BYTE_LEN;
  static const size_t PRE_HASH_2_OFFSET = PRE_HASH_1_OFFSET + HASH_BYTE_LEN;

  static Chunk* CreateNewCellNode(const Utype type, const Hash& dataHash,
                                  const Hash& preHash1, const Hash& preHash2)

  explicit CellNode(const Chunk* chunk);
  ~CellNode();

  inline UType type() const {
    return *reinterpret_cast<UType*>(chunk_->data() + UTYPE_OFFSET);
  }

  inline bool merged() const {
    return *reinterpret_cast<bool*>(chunk_->data() + MERGED_OFFSET);
  }

  inline const Hash& dataHash() const {
    return Hash(chunk_->data() + DATA_HASH_OFFSET);
  }

  inline const Hash& preHash(bool second = false) const {
    byte_t* p = second ? PRE_HASH_2_OFFSET : PRE_HASH_1_OFFSET;
    return Hash(p);
  }

 private:
  const Chunk* chunk_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CELL_NODE_H_
