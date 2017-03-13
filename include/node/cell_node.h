// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_NODE_CELL_NODE_H_
#define USTORE_TYPES_NODE_CELL_NODE_H_

#include <cstddef>

#include "chunk/chunk.h"
#include "types/type.h"
#include "hash/hash.h"

namespace ustore {
class CellNode {
/* CellNode contains a UCell

  Encoding Scheme:
  | -pre_ucell_hash_value- | -data-root-hash_value-- | --type--|
  | --------- -- --------- 20 -----------------------40 ------ 41
*/
 public:
  explicit CellNode(const Chunk* chunk);
  ~CellNode();

  inline Type type() const { return Type(*(chunk_->data() + 40));}

  inline const byte_t* prevHashValue() const {
    return chunk_->data();
  }
  inline const byte_t* dataHashValue() const {
    return chunk_->data() + HASH_BYTE_LEN;
  }

 private:
  const Chunk* chunk_;
};
}  // namespace ustore

#endif  // USTORE_TYPES_CELL_NODE_H_
