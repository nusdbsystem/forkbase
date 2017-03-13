// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CELL_NODE_H_
#define USTORE_TYPES_CELL_NODE_H_

#include <cstddef>

#include "chunk/chunk.h"
#include "types/type.h"

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

  Type type() const;
  const byte_t* prevHashValue() const;
  const byte_t* dataHashValue() const;

 private:
  const Chunk* chunk_;
};
}  // namespace ustore

#endif  // USTORE_TYPES_CELL_NODE_H_
