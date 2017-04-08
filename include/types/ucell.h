// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UCELL_H_
#define USTORE_TYPES_UCELL_H_

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "node/cell_node.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class UCell : private Noncopyable {
 public:
  // Create the chunk data and dump to storage
  // Return the UCell instance
  static const UCell* Create(UType data_type, const Hash& data_root_hash,
                             const Hash& preHash1, const Hash& preHash2);
  static const UCell* Load(const Hash& unode_hash);

  ~UCell() { delete node_; }  // remove node_;

  inline UType type() const { return node_->type(); }
  inline const bool merged() const { return node_->merged(); }
  inline const Hash dataHash() const { return node_->dataHash(); }
  // return empty hash (Hash()) if
  // the request second prehash does not exist
  inline const Hash preUNodeHash(bool second = false) const {
    return node_->preHash(second);
  }

  // hash of this ucell
  inline const Hash hash() const { return node_->hash(); }

 private:
  // Private contructor to be called by Load() or Create()
  explicit UCell(const Chunk* chunk);

  const CellNode* node_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_UCELL_H_
