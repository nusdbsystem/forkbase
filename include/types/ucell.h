// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UCELL_H_
#define USTORE_TYPES_UCELL_H_

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class UCell : private Noncopyable {
 public:
  static UCell Load(const Hash& unode_hash);
  // Create the chunk data and dump to storage
  // Return the UCell instance
  static UCell Create(const Hash& pre_hash, Type data_type,
                      const Hash& data_root_hash);

  ~UCell();  // remove node_;

  inline Type type() const { return node_->type(); }
  const Hash preUNodeHash() const;
  const Hash dataHash() const;

 private:
  // Private contructor to be called by Load() or Create()
  explicit UCell(const Chunk* chunk);

  // Hash of node_
  // Make sure they are in sync
  const Hash node_hash_;
  const CellNode* node_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_UCELL_H_
