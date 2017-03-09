// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UCELL_H_
#define USTORE_TYPES_UCELL_H_

#include "types/type.h"
#include "hash/hash.h"
#include "chunk/chunk.h"

namespace ustore {
class UCell {
 public:
  static UCell Load(const Hash& unode_hash);

  // Create the chunk data and dump to storage
  // Return the UCell instance
  static UCell Create(const Hash& pre_hash,
                      Type data_type,
                      const Hash& data_root_hash);

  ~UBlob();  // remove root_node_;

  inline Type GetType() const { return node_->type();}
  const Hash GetPreUNodeHash() const;
  const Hash GetDataRootHash() const;

 private:
  // Hash of node_
  // Make sure they are in sync
  const Hash node_hash_;

  const CellNode* node_;

  // Private contructor to be called by Load() or Create()
  explicit UCell(const Chunk* chunk);

  UCell(const UCell&) = delete  // disable copy-constructor
  UCell& operator=(const UCell&) = delete;  // disable copy-assignment
};

}  // namespace ustore
#endif  // USTORE_TYPES_UCELL_H_
