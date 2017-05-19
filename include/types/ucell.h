// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UCELL_H_
#define USTORE_TYPES_UCELL_H_

#include <memory>
#include <utility>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "node/cell_node.h"
#include "spec/slice.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class UCell : private Noncopyable {
 public:
  // Create the chunk data and dump to storage
  // Return the UCell instance
  static UCell Create(UType data_type, const Slice& key,
                      const Hash& data_root_hash, const Hash& preHash1,
                      const Hash& preHash2);
  static UCell Load(const Hash& unode_hash);

  UCell() = default;
  UCell(UCell&& ucell) : node_(std::move(ucell.node_)) {}
  ~UCell() = default;

  UCell& operator=(UCell&& ucell) {
    std::swap(node_, ucell.node_);
    return *this;
  }

  inline bool empty() const { return node_.get() == nullptr; }
  inline UType type() const { return node_->type(); }
  inline bool merged() const { return node_->merged(); }
  inline Hash dataHash() const { return node_->dataHash(); }
  // return empty hash (Hash()) if
  // the request second prehash does not exist
  inline Hash preHash(bool second = false) const {
    return node_->preHash(second);
  }
  inline Slice key() const { return node_->key(); }
  // hash of this ucell
  inline Hash hash() const { return node_->hash(); }

 private:
  // Private contructor to be called by Load() or Create()
  explicit UCell(Chunk&& chunk);

  std::unique_ptr<const CellNode> node_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_UCELL_H_
