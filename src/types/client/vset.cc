// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vset.h"

namespace ustore {

VSet::VSet(const std::vector<Slice>& keys)
    noexcept {
  buffer_ = {UType::kSet, {}, 0, 0, {}, keys};
}

VSet::VSet(std::shared_ptr<ChunkLoader> loader, const Hash& root_hash)
    noexcept : USet(loader) {
  SetNodeForHash(root_hash);
  buffer_ = {UType::kSet, root_node_->hash(), 0, 0, {}, {}};
}

Hash VSet::Set(const Slice& key) const {
  buffer_ = {UType::kSet, root_node_->hash(), 0, 0, {}, {key}};
  return Hash::kNull;
}

Hash VSet::Remove(const Slice& key) const {
  buffer_ = {UType::kSet, root_node_->hash(), 0, 1, {}, {key}};
  return Hash::kNull;
}

}  // namespace ustore
