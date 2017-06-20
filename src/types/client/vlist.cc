// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vlist.h"

namespace ustore {

VList::VList(const std::vector<Slice>& data) noexcept : UList(nullptr) {
  buffer_ = {UType::kList, {}, 0, 0, data, {}};
}

VList::VList(std::shared_ptr<ChunkLoader> loader, const Hash& root_hash)
    noexcept : UList(loader) {
  SetNodeForHash(root_hash);
  buffer_ = {UType::kList, root_node_->hash(), 0, 0, {}, {}};
}

Hash VList::Splice(uint64_t start_idx, uint64_t num_to_delete,
                   const std::vector<Slice>& entries) const {
  buffer_ = {UType::kList, root_node_->hash(), start_idx, num_to_delete,
             entries, {}};
  return Hash::kNull;
}

}  // namespace ustore
