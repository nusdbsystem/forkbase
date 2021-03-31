// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vblob.h"

namespace ustore {

VBlob::VBlob(const Slice& data) noexcept : UBlob(nullptr) {
  buffer_ = {UType::kBlob, {}, 0, 0, {data}, {}};
}

VBlob::VBlob(std::istream* is) noexcept : UBlob(nullptr) {
  buffer_ = {UType::kBlob, {}, 0, 0, {}, {}};
  useStream_ = true;
  dataStream_ = is;
}

VBlob::VBlob(std::shared_ptr<ChunkLoader> loader, const Hash& root_hash)
    noexcept : UBlob(loader) {
  SetNodeForHash(root_hash);
  buffer_ = {UType::kBlob, root_node_->hash(), 0, 0, {}, {}};
}

Hash VBlob::Splice(size_t pos, size_t num_delete, const byte_t* data,
                   size_t num_append) const {
  buffer_ = {UType::kBlob, root_node_->hash(), pos, num_delete,
             {Slice(reinterpret_cast<const char*>(data), num_append)}, {}};
  return Hash::kNull;
}

Hash VBlob::Splice(size_t pos, size_t num_delete, std::istream* is,
                   size_t num_append) const {
  buffer_ = {UType::kBlob, root_node_->hash(), pos, num_delete, {}, {}};
  useStream_ = true;
  dataStream_ = is;
  return Hash::kNull;
}

}  // namespace ustore
