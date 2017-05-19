// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vstring.h"

namespace ustore {

VString::VString(const Slice& data) noexcept : UString(nullptr) {
  buffer_ = {UType::kString, Hash::kNull, 0, 0, {data}, {}};
}

VString::VString(std::shared_ptr<ChunkLoader> loader, const Hash& root_hash)
    noexcept : UString(loader) {
  SetNodeForHash(root_hash);
}

}  // namespace ustore
