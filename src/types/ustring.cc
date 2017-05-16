// Copyright (c) 2017 The Ustore Authors.

#include "types/ustring.h"

#include <cstring>

#include "utils/logging.h"

namespace ustore {

bool UString::SetNodeForHash(const Hash& hash) {
  const Chunk* chunk = chunk_loader_->Load(hash);
  if (chunk == nullptr) return false;

  if (chunk->type() == ChunkType::kString) {
    node_.reset(new StringNode(chunk));
    return true;
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UString";
  }
  return false;
}

}  // namespace ustore
