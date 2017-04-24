// Copyright (c) 2017 The Ustore Authors.

#include "types/ucell.h"

#include <cstring>
#include "node/node_builder.h"
#include "store/chunk_store.h"
#include "utils/logging.h"

namespace ustore {

UCell UCell::Create(UType data_type, const Slice& key,
                    const Hash& data_root_hash, const Hash& preHash1,
                    const Hash& preHash2) {
  const Chunk* chunk =
      CellNode::NewChunk(data_type, key, data_root_hash, preHash1, preHash2);
  store::GetChunkStore()->Put(chunk->hash(), *chunk);
  return UCell(chunk);
}

UCell UCell::Load(const Hash& hash) {
  // ucell do not need chunk loader, as it has only one chunk
  const Chunk* chunk = store::GetChunkStore()->Get(hash);
  return UCell(chunk);
}

UCell::UCell(const Chunk* chunk) {
  if (chunk->type() == ChunkType::kCell) {
    node_.reset(new CellNode(chunk));
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UCell";
  }
}

}  // namespace ustore
