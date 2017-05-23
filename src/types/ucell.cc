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
  Chunk chunk = CellNode::NewChunk(data_type, key, data_root_hash, preHash1,
                                   preHash2);
  store::GetChunkStore()->Put(chunk.hash(), chunk);
  return UCell(std::move(chunk));
}

UCell UCell::Load(const Hash& hash) {
  // ucell do not need chunk loader, as it has only one chunk
  return UCell(store::GetChunkStore()->Get(hash));
}

UCell::UCell(Chunk&& chunk) {
  if (chunk.empty()) {
    LOG(WARNING) << "Empty Chunk. Loading Failed. ";
  } else if (chunk.type() == ChunkType::kCell) {
    node_.reset(new CellNode(std::move(chunk)));
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UCell"
               << " (actual type: " << static_cast<int>(chunk.type()) << ")";
  }
}

}  // namespace ustore
