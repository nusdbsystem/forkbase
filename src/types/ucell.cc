// Copyright (c) 2017 The Ustore Authors.

#include "types/ucell.h"

#include <cstring>
#include "node/node_builder.h"
#include "utils/logging.h"

namespace ustore {

const UCell* UCell::Create(UType data_type, const Hash& data_root_hash,
                           const Hash& preHash1, const Hash& preHash2) {
  const Chunk* chunk = CellNode::NewChunk(data_type, data_root_hash, preHash1,
                                          preHash2);
  ChunkStore* cs = ustore::GetChunkStore();
  cs->Put(chunk->hash(), *chunk);
  return new UCell(chunk);
}

const UCell* UCell::Load(const Hash& hash) {
  ustore::ChunkStore* cs = ustore::GetChunkStore();
  const Chunk* chunk = cs->Get(hash);
  return new UCell(chunk);
}

UCell::UCell(const Chunk* chunk) {
  if (chunk->type() == kCellChunk) {
    node_ = new CellNode(chunk);
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UCell";
  }
}

}  // namespace ustore
