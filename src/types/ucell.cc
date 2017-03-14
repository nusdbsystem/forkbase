// Copyright (c) 2017 The Ustore Authors.

#include <cstring>

#include "types/ucell.h"

#include "node/node_builder.h"

#ifdef USE_LEVELDB
#include "store/ldb_store.h"
#include "utils/singleton.h"
#endif  // USE_LEVELDB

#include "utils/logging.h"

namespace ustore {
UCell::UCell(const Chunk* chunk) {
  if (chunk->type() == kCellChunk) {
    node_ = new CellNode(chunk);
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UCell";
  }
}

const UCell* UCell::Create(UType data_type,
                           const Hash& data_root_hash,
                           const Hash& preHash1,
                           const Hash& preHash2) {
  const Chunk* chunk = CellNode::NewChunk(data_type,
                                          data_root_hash,
                                          preHash1,
                                          preHash2);
  #ifdef USE_LEVELDB
  ustore::ChunkStore* cs = ustore::Singleton<ustore::LDBStore>::Instance();
  #else
  // other storage
  #endif  // USE_LEVELDB

  cs->Put(chunk->hash(), *chunk);

  return new UCell(chunk);
}

const UCell* UCell::Load(const Hash& hash) {
  #ifdef USE_LEVELDB
  ustore::ChunkStore* cs = ustore::Singleton<ustore::LDBStore>::Instance();
  #else
  // other storage
  #endif  // USE_LEVELDB

  const Chunk* chunk = cs->Get(hash);
  return new UCell(chunk);
}

UCell::~UCell() {
  delete node_;
}
}  // namespace ustore
