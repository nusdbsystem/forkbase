// Copyright (c) 2017 The Ustore Authors.

#include <cstring>

#include "types/ustring.h"

#include "node/node_builder.h"

#ifdef USE_LEVELDB
#include "store/ldb_store.h"
#include "utils/singleton.h"
#endif  // USE_LEVELDB

#include "utils/logging.h"

namespace ustore {
const UString* UString::Load(const Hash& hash) {
  #ifdef USE_LEVELDB
  ustore::ChunkStore* cs = ustore::Singleton<ustore::LDBStore>::Instance();
  #else
  // other storage
  #endif  // USE_LEVELDB

  const Chunk* chunk = cs->Get(hash);

  return new UString(chunk);
}

const UString* UString::Create(const byte_t* data, size_t num_bytes) {
  #ifdef USE_LEVELDB
  ustore::ChunkStore* cs = ustore::Singleton<ustore::LDBStore>::Instance();
  #else
  // other storage
  #endif  // USE_LEVELDB

  const Chunk* chunk = StringNode::NewChunk(data, num_bytes);
  cs->Put(chunk->hash(), *chunk);
  return new UString(chunk);
}

UString::UString(const Chunk* chunk) {
  if (chunk->type() == kStringChunk) {
    node_ = new StringNode(chunk);
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UString";
  }
}

UString::~UString() {
  delete node_;
}
}  // namespace ustore
