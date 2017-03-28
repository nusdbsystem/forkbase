// Copyright (c) 2017 The Ustore Authors.

#include "store/chunk_store.h"

#include "utils/logging.h"
#include "utils/singleton.h"
#ifdef USE_LEVELDB
#include "store/ldb_store.h"
#endif  // USE_LEVELDB

namespace ustore {
namespace store {

ChunkStore* GetChunkStore() {
#ifdef USE_LEVELDB
  return Singleton<LDBStore>::Instance();
#endif
  LOG(FATAL) << "No chunk storage impl";
  return nullptr;
}

}  // namespace store
}  // namespace ustore
