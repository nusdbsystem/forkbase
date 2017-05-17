// Copyright (c) 2017 The Ustore Authors.

#include "store/chunk_store.h"

#include <iomanip>
#include <iostream>

#include "utils/iterator.h"
#include "utils/logging.h"
#ifdef USE_LEVELDB
#include "store/ldb_store.h"
#endif  // USE_LEVELDB
#include "store/lst_store.h"

namespace ustore {

std::unordered_map<ChunkType, std::string> chunkTypeNames = {
  {ChunkType::kNull, "kNull"},
  {ChunkType::kCell, "kCell"},
  {ChunkType::kMeta, "kMeta"},
  {ChunkType::kBlob, "kBlob"},
  {ChunkType::kString, "kString"},
  {ChunkType::kMap, "kMap"},
  {ChunkType::kList, "kList"}
};

void StoreInfo::Print() const {
  std::cout << std::setw(30)
    << "==============Storage Usage Information==============" << std::endl;
  std::cout << std::setw(30)
    << "Number of segments: " << segments << std::endl;
  std::cout << std::setw(30)
    << "Number of free segments: " << freeSegments << std::endl;
  std::cout << std::setw(30)
    << "Number of used segments: " << usedSegments << std::endl;
  std::cout << std::endl;

  std::cout << std::setw(30)
    << "Number of chunks: " << chunks << std::endl;
  std::cout << std::setw(30)
    << "Number of valid chunks: " << validChunks << std::endl;
  std::cout << std::setw(30)
    << "Bytes of chunks: " << chunkBytes << std::endl;
  std::cout << std::setw(30)
    << "Bytes of valid chunks: " << validChunkBytes << std::endl;
  for (auto type : Enum<ChunkType>()) {
    std::cout << std::setw(30)
      << "Number of " + chunkTypeNames[type] + " chunks: "
      << chunksPerType.at(type) << std::endl;
    std::cout << std::setw(30)
      << "Bytes of " + chunkTypeNames[type] + " chunks: "
      << bytesPerType.at(type) << std::endl;
  }
  std::cout << std::setw(30)
    << "====================================================" << std::endl;
}

namespace store {

ChunkStore* GetChunkStore() {
#ifdef USE_LEVELDB
  return LDBStore::Instance();
#endif
  return lst_store::LSTStore::Instance();
  LOG(FATAL) << "No chunk storage impl";
  return nullptr;
}

}  // namespace store
}  // namespace ustore
