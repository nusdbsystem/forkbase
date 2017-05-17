// Copyright (c) 2017 The Ustore Authors.

#include "store/chunk_store.h"

#include <cstdio>
#include <iomanip>
#include <iostream>

#include "utils/iterator.h"
#include "utils/logging.h"
#ifdef USE_LEVELDB
#include "store/ldb_store.h"
#endif  // USE_LEVELDB
#include "store/lst_store.h"

namespace ustore {

using std::cout;
using std::setw;
using std::left;
using std::right;
using std::endl;

std::unordered_map<ChunkType, std::string> chunkTypeNames = {
  {ChunkType::kNull, "Null"},
  {ChunkType::kCell, "Cell"},
  {ChunkType::kMeta, "Meta"},
  {ChunkType::kBlob, "Blob"},
  {ChunkType::kString, "String"},
  {ChunkType::kMap, "Map"},
  {ChunkType::kList, "List"}
};

const char* kReadableUnit = "BKMGTPE";

std::string Readable(size_t bytes) {
  std::ostringstream os;
  double v = bytes;
  for (int i = 0; kReadableUnit[i] != 0; ++i) {
    if (v < 1024.0 || !kReadableUnit[i+1]) {
      os << std::fixed << std::setprecision(2) << v << kReadableUnit[i];
      return os.str();
    }
    v /= 1024.0;
  }
  return os.str();
}

void StoreInfo::Print() const {
  constexpr int kSegmentAlign = 12;
  constexpr int kChunkAlign = 12;

  cout << "============= Storage Usage Information ==============" << endl;
  // segment type
  cout << "Segment Type"
       << " |" << setw(kSegmentAlign) << right << "Total"
       << " |" << setw(kSegmentAlign) << right << "Free"
       << " |" << setw(kSegmentAlign) << right << "Used"
       << endl;
  // segment count
  cout << "Count       "
       << " |" << setw(kSegmentAlign) << right << segments
       << " |" << setw(kSegmentAlign) << right << freeSegments
       << " |" << setw(kSegmentAlign) << right << usedSegments
       << endl;
  cout << "======================================================" << endl;
  // chunk meta
  cout << setw(kChunkAlign) << left << "Chunk Type"
       << " |" << setw(kChunkAlign) << right << "Count"
       << " |" << setw(kChunkAlign) << right << "Readable"
       << " |" << setw(kChunkAlign) << right << "Bytes"
       << endl;
  // chunk info
  cout << setw(kChunkAlign) << left << "Total"
       << " |" << setw(kChunkAlign) << right << chunks
       << " |" << setw(kChunkAlign) << right << Readable(chunkBytes)
       << " |" << setw(kChunkAlign) << right << chunkBytes
       << endl;
  cout << setw(kChunkAlign) << left << "Valid"
       << " |" << setw(kChunkAlign) << right << validChunks
       << " |" << setw(kChunkAlign) << right << Readable(validChunkBytes)
       << " |" << setw(kChunkAlign) << right << validChunkBytes
       << endl;
  for (auto type : Enum<ChunkType>())
    cout << setw(kChunkAlign) << left << chunkTypeNames[type]
         << " |" << setw(kChunkAlign) << right << chunksPerType.at(type)
         << " |" << setw(kChunkAlign) << right << Readable(bytesPerType.at(type))  // NOLINT
         << " |" << setw(kChunkAlign) << right << bytesPerType.at(type)
         << endl;
  cout << "======================================================" << endl;
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
