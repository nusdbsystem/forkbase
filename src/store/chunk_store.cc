// Copyright (c) 2017 The Ustore Authors.

#include "store/chunk_store.h"

#include <cstdio>
#include <iomanip>

#include "utils/iterator.h"
#include "utils/logging.h"
#ifdef USE_LEVELDB
#include "store/ldb_store.h"
#endif  // USE_LEVELDB
#include "store/lst_store.h"

namespace ustore {

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

std::ostream& operator<<(std::ostream& os, const StoreInfo& obj) {
  constexpr int kSegmentAlign = 12;
  constexpr int kChunkAlign = 12;

  os << "============= Storage Usage Information ==============" << endl;
  // segment type
  os << setw(kSegmentAlign) << left << "Segment Type"
     << " |" << setw(kSegmentAlign) << right << "Total"
     << " |" << setw(kSegmentAlign) << right << "Free"
     << " |" << setw(kSegmentAlign) << right << "Used"
     << endl;
  // segment count
  os << setw(kSegmentAlign) << left << "Count"
     << " |" << setw(kSegmentAlign) << right << obj.segments
     << " |" << setw(kSegmentAlign) << right << obj.freeSegments
     << " |" << setw(kSegmentAlign) << right << obj.usedSegments
     << endl;
  os << "======================================================" << endl;
  // chunk meta
  os << setw(kChunkAlign) << left << "Chunk Type"
     << " |" << setw(kChunkAlign) << right << "Count"
     << " |" << setw(kChunkAlign) << right << "Readable"
     << " |" << setw(kChunkAlign) << right << "Bytes"
     << endl;
  // chunk info
  os << setw(kChunkAlign) << left << "Total"
     << " |" << setw(kChunkAlign) << right << obj.chunks
     << " |" << setw(kChunkAlign) << right << Readable(obj.chunkBytes)
     << " |" << setw(kChunkAlign) << right << obj.chunkBytes
     << endl;
  os << setw(kChunkAlign) << left << "Valid"
     << " |" << setw(kChunkAlign) << right << obj.validChunks
     << " |" << setw(kChunkAlign) << right << Readable(obj.validChunkBytes)
     << " |" << setw(kChunkAlign) << right << obj.validChunkBytes
     << endl;
  for (auto type : Enum<ChunkType>())
    os << setw(kChunkAlign) << left << chunkTypeNames[type]
       << " |" << setw(kChunkAlign) << right << obj.chunksPerType.at(type)
       << " |" << setw(kChunkAlign) << right << Readable(obj.bytesPerType.at(type))  // NOLINT
       << " |" << setw(kChunkAlign) << right << obj.bytesPerType.at(type)
       << endl;
  os << "======================================================" << endl;
  return os;
}

namespace store {

ChunkStore* InitChunkStore(const std::string& dir, const std::string& file,
                           bool persist) {
#ifdef USE_LEVELDB
  return LDBStore::MakeSingleton(file+"_ldb");
#endif
  return lst_store::LSTStore::MakeSingleton(dir, file, persist);
  LOG(FATAL) << "No chunk storage impl";
  return nullptr;
}

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
