// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_LST_STORE_H_
#define USTORE_STORE_LST_STORE_H_

#include <cstdint>
#include <cstring>

#include <algorithm>
#include <array>
#include <functional>
#include <limits>
#include <unordered_map>

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "store/chunk_store.h"
#include "types/type.h"
#include "utils/chars.h"
#include "utils/noncopyable.h"
#include "utils/singleton.h"

namespace ustore {
namespace lst_store {

struct LSTHash {
  const byte_t* hash_;
  LSTHash(const byte_t* ptr) noexcept : hash_(ptr) {}  // NOLINT
  bool operator==(const LSTHash& rhs) const noexcept {
    return std::memcmp(hash_, rhs.hash_, Hash::kByteLength) == 0;
  }
  Hash ToHash() {
    return Hash(const_cast<byte_t*>(hash_));
  }
};

}  // namespace lst_store
}  // namespace ustore

namespace std {
template<> struct hash<::ustore::lst_store::LSTHash> {
  size_t operator()(const ::ustore::lst_store::LSTHash& key) const noexcept {
    size_t ret;
    std::copy(key.hash_, key.hash_ + sizeof(ret),
              reinterpret_cast<::ustore::byte_t*>(&ret));
    return ret;
  }
};
}  // namespace std

namespace ustore {
namespace lst_store {

const size_t kMetaLogSize = 4096;
const size_t kSegmentSize = (1<<22);
const size_t kNumSegments = 16;
const size_t kLogFileSize = kSegmentSize * kNumSegments+ kMetaLogSize;
const size_t kMaxPendingSyncChunks = 1024;
const uint64_t kMaxSyncTimeoutMilliseconds = 3000;

struct LSTChunk {
  const byte_t* chunk_;
  LSTChunk(const byte_t* ptr) noexcept: chunk_(ptr) {}  // NOLINT
  const Chunk* toChunk() {
    return new Chunk(chunk_);
  }
};

/**
 * @brief layout: 8-byte prev offset | 8-byte next offset | [20-byte hash,
 * chunks] | 20-byte hash which is the same as the hash of the first chunk
 * and acts as the crc code
 */
struct LSTSegment {
  static void* base_addr_;
  LSTSegment *prev_, *next_;
  size_t nchunks_;
  void* segment_;

  inline explicit LSTSegment(void* segment) noexcept : segment_(segment) {}
};

class LSTStore
    : private Noncopyable, public Singleton<LSTStore>, public ChunkStore {
  friend class Singleton<LSTStore>;

 public:
  void Sync();
  virtual const Chunk* Get(const Hash& key);
  virtual bool Put(const Hash& key, const Chunk& chunk);

 private:
  // TODO(qingchao): add a remove-old-log flag to ease unit test
  inline LSTStore(const char* dir = nullptr,
                  const char* log_file = "ustore.log") {
    MmapUstoreLogFile(dir, log_file);
  }
  ~LSTStore();

  inline size_t GetFreeSpaceMajor() {
    return kSegmentSize - ::ustore::Hash::kByteLength - major_segment_offset_;
  }

  inline size_t GetFreeSpaceMinor() {
    return kSegmentSize - ::ustore::Hash::kByteLength - minor_segment_offset_;
  }
  void GC() {}
  void Load(void*);
  size_t LoadFromValidSegment(LSTSegment*);
  size_t LoadFromLastSegment(LSTSegment*);
  void* MmapUstoreLogFile(const char* dir, const char* log_file);
  LSTSegment* Allocate(LSTSegment*);
  LSTSegment* AllocateMajor();
  LSTSegment* AllocateMinor();

  std::unordered_map<LSTHash, LSTChunk> chunk_map_;
  LSTSegment *free_list_, *major_list_, *minor_list_;
  LSTSegment *current_major_segment_;
  LSTSegment *current_minor_segment_;
  size_t major_segment_offset_;
  size_t minor_segment_offset_;
};

}  // namespace lst_store
}  // namespace ustore

#endif  // USTORE_STORE_LST_STORE_H_
