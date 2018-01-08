// Copyright (c) 2017 The Ustore Authors.

#ifdef USE_ROCKSDB

#include <utility>
#include "utils/enum.h"

#include "store/rocks_store.h"

namespace ustore {

static const size_t kCacheSizeBytes(128 << 20);
static const size_t kWriteBufferSize(256 << 20);
static const uint64_t kMemtableMemoryBudget(1 << 30);

RocksStore::RocksStore() : RocksStore("/tmp/ustore.store", false) {}

RocksStore::RocksStore(const std::string& db_path, const bool persist)
  : persist_(persist) {
  db_opts_.error_if_exists = false;
  db_opts_.OptimizeLevelStyleCompaction(kMemtableMemoryBudget);
  db_opts_.write_buffer_size = kWriteBufferSize;
  db_blk_tab_opts_.block_cache = rocksdb::NewClockCache(kCacheSizeBytes);

  CHECK(OpenDB(db_path));

  InitStoreInfo();
#ifdef ENABLE_STORE_INFO
  DBFullScan([this](const rocksdb::Iterator * it) {
    UpdateStoreInfoForNewChunk(ToChunk(it->value()));
  });
#endif
}

RocksStore::~RocksStore() {
  persist_ ? CloseDB() : static_cast<void>(DestroyDB());
}

Chunk RocksStore::Get(const Hash& key) {
  rocksdb::PinnableSlice value;
  if (DBGet(ToRocksSlice(key), &value)) {
    auto chunk = ToChunk(value);
    DCHECK(key == chunk.hash());
    return chunk;
  } else {
    LOG(WARNING) << "Failed to get chunk \"" << key << "\"";
    return Chunk();
  }
}

bool RocksStore::Exists(const Hash& key) {
  return DBExists(ToRocksSlice(key));
}

bool RocksStore::Put(const Hash& key, const Chunk& chunk) {
  const auto key_slice = ToRocksSlice(key);
  if (DBExists(key_slice)) return true;
  auto success = DBPut(key_slice, ToRocksSlice(chunk));

#ifdef ENABLE_STORE_INFO
  if (success) {
    std::lock_guard<std::mutex> lock(mtx_store_info_);
    UpdateStoreInfoForNewChunk(chunk);
  }
#endif

  return success;
}

void RocksStore::InitStoreInfo() {
  store_info_.chunks = 0;
  store_info_.chunkBytes = 0;
  store_info_.validChunks = 0;
  store_info_.validChunkBytes = 0;
  for (auto chunk_type : Enum<ChunkType>()) {
    store_info_.bytesPerType[chunk_type] = 0;
    store_info_.chunksPerType[chunk_type] = 0;
  }
}

void RocksStore::UpdateStoreInfoForNewChunk(const Chunk& chunk) {
  const auto chunk_type = chunk.type();
  ++store_info_.chunksPerType[chunk_type];
  store_info_.bytesPerType[chunk_type] += chunk.numBytes();
}

const StoreInfo& RocksStore::GetInfo() {
#ifdef ENABLE_STORE_INFO
  std::lock_guard<std::mutex> lock(mtx_store_info_);
  store_info_.chunks = 0;
  for (auto& n_chunks_per_type : store_info_.chunksPerType) {
    store_info_.chunks += n_chunks_per_type.second;
  }
  store_info_.validChunks = store_info_.chunks;

  store_info_.chunkBytes = 0;
  for (auto& n_bytes_per_type : store_info_.bytesPerType) {
    store_info_.chunkBytes += n_bytes_per_type.second;
  }
  store_info_.validChunkBytes = store_info_.chunkBytes;
#endif
  return store_info_;
}

StoreIterator RocksStore::begin() const {
  // TODO(linqian)
  LOG(WARNING) << "RocksStore::begin() is called";
  return StoreIterator(nullptr);
}

StoreIterator RocksStore::cbegin() const {
  // TODO(linqian)
  LOG(WARNING) << "RocksStore::cbegin() is called";
  return StoreIterator(nullptr);
}

StoreIterator RocksStore::end() const {
  // TODO(linqian)
  LOG(WARNING) << "RocksStore::end() is called";
  return StoreIterator(nullptr);
}

StoreIterator RocksStore::cend() const {
  // TODO(linqian)
  LOG(WARNING) << "RocksStore::cend() is called";
  return StoreIterator(nullptr);
}

}  // namespace ustore

#endif  // USE_ROCKSDB
