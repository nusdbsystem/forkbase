// Copyright (c) 2017 The Ustore Authors.

#ifdef USE_ROCKSDB

#include <utility>
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
}

RocksStore::~RocksStore() {
  if (persist_)
    CloseDB();
  else
    DestroyDB();
}

Chunk RocksStore::Get(const Hash& key) {
  rocksdb::PinnableSlice value;
  if (DBGet(ToRocksSlice(key), &value)) {
    const auto value_size = value.size();
    std::unique_ptr<byte_t[]> buf(new byte_t[value_size]);
    std::memcpy(buf.get(), value.data(), value_size);
    Chunk chunk(std::move(buf));
    DCHECK(key == chunk.hash());
    return chunk;
  } else {
    LOG(WARNING) << "Failed to get chunk \"" << key << "\"";
    return Chunk();
  }
}

// TODO(linqian): The current impl of checking existence is slow
bool RocksStore::Exists(const rocksdb::Slice& key) const {
  static rocksdb::PinnableSlice value;
  value.Reset();
  return DBGet(key, &value);
}

bool RocksStore::Exists(const Hash& key) {
  return Exists(ToRocksSlice(key));
}

bool RocksStore::Put(const Hash& key, const Chunk& chunk) {
  const auto key_slice = ToRocksSlice(key);
  if (Exists(key_slice)) return true;
  return DBPut(key_slice, ToRocksSlice(chunk));
}

const StoreInfo& RocksStore::GetInfo() const {
  static StoreInfo info;
  return info;
}

StoreIterator RocksStore::begin() const {
  // TODO(linqian)
  return StoreIterator(nullptr);
}

StoreIterator RocksStore::cbegin() const {
  // TODO(linqian)
  return StoreIterator(nullptr);
}

StoreIterator RocksStore::end() const {
  // TODO(linqian)
  return StoreIterator(nullptr);
}

StoreIterator RocksStore::cend() const {
  // TODO(linqian)
  return StoreIterator(nullptr);
}

}  // namespace ustore

#endif  // USE_ROCKSDB
