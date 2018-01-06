// Copyright (c) 2017 The Ustore Authors.

#if defined(USE_ROCKSDB)

#include <thread>
#include "rocksdb/filter_policy.h"
#include "utils/logging.h"

#include "utils/rocksdb.h"

namespace ustore {

static const size_t kDefaultWriteBufferSize(128 << 20);
static const uint64_t kDefaultMemtableMemoryBudget(512 << 20);

RocksDB::RocksDB()
  : db_(nullptr) {
  db_opts_.create_if_missing = true;
  db_opts_.write_buffer_size = kDefaultWriteBufferSize;
  db_opts_.IncreaseParallelism(std::thread::hardware_concurrency());
  db_opts_.OptimizeLevelStyleCompaction(kDefaultMemtableMemoryBudget);
}

bool RocksDB::OpenDB(const std::string& db_path) {
  if (db_ != nullptr) {
    LOG(ERROR) << "DB is already opened";
    return false;
  }
  // Note: NewPrefixTransform() shouldn't be called in the constructor.
  const rocksdb::SliceTransform* prefix_trans = NewPrefixTransform();
  if (prefix_trans != nullptr) {
    db_opts_.prefix_extractor.reset(prefix_trans);
    db_read_opts_.prefix_same_as_start = true;
    db_blk_tab_opts_.filter_policy.reset(  // enable prefix bloom for SST files
      rocksdb::NewBloomFilterPolicy(10, true));
  }
  // Note: NewMergeOperator() shouldn't be called in the constructor.
  rocksdb::MergeOperator* merge_op = NewMergeOperator();
  if (merge_op != nullptr) db_opts_.merge_operator.reset(merge_op);

  db_opts_.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(db_blk_tab_opts_));

  auto db_stat = rocksdb::DB::Open(db_opts_, db_path, &db_);
  if (db_stat.ok()) {
    db_path_ = db_path;
    LOG(INFO) << "DB is successfully opened: " << db_path_;
    return true;
  } else {
    LOG(ERROR) << "Failed to open DB: " << db_stat.ToString();
    return false;
  }
}

void RocksDB::CloseDB(const bool flush) {
  if (flush) FlushDB();
  if (db_ != nullptr) delete db_;
  db_ = nullptr;
}

bool RocksDB::DestroyDB(const std::string& db_path) {
  auto db_stat = rocksdb::DestroyDB(db_path, rocksdb::Options());
  if (db_stat.ok()) {
    LOG(INFO) << "DB at \"" << db_path << "\" has been deleted";
    return true;
  } else {
    LOG(ERROR) << "Failed to delete DB: " << db_path;
    return false;
  }
}

bool RocksDB::DestroyDB() {
  CloseDB(false);
  bool success = db_path_.empty() ? false : DestroyDB(db_path_);
  if (success) db_path_.clear();
  return success;
}

bool RocksDB::FlushDB() {
  auto db_stat = db_->Flush(db_flush_opts_);
  bool success = db_stat.ok();
  CHECK(success) << "Failed to flush DB: " << db_stat.ToString();
  return success;
}

void RocksDB::DBFullScan(
  const std::function<void(const rocksdb::Iterator*)>& f_proc_entry) const {
  auto it = db_->NewIterator(db_read_opts_);
  for (it->SeekToFirst(); it->Valid(); it->Next()) f_proc_entry(it);
  delete it;
}

void RocksDB::DBPrefixScan(
  const rocksdb::Slice& seek_key,
  const std::function<void(const rocksdb::Iterator*)>& f_proc_entry) const {
  auto it = db_->NewIterator(db_read_opts_);
  for (it->Seek(seek_key); it->Valid(); it->Next()) f_proc_entry(it);
  delete it;
}

}  // namespace ustore

#endif  // USE_ROCKSDB
