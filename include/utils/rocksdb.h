// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_ROCKSDB_H_
#define USTORE_UTILS_ROCKSDB_H_

#include <string>
#include <vector>
#include "hash/hash.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "spec/slice.h"
#include "utils/noncopyable.h"

namespace ustore {

/**
 * This is a building-block class for customizing a key-value store backed
 * by RocksDB. It should be extended and further to implement the application-
 * specific key-value APIs by calling the DB* methods.
 */
class RocksDB : private Noncopyable {
 public:
  static inline rocksdb::Slice ToRocksSlice(const ustore::Slice& x) {
    return rocksdb::Slice(reinterpret_cast<const char*>(x.data()), x.len());
  }

  static inline rocksdb::Slice ToRocksSlice(const ustore::Hash& x) {
    return rocksdb::Slice(reinterpret_cast<const char*>(x.value()),
                          ustore::Hash::kByteLength);
  }

  static inline rocksdb::Env* DefaultEnv() { return rocksdb::Env::Default(); }

  RocksDB();
  ~RocksDB() = default;

  bool OpenDB(const std::string& db_path);

  void CloseDB(const bool flush = true);

  static bool DestroyDB(const std::string& db_path);

  bool DestroyDB();

  bool FlushDB();

 protected:
  virtual const rocksdb::SliceTransform* NewPrefixTransform() const {
    return nullptr;
  }

  virtual rocksdb::MergeOperator* NewMergeOperator() const {
    return nullptr;
  }

  inline bool DBGet(const rocksdb::Slice& key, std::string* value) const {
    return db_->Get(db_read_opts_, key, value).ok();
  }

  inline bool DBGet(const rocksdb::Slice& key,
                    rocksdb::PinnableSlice* value) const {
    return db_->Get(db_read_opts_, db_->DefaultColumnFamily(), key, value).ok();
  }

  inline bool DBPut(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    return db_->Put(db_write_opts_, key, value).ok();
  }

  inline bool DBDelete(const rocksdb::Slice& key) {
    return db_->Delete(db_write_opts_, key).ok();
  }

  inline bool DBMerge(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    return db_->Merge(db_write_opts_, key, value).ok();
  }

  inline bool DBWrite(rocksdb::WriteBatch* updates) {
    return db_->Write(db_write_opts_, updates).ok();
  }

  void DBFullScan(
    const std::function<void(const rocksdb::Iterator*)>& f_proc_entry) const;

  void DBPrefixScan(
    const rocksdb::Slice& seek_key,
    const std::function<void(const rocksdb::Iterator*)>& f_proc_entry) const;

  std::string db_path_;
  rocksdb::DB* db_;
  rocksdb::Options db_opts_;
  rocksdb::BlockBasedTableOptions db_blk_tab_opts_;
  rocksdb::ReadOptions db_read_opts_;
  rocksdb::WriteOptions db_write_opts_;
  rocksdb::FlushOptions db_flush_opts_;
};

}  // namespace ustore

#endif  // USTORE_UTILS_ROCKSDB_H_
