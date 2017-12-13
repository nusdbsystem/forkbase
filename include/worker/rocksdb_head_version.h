// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_ROCKSDB_HEAD_VERSION_H_
#define USTORE_WORKER_ROCKSDB_HEAD_VERSION_H_

#include "rocksdb/db.h"
#include "rocksdb/slice_transform.h"
#include "worker/head_version.h"

namespace ustore {

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
class RocksDBHeadVersion : public HeadVersion {
 public:
  explicit RocksDBHeadVersion();
  ~RocksDBHeadVersion();

  void CloseDB();

  static bool DeleteDB(const std::string& db_path);

  bool DeleteDB();

  bool LoadBranchVersion(const std::string& store_path) override;

  bool DumpBranchVersion(const std::string& log_path) override;

  bool GetBranch(const Slice& key, const Slice& branch,
                 Hash* ver) const override;

  std::vector<Hash> GetLatest(const Slice& key) const override;

  void PutBranch(const Slice& key, const Slice& branch,
                 const Hash& ver) override;

  void PutLatest(const Slice& key, const Hash& prev_ver1,
                 const Hash& prev_ver2, const Hash& ver) override;

  void RemoveBranch(const Slice& key, const Slice& branch) override;

  void RenameBranch(const Slice& key, const Slice& old_branch,
                    const Slice& new_branch) override;

  bool Exists(const Slice& key) const override;

  bool Exists(const Slice& key, const Slice& branch) const override;

  bool IsLatest(const Slice& key, const Hash& ver) const override;

  bool IsBranchHead(const Slice& key, const Slice& branch,
                    const Hash& ver) const override;

  std::vector<std::string> ListKey() const override;

  std::vector<std::string> ListBranch(const Slice& key) const override;

 private:
  std::string DBKey(const Slice& key) const;
  std::string ExtractKey(const rocksdb::Slice& db_key) const;

  std::string DBKey(const Slice& key, const Slice& branch) const;
  std::string ExtractBranch(const rocksdb::Slice& db_key) const;

  const rocksdb::SliceTransform* NewMetaPrefixTransform();

  void DeleteBranch(const Slice& key, const Slice& branch);

  bool FlushDB() const;

  std::string db_path_;
  rocksdb::DB* db_;
  rocksdb::Options db_opts_;
  rocksdb::ReadOptions db_read_opts_;
  rocksdb::WriteOptions db_write_opts_;
  rocksdb::FlushOptions db_flush_opts_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_ROCKSDB_HEAD_VERSION_H_
