// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_ROCKSDB_HEAD_VERSION_H_
#define USTORE_WORKER_ROCKSDB_HEAD_VERSION_H_

#include <iomanip>
#include "rocksdb/db.h"

#include "worker/head_version.h"

namespace ustore {

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
// class RocksDBHeadVersion : public HeadVersion {
class RocksDBHeadVersion {
 public:
  RocksDBHeadVersion();
  ~RocksDBHeadVersion();

  inline bool LoadBranchVersion(const std::string& log_path) { return true; }

  inline bool DumpBranchVersion(const std::string& log_path) const {
    db_->Flush(rocksdb::FlushOptions());
    return true;
  }

  boost::optional<Hash> GetBranch(const Slice& key,
                                  const Slice& branch) const;

  std::vector<Hash> GetLatest(const Slice& key) const;

  void PutBranch(const Slice& key, const Slice& branch, const Hash& ver);

  void PutLatest(const Slice& key, const Hash& prev_ver1,
                 const Hash& prev_ver2, const Hash& ver);

  void RemoveBranch(const Slice& key, const Slice& branch);

  void RenameBranch(const Slice& key, const Slice& old_branch,
                    const Slice& new_branch);

  bool Exists(const Slice& key) const;

  bool Exists(const Slice& key, const Slice& branch) const;

  bool IsLatest(const Slice& key, const Hash& ver) const;

  bool IsBranchHead(const Slice& key, const Slice& branch,
                    const Hash& ver) const;

  // std::vector<Slice> ListKey() const;

  // std::vector<Slice> ListBranch(const Slice& key) const;

 private:
  inline std::string DBKey(const Slice& key) const {
    return "$" + key.ToString();
  }

  inline std::string DBKey(const Slice& key, const Slice& branch) const {
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(4) << key.len() << key
       << branch;
    return ss.str();
  }

  void DeleteBranch(const Slice& key, const Slice& branch);

  rocksdb::DB* db_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_ROCKSDB_HEAD_VERSION_H_
