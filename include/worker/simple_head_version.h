// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_SIMPLE_HEAD_VERSION_H_
#define USTORE_WORKER_SIMPLE_HEAD_VERSION_H_

#include "worker/head_version.h"

namespace ustore {

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
class SimpleHeadVersion : public HeadVersion {
 public:
  SimpleHeadVersion() = default;
  ~SimpleHeadVersion() = default;

  bool LoadBranchVersion(const std::string& log_path);

  bool DumpBranchVersion(const std::string& log_path) const;

  boost::optional<Hash> GetBranch(const Slice& key,
                                  const Slice& branch) const;

  std::vector<Hash> GetLatest(const Slice& key) const;

  void PutBranch(const Slice& key, const Slice& branch, const Hash& ver);

  void PutLatest(const Slice& key, const Hash& prev_ver1,
                 const Hash& prev_ver2, const Hash& ver);

  void RemoveBranch(const Slice& key, const Slice& branch);

  void RenameBranch(const Slice& key, const Slice& old_branch,
                    const Slice& new_branch);

  std::vector<Slice> ListKey() const;

  inline bool Exists(const Slice& key) const {
    return latest_ver_.find(key) != latest_ver_.end();
  }

  bool Exists(const Slice& key, const Slice& branch) const;

  bool IsLatest(const Slice& key, const Hash& ver) const;

  inline bool IsBranchHead(const Slice& key, const Slice& branch,
                           const Hash& ver) const {
    return Exists(key, branch) ? branch_ver_.at(key).at(branch) == ver : false;
  }

  std::vector<Slice> ListBranch(const Slice& key) const;

  /**
   * linqian: remove this since branchVersion() is unused.
   */
  // inline const std::unordered_map<PSlice, std::map<PSlice, Hash>>&
  //     branchVersion() const {
  //   return branch_ver_;
  // }

 private:
  // use std::map for branch to preserve branch order
  std::unordered_map<PSlice, std::map<PSlice, Hash>> branch_ver_;
  std::unordered_map<PSlice, std::unordered_set<Hash>> latest_ver_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_SIMPLE_HEAD_VERSION_H_
