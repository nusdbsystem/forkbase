// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_HEAD_VERSION_H_
#define USTORE_WORKER_HEAD_VERSION_H_

#include <boost/optional.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "hash/hash.h"
#include "spec/slice.h"
#include "utils/noncopyable.h"

namespace ustore {

using HashOpt = boost::optional<Hash>;

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
class HeadVersion : private Noncopyable {
 public:
  // TODO(yaochang): persist the log of branch update.
  inline void LogBranchUpdate(const Slice& key, const Slice& branch,
                              const Hash& ver) const {}

  HeadVersion() {}
  ~HeadVersion() {}

  const HashOpt GetBranch(const Slice& key, const Slice& branch) const;

  const std::unordered_set<Hash>& GetLatest(const Slice& key) const;

  void PutBranch(const Slice& key, const Slice& branch, const Hash& ver);

  void PutLatest(const Slice& key, const Hash& prev_ver1,
                 const Hash& prev_ver2, const Hash& ver);

  void RemoveBranch(const Slice& key, const Slice& branch);

  void RenameBranch(const Slice& key, const Slice& old_branch,
                    const Slice& new_branch);

  bool Exists(const Slice& key, const Slice& branch) const;

  bool IsLatest(const Slice& key, const Hash& ver) const;

  bool IsBranchHead(const Slice& key, const Slice& branch,
                    const Hash& ver) const;

  std::unordered_set<Slice> ListBranch(const Slice& key) const;

 private:
  std::unordered_map<PSlice, std::unordered_map<PSlice, Hash>> branch_ver_;
  std::unordered_map<PSlice, std::unordered_set<Hash>> latest_ver_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_HEAD_VERSION_H_
