// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_HEAD_VERSION_H_
#define USTORE_WORKER_HEAD_VERSION_H_

#include <boost/optional.hpp>
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
  HeadVersion() {}
  ~HeadVersion() {}

  const HashOpt Get(const Slice& key, const Slice& branch) const;

  const std::unordered_set<Hash>& GetLatest(const Slice& key) const;

  void Put(const Slice& key, const Slice& branch, const Hash& ver,
           const bool is_new_ver = true);

  void Put(const Slice& key, const Hash& old_ver, const Hash& new_ver);

  void Merge(const Slice& key, const Hash& old_ver1, const Hash& old_ver2,
             const Hash& new_ver);

  void RemoveBranch(const Slice& key, const Slice& branch);

  void RenameBranch(const Slice& key, const Slice& old_branch,
                    const Slice& new_branch);

  const bool Exists(const Slice& key, const Slice& branch) const;

  const bool IsLatest(const Slice& key, const Hash& ver) const;

  const bool IsBranchHead(const Slice& key, const Slice& branch,
                          const Hash& ver) const;

  const std::unordered_set<Slice> ListBranch(const Slice& key) const;

 private:
  std::unordered_map<Slice, std::unordered_map<Slice, Hash>> branch_ver_;
  std::unordered_map<Slice, std::unordered_set<Hash>> latest_ver_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_HEAD_VERSION_H_
