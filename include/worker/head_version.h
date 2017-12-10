// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_HEAD_VERSION_H_
#define USTORE_WORKER_HEAD_VERSION_H_

#include <boost/optional.hpp>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "hash/hash.h"
#include "spec/slice.h"
#include "utils/noncopyable.h"

namespace ustore {

class HeadVersion : private Noncopyable {
 public:
  // TODO(yaochang): persist the log of branch update.
  virtual void LogBranchUpdate(const Slice& key, const Slice& branch,
                               const Hash& ver) const {}

  // Load branch version info from log path
  // Add them into member branch_ver_
  // Return whether loading succeeds
  virtual bool LoadBranchVersion(const std::string& log_path) = 0;

  // Dump branch_ver_ into log_path
  // Return whether Dumping succeeds
  virtual bool DumpBranchVersion(const std::string& log_path) const = 0;

  virtual boost::optional<Hash> GetBranch(const Slice& key,
                                          const Slice& branch) const = 0;

  virtual std::vector<Hash> GetLatest(const Slice& key) const = 0;

  virtual void PutBranch(const Slice& key, const Slice& branch,
                         const Hash& ver) = 0;

  virtual void PutLatest(const Slice& key, const Hash& prev_ver1,
                         const Hash& prev_ver2, const Hash& ver) = 0;

  virtual void RemoveBranch(const Slice& key, const Slice& branch) = 0;

  virtual void RenameBranch(const Slice& key, const Slice& old_branch,
                            const Slice& new_branch) = 0;

  virtual std::vector<Slice> ListKey() const = 0;

  virtual bool Exists(const Slice& key) const = 0;

  virtual bool Exists(const Slice& key, const Slice& branch) const = 0;

  virtual bool IsLatest(const Slice& key, const Hash& ver) const = 0;

  virtual bool IsBranchHead(const Slice& key, const Slice& branch,
                            const Hash& ver) const = 0;

  virtual std::vector<Slice> ListBranch(const Slice& key) const = 0;
};

}  // namespace ustore

#endif  // USTORE_WORKER_HEAD_VERSION_H_
