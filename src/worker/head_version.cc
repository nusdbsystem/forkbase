// Copyright (c) 2017 The Ustore Authors.

#include "worker/head_version.h"

#include "utils/logging.h"

namespace ustore {

const HashOpt HeadVersion::GetBranch(const Slice& key,
                                     const Slice& branch) const {
  return Exists(key, branch) ?
         boost::make_optional(branch_ver_.at(key).at(branch)) : boost::none;
}

const std::unordered_set<Hash>& HeadVersion::GetLatest(
  const Slice& key) const {
  if (latest_ver_.find(key) == latest_ver_.end()) {
    DLOG(INFO) << "No data exists for Key \"" << key << "\"";
    static const std::unordered_set<Hash> empty;
    return empty;
  }
  return latest_ver_.at(key);
}

void HeadVersion::PutBranch(const Slice& key, const Slice& branch,
                            const Hash& ver) {
  branch_ver_[Persist(key)][Persist(branch)] = ver.Clone();
  LogBranchUpdate(key, branch, ver);
}

void HeadVersion::PutLatest(const Slice& key, const Hash& prev_ver1,
                            const Hash& prev_ver2, const Hash& ver) {
  auto& lv_key = latest_ver_[key];
  lv_key.erase(prev_ver1);
  lv_key.erase(prev_ver2);
  lv_key.insert(ver.Clone());
}

void HeadVersion::RemoveBranch(const Slice& key, const Slice& branch) {
  if (Exists(key, branch)) {
    auto& bv_key = branch_ver_.at(key);
    bv_key.erase(branch);
    LogBranchUpdate(key, branch, Hash::kNull);
  } else {
    LOG(WARNING) << "Branch \"" << branch << "for Key \"" << key
                 << "\" does not exist!";
  }
}

void HeadVersion::RenameBranch(const Slice& key, const Slice& old_branch,
                               const Slice& new_branch) {
  DCHECK(Exists(key, old_branch)) << ": Branch \"" << old_branch
                                  << "for Key \"" << key
                                  << "\" does not exist!";
  DCHECK(!Exists(key, new_branch)) << ": Branch \"" << new_branch
                                   << "for Key \"" << key
                                   << "\" already exists!";
  auto& bv_key = branch_ver_.at(key);
  bv_key[Persist(new_branch)] = std::move(bv_key.at(old_branch));
  LogBranchUpdate(key, new_branch, bv_key.at(new_branch));
  bv_key.erase(old_branch);
  LogBranchUpdate(key, old_branch, Hash::kNull);
}

bool HeadVersion::Exists(const Slice& key, const Slice& branch) const {
  if (branch_ver_.find(key) == branch_ver_.end()) return false;
  const auto& bv_key = branch_ver_.at(key);
  return bv_key.find(branch) != bv_key.end();
}

bool HeadVersion::IsLatest(const Slice& key, const Hash& ver) const {
  if (latest_ver_.find(key) == latest_ver_.end()) return false;
  const auto& lv_key = latest_ver_.at(key);
  return lv_key.find(ver) != lv_key.end();
}

bool HeadVersion::IsBranchHead(const Slice& key, const Slice& branch,
                               const Hash& ver) const {
  return Exists(key, branch) ? branch_ver_.at(key).at(branch) == ver : false;
}

std::unordered_set<Slice> HeadVersion::ListBranch(const Slice& key) const {
  std::unordered_set<Slice> branchs;
  if (branch_ver_.find(key) != branch_ver_.end()) {
    for (const auto& bv : branch_ver_.at(key)) {
      branchs.insert(bv.first);
    }
  }
  return branchs;
}

Slice HeadVersion::Persist(const Slice& slice) {
  std::string s(slice.data(), slice.len());
  branch_str_.insert(s);
  return Slice(*branch_str_.find(s));
}

}  // namespace ustore
