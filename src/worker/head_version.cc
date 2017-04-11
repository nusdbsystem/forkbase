// Copyright (c) 2017 The Ustore Authors.

#include "worker/head_version.h"
#include "utils/logging.h"

namespace ustore {

const HashOpt HeadVersion::Get(const Slice& key, const Slice& branch) const {
  if (Exists(key, branch)) {
    return boost::make_optional(branch_ver_.at(key).at(branch));
  } else {
    return boost::none;
  }
}

const std::unordered_set<Hash>& HeadVersion::GetLatest(const Slice& key) const {
  if (latest_ver_.find(key) == latest_ver_.end()) {
    DLOG(INFO) << "No data exists for Key \"" << key << "\"";
    static const std::unordered_set<Hash> empty;
    return empty;
  } else {
    return latest_ver_.at(key);
  }
}

void HeadVersion::Put(const Slice& key, const Slice& branch,
                      const Hash& ver, const bool is_new_ver) {
  auto& bv_key = branch_ver_[key];
  auto& lv_key = latest_ver_[key];

  if (const auto& old_ver_opt = Get(key, branch)) {
    lv_key.erase(*old_ver_opt);
  } else {
    DLOG(INFO) << "Branch \"" << branch << "\" for Key \"" << key << "\" is created";
  }

  bv_key[branch] = ver.Clone();
  if (is_new_ver) { lv_key.insert(ver.Clone()); }
}

void HeadVersion::Put(const Slice& key, const Hash& old_ver,
                      const Hash& new_ver) {
  auto& lv_key = latest_ver_[key];
  lv_key.erase(old_ver);
  lv_key.insert(new_ver.Clone());
}

void HeadVersion::Merge(const Slice& key, const Hash& old_ver1,
                        const Hash& old_ver2, const Hash& new_ver) {
  auto& lv_key = latest_ver_[key];

  lv_key.erase(old_ver1);
  lv_key.erase(old_ver2);
  lv_key.insert(new_ver.Clone());
}

void HeadVersion::RemoveBranch(const Slice& key, const Slice& branch) {
  if (Exists(key, branch)) {
    auto& bv_key = branch_ver_.at(key);
    auto& lv_key = latest_ver_.at(key);

    lv_key.erase(bv_key[branch]);
    bv_key.erase(branch);
  } else {
    LOG(WARNING) << "Branch \"" << branch << "for Key \"" << key << "\" does not exist!";
  }
}

void HeadVersion::RenameBranch(const Slice& key, const Slice& old_branch,
                               const Slice& new_branch) {
  DCHECK(Exists(key, old_branch))
      << ": Branch \"" << old_branch << "for Key \"" << key << "\" does not exist!";
  DCHECK(!Exists(key, new_branch))
      << ": Branch \"" << new_branch << "for Key \"" << key << "\" already exists!";

  auto& bv_key = branch_ver_.at(key);
  bv_key[new_branch] = bv_key[old_branch];
  bv_key.erase(old_branch);
}

const bool HeadVersion::Exists(const Slice& key, const Slice& branch) const {
  if (branch_ver_.find(key) == branch_ver_.end()) {
    return false;
  } else {
    DCHECK_NE(latest_ver_.find(key), latest_ver_.end());
    const auto& bv_key = branch_ver_.at(key);
    return bv_key.find(branch) != bv_key.end();
  }
}

const bool HeadVersion::IsLatest(const Slice& key, const Hash& ver) const {
  if (latest_ver_.find(key) == latest_ver_.end()) {
    return false;
  } else {
    const auto& lv_key = latest_ver_.at(key);
    return lv_key.find(ver) != lv_key.end();
  }
}

const bool HeadVersion::IsBranchHead(const Slice& key, const Slice& branch,
                                     const Hash& ver) const {
  if (Exists(key, branch)) {
    return branch_ver_.at(key).at(branch) == ver;
  } else {
    return false;
  }
}

const std::unordered_set<Slice> HeadVersion::ListBranch(const Slice& key) const {
  std::unordered_set<Slice> branchs;

  if (branch_ver_.find(key) != branch_ver_.end()) {
    for (auto && bv : branch_ver_.at(key)) {
      branchs.insert(bv.first);
    }
  }

  return branchs;
}

}  // namespace ustore
