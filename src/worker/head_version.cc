// Copyright (c) 2017 The Ustore Authors.

#include <fstream>      // std::fstream
#include <utility>

#include "proto/head_version.pb.h"
#include "worker/head_version.h"
#include "utils/logging.h"

namespace ustore {
bool HeadVersion::LoadBranchVersion(const std::string& log_path) {
  std::ifstream ifs(log_path, std::ifstream::in);

  if (ifs) {
    LOG(INFO) << "Loading head version file: " << log_path << " ......";
    KeyVersions key_versions;  // protobuf
    if (!key_versions.ParseFromIstream(&ifs)) return false;

    size_t num_keys = key_versions.key_versions_size();

    for (size_t key_idx = 0; key_idx < num_keys; ++key_idx) {
      const KeyVersion& key_version = key_versions.key_versions(key_idx);
      size_t num_branches = key_version.branches_size();

      PSlice key = PSlice::Persist(Slice(key_version.key()));
      branch_ver_.emplace(key, std::unordered_map<PSlice, Hash>());
      auto& branch_map = branch_ver_.find(key)->second;

      DLOG(INFO) << "Getting Key: " << key.ToString();
      for (size_t branch_idx = 0; branch_idx < num_branches; ++branch_idx) {
        PSlice branch = PSlice::Persist(
            Slice(key_version.branches(branch_idx).branch()));
        std::string version_base32 =
            key_version.branches(branch_idx).hash_base32();
        DLOG(INFO) << "\tGetting Branch: " << branch.ToString();
        DLOG(INFO) << "\tGetting Version:  " << version_base32;
        branch_map.emplace(branch, Hash());
        branch_map.find(branch)->second = Hash::FromBase32(version_base32);
      }
    }
    LOG(INFO) << "Loaded head versions";
    return true;
  } else {
    return false;
  }  // end ifs
}

bool HeadVersion::DumpBranchVersion(const std::string& log_path) {
  // Dump the brach_ver_ to external file to persist
  LOG(INFO) << "Dumping head version file: " << log_path << " ......";
  std::ofstream ofs(log_path, std::ofstream::out);
  KeyVersions key_versions;
  for (const auto& k2branchversion : branch_ver_) {
    PSlice key = k2branchversion.first;
    KeyVersion* key_version = key_versions.add_key_versions();
    key_version->set_key(key.ToString());
    DLOG(INFO) << "Dumping key: " << key_version->key();
    auto branch_version = k2branchversion.second;

    for (const auto branch2version : branch_version) {
      PSlice branch = branch2version.first;
      Hash hash = branch2version.second;

      BranchVersion* b2v =
          key_version->add_branches();
      b2v->set_branch(branch.ToString());
      b2v->set_hash_base32(hash.ToBase32());
      DLOG(INFO) << "\t branch: " << branch.ToString();
      DLOG(INFO) << "\t version: " << hash.ToBase32();
    }  // end for branch2version
  }  // end for k2branchversion
  bool succeeds = key_versions.SerializeToOstream(&ofs);
  ofs.close();
  LOG(INFO) << "Dumped head versions";
  return succeeds;
}

boost::optional<Hash> HeadVersion::GetBranch(const Slice& key,
    const Slice& branch) const {
  return Exists(key, branch)
         ? boost::make_optional(branch_ver_.at(key).at(branch))
         : boost::none;
}

std::vector<Hash> HeadVersion::GetLatest(const Slice& key) const {
  if (latest_ver_.find(key) == latest_ver_.end()) {
    DLOG(INFO) << "No data exists for Key \"" << key << "\"";
    static const std::vector<Hash> empty;
    return empty;
  } else {
    const auto& lv_key = latest_ver_.at(key);
    std::vector<Hash> latest;
    for (const auto& v : lv_key) {
      latest.push_back(v);
    }
    DCHECK_EQ(lv_key.size(), latest.size());
    return latest;
  }
}

void HeadVersion::PutBranch(const Slice& key, const Slice& branch,
                            const Hash& ver) {
  auto key_it = branch_ver_.find(key);
  // create key if not exists
  if (key_it == branch_ver_.end()) {
    branch_ver_.emplace(PSlice::Persist(key),
                        std::unordered_map<PSlice, Hash>());
    key_it = branch_ver_.find(key);
    DCHECK(key_it != branch_ver_.end())
        << "fail to insert new key into head table";
  }
  auto& branch_map = key_it->second;
  auto branch_it = branch_map.find(branch);
  // create branch if not exists
  if (branch_it == branch_map.end()) {
    branch_map.emplace(PSlice::Persist(branch), Hash());
    branch_it = branch_map.find(branch);
    DCHECK(branch_it != branch_map.end())
        << "fail to insert new branch into head table";
  }
  branch_it->second = ver.Clone();
  LogBranchUpdate(key, branch, ver);
}

void HeadVersion::PutLatest(const Slice& key, const Hash& prev_ver1,
                            const Hash& prev_ver2, const Hash& ver) {
  auto key_it = latest_ver_.find(key);
  // create key is not exists
  if (key_it == latest_ver_.end()) {
    latest_ver_.emplace(PSlice::Persist(key), std::unordered_set<Hash>());
    key_it = latest_ver_.find(key);
    DCHECK(key_it != latest_ver_.end())
        << "fail to insert new key into latest version table";
  }
  auto& lv_key = key_it->second;
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
                                  << "\" for Key \"" << key
                                  << "\" does not exist!";
  DCHECK(!Exists(key, new_branch)) << ": Branch \"" << new_branch
                                   << "\" for Key \"" << key
                                   << "\" already exists!";
  auto& bv_key = branch_ver_.at(key);
  bv_key.emplace(PSlice::Persist(new_branch), std::move(bv_key.at(old_branch)));
  LogBranchUpdate(key, new_branch, bv_key.at(new_branch));
  bv_key.erase(old_branch);
  LogBranchUpdate(key, old_branch, Hash::kNull);
}

bool HeadVersion::Exists(const Slice& key, const Slice& branch) const {
  auto key_it = branch_ver_.find(key);
  if (key_it == branch_ver_.end()) return false;
  const auto& bv_key = key_it->second;
  return bv_key.find(branch) != bv_key.end();
}

bool HeadVersion::IsLatest(const Slice& key, const Hash& ver) const {
  auto key_it = latest_ver_.find(key);
  if (key_it == latest_ver_.end()) return false;
  const auto& lv_key = key_it->second;
  return lv_key.find(ver) != lv_key.end();
}

std::vector<Slice> HeadVersion::ListKey() const {
  std::vector<Slice> keys;
  for (auto& lv : latest_ver_) keys.emplace_back(lv.first);
  return keys;
}

std::vector<Slice> HeadVersion::ListBranch(const Slice& key) const {
  std::vector<Slice> branchs;
  if (branch_ver_.find(key) != branch_ver_.end()) {
    for (const auto& bv : branch_ver_.at(key)) {
      branchs.emplace_back(bv.first);
    }
  }
  return branchs;
}

}  // namespace ustore
