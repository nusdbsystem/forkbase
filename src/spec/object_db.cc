// Copyright (c) 2017 The Ustore Authors.

#include "spec/object_db.h"

namespace ustore {

Result<VMeta> ObjectDB::Get(const Slice& key, const Slice& branch) const {
  UCell cell;
  ErrorCode code = db_->Get(key, branch, &cell);
  if (loader_) return {VMeta(db_, std::move(cell), loader_), code};
  return {VMeta(db_, std::move(cell)), code};
}

Result<VMeta> ObjectDB::Get(const Slice& key, const Hash& version) const {
  UCell cell;
  ErrorCode code = db_->Get(key, version, &cell);
  if (loader_) return {VMeta(db_, std::move(cell), loader_), code};
  return {VMeta(db_, std::move(cell)), code};
}

Result<Hash> ObjectDB::Put(const Slice& key, const VObject& object,
                           const Slice& branch) {
  Hash hash;
  ErrorCode code = db_->Put(key, object.value(), branch, &hash);
  return {std::move(hash), code};
}

Result<Hash> ObjectDB::Put(const Slice& key, const VObject& object,
                           const Hash& pre_version) {
  Hash hash;
  ErrorCode code = db_->Put(key, object.value(), pre_version, &hash);
  return {std::move(hash), code};
}

Result<Hash> ObjectDB::Merge(const Slice& key, const VObject& object,
                             const Slice& tgt_branch, const Slice& ref_branch) {
  Hash hash;
  ErrorCode code = db_->Merge(key, object.value(), tgt_branch, ref_branch,
                              &hash);
  return {std::move(hash), code};
}

Result<Hash> ObjectDB::Merge(const Slice& key, const VObject& object,
                             const Slice& tgt_branch, const Hash& ref_version) {
  Hash hash;
  ErrorCode code = db_->Merge(key, object.value(), tgt_branch, ref_version,
                              &hash);
  return {std::move(hash), code};
}

Result<Hash> ObjectDB::Merge(const Slice& key, const VObject& object,
                             const Hash& ref_version1,
                             const Hash& ref_version2) {
  Hash hash;
  ErrorCode code = db_->Merge(key, object.value(), ref_version1, ref_version2,
                              &hash);
  return {std::move(hash), code};
}

Result<std::vector<std::string>> ObjectDB::ListKeys() const {
  std::vector<std::string> keys;
  ErrorCode code = db_->ListKeys(&keys);
  return {std::move(keys), code};
}

Result<std::vector<std::string>> ObjectDB::ListBranches(const Slice& key)
    const {
  std::vector<std::string> branches;
  ErrorCode code = db_->ListBranches(key, &branches);
  return {std::move(branches), code};
}

Result<bool> ObjectDB::Exists(const Slice& key) const {
  bool exists;
  ErrorCode code = db_->Exists(key, &exists);
  return {exists, code};
}

Result<bool> ObjectDB::Exists(const Slice& key, const Slice& branch) const {
  bool exists;
  ErrorCode code = db_->Exists(key, branch, &exists);
  return {exists, code};
}

Result<Hash> ObjectDB::GetBranchHead(const Slice& key, const Slice& branch)
    const {
  Hash hash;
  ErrorCode code = db_->GetBranchHead(key, branch, &hash);
  return {std::move(hash), code};
}

Result<bool> ObjectDB::IsBranchHead(const Slice& key, const Slice& branch,
                                    const Hash& version) const {
  bool head;
  ErrorCode code = db_->IsBranchHead(key, branch, version, &head);
  return {head, code};
}

Result<std::vector<Hash>> ObjectDB::GetLatestVersions(const Slice& key) const {
  std::vector<Hash> versions;
  ErrorCode code = db_->GetLatestVersions(key, &versions);
  return {std::move(versions), code};
}

Result<bool> ObjectDB::IsLatestVersion(const Slice& key, const Hash& version)
    const {
  bool latest;
  ErrorCode code = db_->IsLatestVersion(key, version, &latest);
  return {latest, code};
}

ErrorCode ObjectDB::Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) {
  return db_->Branch(key, old_branch, new_branch);
}

ErrorCode ObjectDB::Branch(const Slice& key, const Hash& old_version,
                           const Slice& new_branch) {
  return db_->Branch(key, old_version, new_branch);
}

ErrorCode ObjectDB::Rename(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) {
  return db_->Rename(key, old_branch, new_branch);
}

ErrorCode ObjectDB::Delete(const Slice& key, const Slice& branch) {
  return db_->Delete(key, branch);
}

Result<std::vector<StoreInfo>> ObjectDB::GetStorageInfo() const {
  std::vector<StoreInfo> info;
  ErrorCode code = db_->GetStorageInfo(&info);
  return {std::move(info), code};
}

}  // namespace ustore
