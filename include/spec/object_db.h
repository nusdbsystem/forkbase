// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_OBJECT_DB_H_
#define USTORE_SPEC_OBJECT_DB_H_

#include "spec/db.h"
#include "types/client/vmeta.h"

namespace ustore {

template <typename T>
struct Result {
  T value;
  ErrorCode stat;
};

// Used by end-user
class ObjectDB {
 public:
  explicit ObjectDB(DB* db) noexcept : db_(db) {}

  // Get Object
  Result<VMeta> Get(const Slice& key, const Slice& branch);
  Result<VMeta> Get(const Slice& key, const Hash& version);
  // Put Object
  Result<Hash> Put(const Slice& key, const VObject& object,
                   const Slice& branch);
  Result<Hash> Put(const Slice& key, const VObject& object,
                   const Hash& pre_version);
  // Merge Objects
  Result<Hash> Merge(const Slice& key, const VObject& object,
                     const Slice& tgt_branch, const Slice& ref_branch);
  Result<Hash> Merge(const Slice& key, const VObject& object,
                     const Slice& tgt_branch, const Hash& ref_version);
  Result<Hash> Merge(const Slice& key, const VObject& object,
                     const Hash& ref_version1, const Hash& ref_version2);
  // List Keys/Branches
  Result<std::vector<std::string>> ListKeys();
  Result<std::vector<std::string>> ListBranches(const Slice& key);
  // Check Existence
  Result<bool> Exists(const Slice& key);
  Result<bool> Exists(const Slice& key, const Slice& branch);
  // Check Branch Head
  Result<Hash> GetBranchHead(const Slice& key, const Slice& branch);
  Result<bool> IsBranchHead(const Slice& key, const Slice& branch,
                            const Hash& version);
  // Check Latest Version
  Result<std::vector<Hash>> GetLatestVersions(const Slice& key);
  Result<bool> IsLatestVersion(const Slice& key, const Hash& version);
  // Create Branch
  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch);
  ErrorCode Branch(const Slice& key, const Hash& old_version,
                   const Slice& new_branch);
  // Rename Branch
  ErrorCode Rename(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch);
  // Delete Branch
  ErrorCode Delete(const Slice& key, const Slice& branch);
  // Get Storage Info
  Result<std::vector<StoreInfo>> GetStorageInfo();

 private:
  DB* db_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_OBJECT_DB_H_
