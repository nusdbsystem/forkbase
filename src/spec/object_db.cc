// Copyright (c) 2017 The Ustore Authors.

#include "spec/object_db.h"

namespace ustore {

VMeta ObjectDB::Get(const Slice& key, const Slice& branch) {
  UCell cell;
  ErrorCode code = db_->Get(key, branch, &cell);
  return VMeta(db_, code, std::move(cell));
}

VMeta ObjectDB::Get(const Slice& key, const Hash& version) {
  UCell cell;
  ErrorCode code = db_->Get(key, version, &cell);
  return VMeta(db_, code, std::move(cell));
}

VMeta ObjectDB::Put(const Slice& key, const VObject& object,
                    const Slice& branch) {
  Hash hash;
  ErrorCode code = db_->Put(key, object.value(), branch, &hash);
  return VMeta(db_, code, std::move(hash));
}

VMeta ObjectDB::Put(const Slice& key, const VObject& object,
                    const Hash& pre_version) {
  Hash hash;
  ErrorCode code = db_->Put(key, object.value(), pre_version, &hash);
  return VMeta(db_, code, std::move(hash));
}

VMeta ObjectDB::Merge(const Slice& key, const VObject& object,
                      const Slice& tgt_branch, const Slice& ref_branch) {
  Hash hash;
  ErrorCode code = db_->Merge(key, object.value(), tgt_branch, ref_branch,
                              &hash);
  return VMeta(db_, code, std::move(hash));
}

VMeta ObjectDB::Merge(const Slice& key, const VObject& object,
                      const Slice& tgt_branch, const Hash& ref_version) {
  Hash hash;
  ErrorCode code = db_->Merge(key, object.value(), tgt_branch, ref_version,
                              &hash);
  return VMeta(db_, code, std::move(hash));
}

VMeta ObjectDB::Merge(const Slice& key, const VObject& object,
                      const Hash& ref_version1, const Hash& ref_version2) {
  Hash hash;
  ErrorCode code = db_->Merge(key, object.value(), ref_version1, ref_version2,
                              &hash);
  return VMeta(db_, code, std::move(hash));
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

}  // namespace ustore
