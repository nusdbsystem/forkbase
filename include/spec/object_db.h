// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_OBJECT_DB_H_
#define USTORE_SPEC_OBJECT_DB_H_

#include "spec/db.h"
#include "types/client/vmeta.h"

namespace ustore {

// Used by end-user
class ObjectDB {
 public:
  explicit ObjectDB(DB* db) noexcept : db_(db) {}

  // Get Object
  VMeta Get(const Slice& key, const Slice& branch);
  VMeta Get(const Slice& key, const Hash& version);
  // Put Object
  VMeta Put(const Slice& key, const VObject& object, const Slice& branch);
  VMeta Put(const Slice& key, const VObject& object, const Hash& pre_version);
  // Merge Objects
  VMeta Merge(const Slice& key, const VObject& object, const Slice& tgt_branch,
              const Slice& ref_branch);
  VMeta Merge(const Slice& key, const VObject& object, const Slice& tgt_branch,
              const Hash& ref_version);
  VMeta Merge(const Slice& key, const VObject& object, const Hash& ref_version1,
              const Hash& ref_version2);
  // Create Branch
  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch);
  ErrorCode Branch(const Slice& key, const Hash& old_version,
                   const Slice& new_branch);
  // Rename Branch
  ErrorCode Rename(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch);

 private:
  DB* db_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_OBJECT_DB_H_
