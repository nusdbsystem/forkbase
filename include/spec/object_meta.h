// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_OBJECT_META_H_
#define USTORE_SPEC_OBJECT_META_H_

#include "spec/object_db.h"

namespace ustore {

class ObjectMeta {
 public:
  explicit ObjectMeta(DB* db) noexcept : odb_(db) {}
  ~ObjectMeta() = default;

  static inline std::string MetaTableKey(const std::string& obj_name) {
    return "$" + obj_name;
  }

 protected:

  ErrorCode CreateMetaTable(const std::string& obj_name,
                            const std::string& branch);

  ErrorCode BranchMetaTable(const std::string& obj_name,
                            const std::string& old_branch,
                            const std::string& new_branch);

  ErrorCode DeleteMetaTable(const std::string& obj_name,
                            const std::string& branch);

  ErrorCode GetMetaTable(const std::string& obj_name,
                         const std::string& branch,
                         VMap* meta_tab) const;

  ErrorCode SetMeta(const std::string& obj_name,
                    const std::string& branch,
                    const std::string& meta_name,
                    const std::string& meta_val);

  ErrorCode GetMeta(const std::string& obj_name,
                    const std::string& branch,
                    const std::string& meta_name,
                    std::string* meta_val) const;

  ErrorCode DeleteMeta(const std::string& obj_name,
                       const std::string& branch,
                       const std::string& meta_name);

 private:
  ErrorCode ReadMetaTable(const Slice& meta_tab_key, const Slice& branch,
                          VMap* meta_tab) const;

  ObjectDB odb_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_OBJECT_META_H_
