// Copyright (c) 2017 The UStore Authors.

#include "spec/object_meta.h"

namespace ustore {

const std::string ObjectMeta::kKeyPrefix("$");

ErrorCode ObjectMeta::CreateMetaTable(const std::string& obj_name,
                                      const std::string& branch) {
  const std::string meta_tab_key(MetaTableKey(obj_name));
  return odb_.Put(Slice(meta_tab_key), VMap(), Slice(branch)).stat;
}

ErrorCode ObjectMeta::BranchMetaTable(const std::string& obj_name,
                                      const std::string& old_branch,
                                      const std::string& new_branch) {
  const std::string meta_tab_key(MetaTableKey(obj_name));
  return odb_.Branch(Slice(meta_tab_key), Slice(old_branch), Slice(new_branch));
}

ErrorCode ObjectMeta::DeleteMetaTable(const std::string& obj_name,
                                      const std::string& branch) {
  const std::string meta_tab_key(MetaTableKey(obj_name));
  return odb_.Delete(Slice(meta_tab_key), Slice(branch));
}

ErrorCode ObjectMeta::GetMetaTable(const std::string& obj_name,
                                   const std::string& branch,
                                   VMap* meta_tab) const {
  const std::string meta_tab_key(MetaTableKey(obj_name));
  return ReadMetaTable(Slice(meta_tab_key), Slice(branch), meta_tab);
}

ErrorCode ObjectMeta::ReadMetaTable(const Slice& meta_tab_key,
                                    const Slice& branch,
                                    VMap* meta_tab) const {
  auto rst = odb_.Get(meta_tab_key, branch);
  auto& ec = rst.stat;
  if (ec == ErrorCode::kOK) {
    *meta_tab = rst.value.Map();
  } else {
    ERROR_CODE_FWD(ec, kKeyNotExists, kObjectMetaNotExists);
  }
  return ec;
}

ErrorCode ObjectMeta::SetMeta(const std::string& obj_name,
                              const std::string& branch,
                              const std::string& meta_name,
                              const std::string& meta_val) {
  const std::string meta_tab_key(MetaTableKey(obj_name));
  const Slice meta_tab_key_slice(meta_tab_key), branch_slice(branch);
  VMap meta_tab;
  USTORE_GUARD(
    ReadMetaTable(meta_tab_key_slice, branch_slice, &meta_tab));
  meta_tab.Set(Slice(meta_name), Slice(meta_val));
  return odb_.Put(meta_tab_key_slice, meta_tab, branch_slice).stat;
}

ErrorCode ObjectMeta::GetMeta(const std::string& obj_name,
                              const std::string& branch,
                              const std::string& meta_name,
                              std::string* meta_val) const {
  VMap meta_tab;
  USTORE_GUARD(
    GetMetaTable(obj_name, branch, &meta_tab));
  const auto val = meta_tab.Get(Slice(meta_name));
  *meta_val = val.empty() ? "" : val.ToString();
  return ErrorCode::kOK;
}

ErrorCode ObjectMeta::DeleteMeta(const std::string& obj_name,
                                 const std::string& branch,
                                 const std::string& meta_name) {
  const std::string meta_tab_key(MetaTableKey(obj_name));
  const Slice meta_tab_key_slice(meta_tab_key), branch_slice(branch);
  VMap meta_tab;
  USTORE_GUARD(
    ReadMetaTable(meta_tab_key_slice, branch_slice, &meta_tab));
  meta_tab.Remove(Slice(meta_name));
  return odb_.Put(meta_tab_key_slice, meta_tab, branch_slice).stat;
}

}  // namespace ustore
