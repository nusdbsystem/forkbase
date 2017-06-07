// Copyright (c) 2017 The UStore Authors.

#include "spec/relational.h"
#include "utils/logging.h"

namespace ustore {

ErrorCode ColumnStore::CreateTable(const std::string& table_name,
                                   const std::string& branch_name) {
  auto rst = odb_.Exists(Slice(table_name), Slice(branch_name));
  auto& tab_exist = rst.value;
  return tab_exist ? ErrorCode::kBranchExists :
         odb_.Put(Slice(table_name), Table(), Slice(branch_name)).stat;
}

ErrorCode ColumnStore::GetTable(const std::string& table_name,
                                const std::string& branch_name,
                                Table* table) {
  auto tab_rst = odb_.Get(Slice(table_name), Slice(branch_name));
  USTORE_GUARD(tab_rst.stat);
  *table = tab_rst.value.Map();
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::BranchTable(const std::string& table_name,
                                   const std::string& old_branch_name,
                                   const std::string& new_branch_name) {
  const Slice old_branch(old_branch_name);
  const Slice new_branch(new_branch_name);

  // branch all columns of the table
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, old_branch_name, &tab));
  for (auto it = tab.Scan(); !it.end(); it.next()) {
    auto col_key = GlobalKey(table_name, it.key());
    USTORE_GUARD(
      odb_.Branch(Slice(col_key), old_branch, new_branch));
  }

  // branch the table
  return odb_.Branch(Slice(table_name), old_branch, new_branch);
}

ErrorCode ColumnStore::MergeTable(const std::string& table_name,
                                  const std::string& tgt_branch_name,
                                  const std::string& ref_branch_name,
                                  const std::string& remove_col_name) {
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, tgt_branch_name, &tab));
  tab.Remove(Slice(remove_col_name));
  return odb_.Merge(Slice(table_name), tab, Slice(tgt_branch_name),
                    Slice(ref_branch_name)).stat;
}

ErrorCode ColumnStore::MergeTable(
  const std::string& table_name, const std::string& tgt_branch_name,
  const std::string& ref_branch_name, const std::string& new_col_name,
  const std::vector<std::string>& new_col_vals) {
  Version new_col_ver;
  USTORE_GUARD(
    WriteColumn(table_name, tgt_branch_name, new_col_name, new_col_vals,
                &new_col_ver));
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, tgt_branch_name, &tab));
  tab.Set(Slice(new_col_name), Slice(new_col_ver));
  return odb_.Merge(Slice(table_name), tab, Slice(tgt_branch_name),
                    Slice(ref_branch_name)).stat;
}

ErrorCode ColumnStore::GetColumn(
  const std::string& table_name, const std::string& branch_name,
  const std::string& col_name, Column* col) {
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  if (tab.Get(Slice(col_name)).empty()) {
    LOG(WARNING) << "Column \"" << col_name << "\" does not exist in Table \""
                 << table_name << "\" of Branch \"" << branch_name << "\"";
    return ErrorCode::kKeyNotExists;
  }
  return ReadColumn(table_name, branch_name, col_name, col);
}

ErrorCode ColumnStore::PutColumn(const std::string& table_name,
                                 const std::string& branch_name,
                                 const std::string& col_name,
                                 const std::vector<std::string>& col_vals) {
  Version col_ver;
  USTORE_GUARD(
    WriteColumn(table_name, branch_name, col_name, col_vals, &col_ver));
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  tab.Set(Slice(col_name), Slice(col_ver));
  return odb_.Put(Slice(table_name), tab, Slice(branch_name)).stat;
}

ErrorCode ColumnStore::DeleteColumn(const std::string& table_name,
                                    const std::string& branch_name,
                                    const std::string& col_name) {
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  tab.Remove(Slice(col_name));
  return odb_.Put(Slice(table_name), tab, Slice(branch_name)).stat;
}

}  // namespace ustore
