// Copyright (c) 2017 The Ustore Authors.

#include "utils/logging.h"
#include "ca/relational.h"

namespace ustore {
namespace example {
namespace ca {

ErrorCode ColumnStore::CreateTable(const std::string& table_name,
                                   const std::string& branch_name) {
  //TODO(ruanpc): need to PERFECTLY support empty map.
  Slice tab_name(table_name);
  Table tab({tab_name}, {tab_name});
  return odb_.Put(tab_name, tab, Slice(branch_name)).code();
  // return odb_.Put(Slice(table_name), Table(), Slice(branch_name)).code();
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
    //TODO(ruanpc): remove this if empty map is perfectly supported.
    if (it.key() == table_name) continue;

    auto col_key = GlobalKey(table_name, it.key());
    USTORE_GUARD(
      odb_.Branch(Slice(col_key), old_branch, new_branch));
  }

  // branch the table
  return odb_.Branch(Slice(table_name), old_branch, new_branch);
}

ErrorCode ColumnStore::DiffTable(const std::string& lhs_table_name,
                                 const std::string& lhs_branch_name,
                                 const std::string& rhs_table_name,
                                 const std::string& rhs_branch_name,
                                 TableDiffIterator* it_diff) {
  Table lhs;
  USTORE_GUARD(
    GetTable(lhs_table_name, lhs_branch_name, &lhs));
  Table rhs;
  USTORE_GUARD(
    GetTable(rhs_table_name, rhs_branch_name, &rhs));
  *it_diff = UMap::DuallyDiff(lhs, rhs);
  return ErrorCode::kOK;
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
                    Slice(ref_branch_name)).code();
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
                    Slice(ref_branch_name)).code();
}

ErrorCode ColumnStore::GetColumn(
  const std::string& table_name, const std::string& branch_name,
  const std::string& col_name, Column* col) {
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  if (tab.Get(Slice(col_name)).empty()) {
    LOG(ERROR) << "Column \"" << col_name << "\" does not exist in Table \""
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
  return odb_.Put(Slice(table_name), tab, Slice(branch_name)).code();
}

ErrorCode ColumnStore::RemoveColumn(const std::string& table_name,
                                    const std::string& branch_name,
                                    const std::string& col_name) {
  Table tab;
  USTORE_GUARD(
    GetTable(table_name, branch_name, &tab));
  tab.Remove(Slice(col_name));
  return odb_.Put(Slice(table_name), tab, Slice(branch_name)).code();
}

ErrorCode ColumnStore::DiffColumn(const std::string& lhs_table_name,
                                  const std::string& lhs_branch_name,
                                  const std::string& lhs_col_name,
                                  const std::string& rhs_table_name,
                                  const std::string& rhs_branch_name,
                                  const std::string& rhs_col_name,
                                  ColumnDiffIterator* it_diff) {
  Column lhs, rhs;
  USTORE_GUARD(
    GetColumn(lhs_table_name, lhs_branch_name, lhs_col_name, &lhs));
  USTORE_GUARD(
    GetColumn(rhs_table_name, rhs_branch_name, rhs_col_name, &rhs));
  *it_diff = DiffColumn(lhs, rhs);
  return ErrorCode::kOK;
}

}  // namespace ca
}  // namespace example
}  // namespace ustore
