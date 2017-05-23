// Copyright (c) 2017 The Ustore Authors.

#include "ca/relational.h"

#include "utils/utils.h"

namespace ustore {
namespace example {
namespace ca {

const std::string ColumnStore::kDefaultBranch = "master";

ErrorCode ColumnStore::CreateTable(const std::string& table_name) {
  // Init a map to store table name as a unique kv pair
  Slice tkey("tname");
  Slice tname(table_name);
  ustore::VMap tmap({tkey}, {tname});
  // put new map
  VMeta put = odb_.Put(Slice(table_name), tmap, Slice(kDefaultBranch));
  return put.code();
}

ErrorCode ColumnStore::BranchTable(const std::string& table_name,
                                   const std::string& tgt_branch_name,
                                   const std::string& ref_branch_name) {
  Slice tname(table_name);
  Slice new_branch(tgt_branch_name);
  Slice old_branch(ref_branch_name);
  return odb_.Branch(tname, old_branch, new_branch);
}

ErrorCode ColumnStore::DiffTable(const std::string& lhs_table_name,
                                 const std::string& lhs_branch_name,
                                 const std::string& rhs_table_name,
                                 const std::string& rhs_branch_name,
                                 TableDiffIterator* itr) {
  VMeta lhs_meta = odb_.Get(Slice(lhs_table_name), Slice(lhs_branch_name));
  USTORE_GUARD(lhs_meta.code());
  VMeta rhs_meta = odb_.Get(Slice(rhs_table_name), Slice(rhs_branch_name));
  USTORE_GUARD(rhs_meta.code());
  VMap lhs_map = lhs_meta.Map();
  VMap rhs_map = rhs_meta.Map();
  *itr = UMap::DuallyDiff(lhs_map, rhs_map);
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::MergeTable(const std::string& table_name,
                                  const std::string& tgt_branch_name,
                                  const std::string& ref_branch_name,
                                  const std::string& removed_col_name) {
  Slice table_key(table_name);
  Slice tgt_branch(tgt_branch_name);
  Slice ref_branch(ref_branch_name);

  VMeta table_meta = odb_.Get(table_key, tgt_branch);
  USTORE_GUARD(table_meta.code());
  VMap table_map = table_meta.Map();
  std::string table_col_key = GetColumnKeyForTable(table_name,
                                                   removed_col_name);
  table_map.Remove(Slice(table_col_key));
  VMeta updated_table_meta = odb_.Merge(table_key, table_map, tgt_branch,
                                        ref_branch);
  USTORE_GUARD(updated_table_meta.code());
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::MergeTable(const std::string& table_name,
                                  const std::string& tgt_branch_name,
                                  const std::string& ref_branch_name,
                                  const std::string& updated_col_name,
                                  const std::vector<std::string>& updated_col) {
  Slice table_key(table_name);
  Slice ref_branch(ref_branch_name);
  Slice tgt_branch(tgt_branch_name);

  VMeta table_meta = odb_.Get(table_key, tgt_branch);
  USTORE_GUARD(table_meta.code());
  VMap table_map = table_meta.Map();
// Put the entire column as a Vlist
  // the key for Vlist is {table_name}_{col_name}
  const std::string table_col_key = GetColumnKeyForTable(table_name,
                                                         updated_col_name);
  std::vector<Slice> col_vals;
  for (const auto& c : updated_col) {
    col_vals.push_back(Slice(c));
  }
  ustore::VList col_list(col_vals);
  // put new list
  VMeta col_meta = odb_.Put(Slice(table_col_key), col_list,
                            Slice(kDefaultBranch));
  USTORE_GUARD(col_meta.code());
// Put the ucell hash of column list into tablemap
  std::string col_version_base32 = col_meta.version().ToBase32();
  table_map.Set(Slice(table_col_key), Slice(col_version_base32));
  VMeta updated_table_meta = odb_.Merge(table_key, table_map, tgt_branch,
                                        ref_branch);
  USTORE_GUARD(updated_table_meta.code());
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::GetColumn(const std::string& table_name,
                                 const std::string& branch_name,
                                 const std::string& col_name, Column* column) {
// Load the Table Map
  Slice table_key(table_name);
  Slice table_branch(branch_name);
  VMeta table_meta = odb_.Get(table_key, table_branch);
  USTORE_GUARD(table_meta.code());
  VMap table_map = table_meta.Map();
// Get the column hash from Table Map
//   to check this column exists in this table of this branch
  const std::string table_col_key = GetColumnKeyForTable(table_name, col_name);
  Slice col_version_base32 = table_map.Get(Slice(table_col_key));
  if (col_version_base32.empty()) { return ErrorCode::kKeyNotExists; }
  Hash col_version = Hash::FromBase32(col_version_base32.ToString());
  auto col_meta = odb_.Get(Slice(table_col_key), col_version);
  *column = col_meta.List();
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::PutColumn(const std::string& table_name,
                                 const std::string& branch_name,
                                 const std::string& col_name,
                                 const std::vector<std::string>& col) {
// Get the Map for Table first
  Slice table_key(table_name);
  Slice table_branch(branch_name);

  VMeta table_meta = odb_.Get(table_key, table_branch);
  USTORE_GUARD(table_meta.code());
  VMap table_map = table_meta.Map();
// Put the entire colum
  // the key for Vlist is {table_name}_{col_name}
  const std::string table_col_key = GetColumnKeyForTable(table_name, col_name);
  std::vector<Slice> col_vals;
  for (const auto& c : col) {
    col_vals.push_back(Slice(c));
  }
  ustore::VList col_list(col_vals);
  // put new list
  VMeta col_meta = odb_.Put(Slice(table_col_key), col_list,
                            Slice(kDefaultBranch));
  USTORE_GUARD(col_meta.code());
// Put the ucell hash of column list into tablemap
  std::string col_version_base32 = col_meta.version().ToBase32();
  table_map.Set(Slice(table_col_key), Slice(col_version_base32));
  VMeta updated_table_meta = odb_.Put(table_key, table_map, table_branch);
  USTORE_GUARD(updated_table_meta.code());
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::RemoveColumn(const std::string& table_name,
                                    const std::string& branch_name,
                                    const std::string& col_name) {
  Slice table_key(table_name);
  Slice table_branch(branch_name);

  VMeta table_meta = odb_.Get(table_key, table_branch);
  USTORE_GUARD(table_meta.code());
  VMap table_map = table_meta.Map();
  const std::string table_col_key = GetColumnKeyForTable(table_name, col_name);
  table_map.Remove(Slice(table_col_key));
  VMeta updated_table_meta = odb_.Put(table_key, table_map, table_branch);
  USTORE_GUARD(updated_table_meta.code());
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::DiffColumn(const std::string& lhs_table_name,
                                  const std::string& lhs_branch_name,
                                  const std::string& lhs_col_name,
                                  const std::string& rhs_table_name,
                                  const std::string& rhs_branch_name,
                                  const std::string& rhs_col_name,
                                  ColumnDiffIterator* itr) {
  // Get the ucells for two columns
  Column lhs_col, rhs_col;
  USTORE_GUARD(GetColumn(lhs_table_name, lhs_branch_name, lhs_col_name,
                         &lhs_col));
  USTORE_GUARD(GetColumn(rhs_table_name, rhs_branch_name, rhs_col_name,
                         &rhs_col));
  *itr = UList::DuallyDiff(lhs_col, rhs_col);
  return ErrorCode::kOK;
}

}  // namespace ca
}  // namespace example
}  // namespace ustore
