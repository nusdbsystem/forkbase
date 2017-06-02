// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_RELATIONAL_H_
#define USTORE_SPEC_RELATIONAL_H_

#include <string>
#include <vector>
#include "spec/object_db.h"
#include "utils/utils.h"

namespace ustore {

using Table = VMap;
using Column = VList;
using Version = std::string;
using TableDiffIterator = DuallyDiffKeyIterator;
using ColumnDiffIterator = DuallyDiffIndexIterator;

class ColumnStore {
 public:
  explicit ColumnStore(DB* db) noexcept : odb_(db) {}
  ~ColumnStore() = default;

  ErrorCode CreateTable(const std::string& table_name,
                        const std::string& branch_name);

  ErrorCode BranchTable(const std::string& table_name,
                        const std::string& old_branch_name,
                        const std::string& new_branch_name);

  ErrorCode DiffTable(const std::string& lhs_table_name,
                      const std::string& lhs_branch_name,
                      const std::string& rhs_table_name,
                      const std::string& rhs_branch_name,
                      TableDiffIterator* it_diff);

  inline ErrorCode DiffTable(const std::string& table_name,
                             const std::string& lhs_branch_name,
                             const std::string& rhs_branch_name,
                             TableDiffIterator* it_diff) {
    return DiffTable(table_name, lhs_branch_name, table_name, rhs_branch_name,
                     it_diff);
  }

  ErrorCode MergeTable(const std::string& table_name,
                       const std::string& tgt_branch_name,
                       const std::string& ref_branch_name,
                       const std::string& remove_col_name);

  ErrorCode MergeTable(const std::string& table_name,
                       const std::string& tgt_branch_name,
                       const std::string& ref_branch_name,
                       const std::string& new_col_name,
                       const std::vector<std::string>& new_col_vals);

  ErrorCode GetColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name,
                      Column* col);

  ErrorCode PutColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name,
                      const std::vector<std::string>& col_vals);

  ErrorCode RemoveColumn(const std::string& table_name,
                         const std::string& branch_name,
                         const std::string& col_name);

  inline ColumnDiffIterator DiffColumn(const Column& lhs, const Column& rhs) {
    return UList::DuallyDiff(lhs, rhs);
  }

  ErrorCode DiffColumn(const std::string& lhs_table_name,
                       const std::string& lhs_branch_name,
                       const std::string& lhs_col_name,
                       const std::string& rhs_table_name,
                       const std::string& rhs_branch_name,
                       const std::string& rhs_col_name,
                       ColumnDiffIterator* it_diff);

  inline ErrorCode DiffColumn(const std::string& col_name,
                              const std::string& lhs_table_name,
                              const std::string& lhs_branch_name,
                              const std::string& rhs_table_name,
                              const std::string& rhs_branch_name,
                              ColumnDiffIterator* it_diff) {
    return DiffColumn(lhs_table_name, lhs_branch_name, col_name,
                      rhs_table_name, rhs_branch_name, col_name, it_diff);
  }

  inline ErrorCode DiffColumn(const std::string& table_name,
                              const std::string& col_name,
                              const std::string& lhs_branch_name,
                              const std::string& rhs_branch_name,
                              ColumnDiffIterator* it_diff) {
    return DiffColumn(table_name, lhs_branch_name, col_name,
                      table_name, rhs_branch_name, col_name, it_diff);
  }

 private:
  template<class T1, class T2>
  inline const std::string GlobalKey(const T1& table_name,
                                     const T2& col_name) const {
    return Utils::ToString(table_name) + "::" + Utils::ToString(col_name);
  }

  template<class T1, class T2>
  ErrorCode GetTable(const T1& table_name, const T2& branch_name,
                     Table* table);

  template<class T1, class T2, class T3>
  ErrorCode WriteColumn(
    const T1& table_name, const T2& branch_name, const T3& col_name,
    const std::vector<Slice>& col_vals, Version* ver);

  template<class T1, class T2, class T3>
  ErrorCode ReadColumn(const T1& table_name, const T2& branch_name,
                       const T3& col_name, Column* col);

  template<class T1, class T2, class T3>
  inline ErrorCode WriteColumn(
    const T1& table_name, const T2& branch_name, const T3& col_name,
    const std::vector<std::string>& col_vals, Version* ver) {
    std::vector<Slice> col_slices;
    for (const auto& str : col_vals) col_slices.emplace_back(str);
    return WriteColumn(table_name, branch_name, col_name, col_slices, ver);
  }

  ObjectDB odb_;
};

template<class T1, class T2>
ErrorCode ColumnStore::GetTable(const T1& table_name, const T2& branch_name,
                                Table* table) {
  auto tab_rst = odb_.Get(Slice(table_name), Slice(branch_name));
  USTORE_GUARD(tab_rst.stat);
  *table = tab_rst.value.Map();
  return ErrorCode::kOK;
}

template<class T1, class T2, class T3>
ErrorCode ColumnStore::ReadColumn(const T1& table_name, const T2& branch_name,
                                  const T3& col_name, Column* col) {
  auto col_key = GlobalKey(table_name, col_name);
  auto col_rst = odb_.Get(Slice(col_key), Slice(branch_name));
  USTORE_GUARD(col_rst.stat);
  *col = col_rst.value.List();
  return ErrorCode::kOK;
}

template<class T1, class T2, class T3>
ErrorCode ColumnStore::WriteColumn(
  const T1& table_name, const T2& branch_name, const T3& col_name,
  const std::vector<Slice>& col_vals, Version* ver) {
  auto col_key = GlobalKey(table_name, col_name);
  Column col(col_vals);
  auto col_rst = odb_.Put(Slice(col_key), col, Slice(branch_name));
  USTORE_GUARD(col_rst.stat);
  *ver = col_rst.value.ToBase32();
  return ErrorCode::kOK;
}

}  // namespace ustore

#endif  // USTORE_SPEC_RELATIONAL_H_
