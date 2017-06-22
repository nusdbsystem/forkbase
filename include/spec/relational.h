// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_RELATIONAL_H_
#define USTORE_SPEC_RELATIONAL_H_

// #define __MOCK_FLUSH__  // for bottleneck test only
#define __FAST_STRING_TOKENIZE__

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "spec/object_db.h"
#include "utils/blocking_queue.h"
#include "utils/utils.h"

namespace ustore {

using Table = VMap;
using Column = VList;
using Row = std::unordered_map<std::string, std::string>;
using Version = std::string;
using TableDiffIterator = DuallyDiffKeyIterator;
using ColumnDiffIterator = DuallyDiffIndexIterator;

class ColumnStore {
 public:
  explicit ColumnStore(DB* db) noexcept : odb_(db) {}
  ~ColumnStore() = default;

  ErrorCode ExistsTable(const std::string& table_name, bool* exist);

  ErrorCode ExistsTable(const std::string& table_name,
                        const std::string& branch_name, bool* exist);

  ErrorCode CreateTable(const std::string& table_name,
                        const std::string& branch_name);

  ErrorCode LoadCSV(const std::string& file_path,
                    const std::string& table_name,
                    const std::string& branch_name,
                    size_t batch_size = 5000, bool print_progress = true);

  ErrorCode DumpCSV(const std::string& file_path,
                    const std::string& table_name,
                    const std::string& branch_name);

  ErrorCode GetTable(const std::string& table_name,
                     const std::string& branch_name, Table* table);

  ErrorCode BranchTable(const std::string& table_name,
                        const std::string& old_branch_name,
                        const std::string& new_branch_name);

  ErrorCode ListTableBranch(const std::string& table_name,
                            std::vector<std::string>* branches);

  inline TableDiffIterator DiffTable(const Table& lhs, const Table& rhs) {
    return UMap::DuallyDiff(lhs, rhs);
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

  ErrorCode DeleteTable(const std::string& table_name,
                        const std::string& branch_name);

  ErrorCode ExistsColumn(const std::string& table_name,
                         const std::string& col_name, bool* exist);

  ErrorCode ExistsColumn(const std::string& table_name,
                         const std::string& branch_name,
                         const std::string& col_name, bool* exist);

  ErrorCode GetColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name, Column* col);

  ErrorCode PutColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name,
                      const std::vector<std::string>& col_vals);

  ErrorCode ListColumnBranch(const std::string& table_name,
                             const std::string& col_name,
                             std::vector<std::string>* branches);

  ErrorCode DeleteColumn(const std::string& table_name,
                         const std::string& branch_name,
                         const std::string& col_name);

  ErrorCode GetRow(const std::string& table_name,
                   const std::string& branch_name,
                   const std::string& ref_col_name,
                   const std::string& ref_val,
                   std::unordered_map<size_t, Row>* rows);

  inline ColumnDiffIterator DiffColumn(const Column& lhs, const Column& rhs) {
    return UList::DuallyDiff(lhs, rhs);
  }

  template<class T1, class T2>
  static inline std::string GlobalKey(const T1& table_name,
                                      const T2& col_name) {
    return Utils::ToString(table_name) + "::" + Utils::ToString(col_name);
  }

 private:
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

  ErrorCode LoadCSV(
    std::ifstream& ifs, const std::string& table_name,
    const std::string& branch_name, const std::vector<std::string>& col_names,
    size_t batch_size, bool print_progress);

  void ShardCSV(
    std::ifstream& ifs, size_t batch_size, size_t n_cols,
    BlockingQueue<std::vector<std::vector<std::string>>>& batch_queue,
    const ErrorCode& stat_flush);

  void FlushCSV(
    const std::string& table_name, const std::string& branch_name,
    const std::vector<std::string>& col_names,
    BlockingQueue<std::vector<std::vector<std::string>>>& batch_queue,
    ErrorCode& stat, bool print_progress);

  ObjectDB odb_;
};

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
