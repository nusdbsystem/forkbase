// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_RELATIONAL_H_
#define USTORE_EXAMPLE_CA_RELATIONAL_H_

#include <string>
#include <vector>
#include <utility>

#include "spec/object_db.h"
#include "types/type.h"
#include "types/server/slist.h"

namespace ustore {
namespace example {
namespace ca {

using Meta = UCell;
using Column = VList;
using TableDiffIterator = DuallyDiffKeyIterator;
using ColumnDiffIterator = DuallyDiffIndexIterator;

class ColumnStore {
 public:
  static const std::string kDefaultBranch;

  explicit ColumnStore(DB2* db2) noexcept : odb_(db2) {}
  ~ColumnStore() = default;

  ErrorCode CreateTable(const std::string& table_name);

  ErrorCode BranchTable(const std::string& table_name,
                        const std::string& tgt_branch_name,
                        const std::string& ref_branch_name);

  ErrorCode DiffTable(const std::string& lhs_table_name,
                      const std::string& lhs_branch_name,
                      const std::string& rhs_table_name,
                      const std::string& rhs_branch_name,
                      TableDiffIterator* itr);

  ErrorCode MergeTable(const std::string& table_name,
                       const std::string& tgt_branch_name,
                       const std::string& ref_branch_name,
                       const std::string& removed_col_name);

  ErrorCode MergeTable(const std::string& table_name,
                       const std::string& tgt_branch_name,
                       const std::string& ref_branch_name,
                       const std::string& updated_col_name,
                       const std::vector<std::string>& updated_col);

  ErrorCode GetColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name,
                      Column* column);

  ErrorCode PutColumn(const std::string& table_name,
                      const std::string& branch_name,
                      const std::string& col_name,
                      const std::vector<std::string>& col);

  ErrorCode RemoveColumn(const std::string& table_name,
                         const std::string& branch_name,
                         const std::string& col_name);

  ErrorCode DiffColumn(const std::string& lhs_table_name,
                       const std::string& lhs_branch_name,
                       const std::string& lhs_col_name,
                       const std::string& rhs_table_name,
                       const std::string& rhs_branch_name,
                       const std::string& rhs_col_name,
                       ColumnDiffIterator* itr);

 private:
  // Generate key for a column for a table
  // This is both used as a key to store column as a VList
  //   and as a key in table VMap to reference the VList hash
  const std::string GetColumnKeyForTable(
      const std::string& table_name,
      const std::string& col_name) const {
    return table_name + "_" + col_name;
  }

  ObjectDB odb_;
};

}  // namespace ca
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_CA_RELATIONAL_H_
