// Copyright (c) 2017 The Ustore Authors.

#include "relational.h"

namespace ustore {
namespace example {
namespace ca {

const std::string ColumnStore::kDefaultBranch = "master";

ErrorCode ColumnStore::CreateTable(const std::string& table_name) {
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::BranchTable(const std::string& table_name,
                                   const std::string& tgt_branch_name,
                                   const std::string& ref_branch_name) {
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::DiffTable(const std::string& lhs_table_name,
                                 const std::string& lhs_branch_name,
                                 const std::string& rhs_table_name,
                                 const std::string& rhs_branch_name,
                                 TableDiffIterator* itr) {
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::MergeTable(
  const std::string& table_name, const std::string& tgt_branch_name,
  const std::string& ref_branch_name,
  const std::vector<std::pair<std::string, Column>>& updates) {
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::GetColumn(const std::string& table_name,
                                 const std::string& branch_name,
                                 const std::string& col_name, UCell* meta) {
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::PutColumn(const std::string& table_name,
                                 const std::string& branch_name,
                                 const std::string& col_name,
                                 const std::vector<std::string>& col) {
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::RemoveColumn(const std::string& table_name,
                                    const std::string& branch_name,
                                    const std::string& col_name) {
  return ErrorCode::kOK;
}

ErrorCode ColumnStore::DiffColumn(const std::string& lhs_table_name,
                                  const std::string& lhs_branch_name,
                                  const std::string& lhs_col_name,
                                  const std::string& rhs_table_name,
                                  const std::string& rhs_branch_name,
                                  const std::string& rhs_col_name,
                                  ColumnDiffIterator* itr) {
  return ErrorCode::kOK;
}


}  // namespace ca
}  // namespace example
}  // namespace ustore
