// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_TABLE_OP_TABLE_OP_H_
#define USTORE_EXAMPLE_TABLE_OP_TABLE_OP_H_

#include <string>
#include "spec/relational.h"
#include "utils/timer.h"

#include "table_op_arguments.h"

namespace ustore {
namespace example {
namespace table_op {

class TableOp {
 public:
  explicit TableOp(TableOpArguments& args, DB* db) noexcept
    : cs_(db), args_(args) {}

  ~TableOp() = default;

  ErrorCode Run();

 private:
  ErrorCode Init();
  ErrorCode Load();
  ErrorCode Update(const std::string& ref_val);
  ErrorCode Aggregate();
  ErrorCode Diff(const std::string& lhs_branch, const std::string& rhs_branch);

  ErrorCode DropIfExists(const std::string& tab);

  ErrorCode VerifyColumn(const std::string& col);

  ErrorCode MeasureByteIncrement(size_t* bytes_inc,
                                 const std::function<ErrorCode()>& f);

  ColumnStore cs_;
  TableOpArguments& args_;
};

}  // namespace table_op
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_TABLE_OP_TABLE_OP_H_
