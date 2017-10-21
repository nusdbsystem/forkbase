// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_TABLE_OP_TABLE_OP_H_
#define USTORE_EXAMPLE_TABLE_OP_TABLE_OP_H_

#include <string>
#include "spec/relational.h"
#include "utils/timer.h"

#include "arguments.h"

namespace ustore {
namespace example {
namespace table_op {

class TableOp {
 public:
  explicit TableOp(DB* db) noexcept;
  ~TableOp() = default;

  ErrorCode Run(int argc, char* argv[]);

 private:
  ErrorCode Init();
  ErrorCode Load();
  ErrorCode Update(const std::string& ref_val);
  ErrorCode Diff(const std::string& lhs_branch, const std::string& rhs_branch);
  ErrorCode Aggregate();

  ErrorCode VerifyColumn(const std::string& col);
  
  ErrorCode MeasureByteIncrement(const std::function<ErrorCode()>& f,
                                 size_t* bytes_inc);

  ColumnStore cs_;
  Arguments args_;
};

}  // namespace table_op
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_TABLE_OP_TABLE_OP_H_
