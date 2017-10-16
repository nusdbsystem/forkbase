// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_TABLE_OP_H_
#define USTORE_EXAMPLE_TABLE_OP_H_

#include "spec/relational.h"
#include "utils/timer.h"

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

  ErrorCode TimeComplete(const std::string& name,
                         const std::function<ErrorCode()>& f_exec);

  ColumnStore cs_;
};

}  // namespace table_op
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_TABLE_OP_H_
