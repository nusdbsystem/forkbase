// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_TABLE_OP_TABLE_GEN_H_
#define USTORE_EXAMPLE_TABLE_OP_TABLE_GEN_H_

#include "table_op_arguments.h"

namespace ustore {
namespace example {
namespace table_op {

class TableGen {
 public:
  explicit TableGen(TableOpArguments& args) noexcept : args_(args) {}

  ~TableGen() = default;

  ErrorCode Run();

 private:
  TableOpArguments& args_;
};

}  // namespace table_op
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_TABLE_OP_TABLE_GEN_H_
