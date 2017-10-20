// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_TABLE_OP_ARGUMENT_H_
#define USTORE_EXAMPLE_TABLE_OP_ARGUMENT_H_

#include "utils/argument.h"

namespace ustore {
namespace example {
namespace table_op {

class Argument : public ::ustore::Argument {
 public:
  std::string file;
  std::string update_ref_col;
  std::string update_ref_val;
  std::string update_eff_col;
  std::string aggregate_col;

  double test_double;

  Argument() {
    Add(&file, "file", "f",
        "path of input file");
    Add(&update_ref_col, "update-ref-col", "r",
        "name of update-referring column");
    Add(&update_ref_val, "update-ref-val", "v",
        "path of update-referring-value file");
    Add(&update_eff_col, "update-eff-col", "e",
        "name of update-effecting column");
    Add(&aggregate_col, "aggregate-col", "a",
        "name of aggregating column");
  }

  ~Argument() = default;
};

}  // namespace table_op
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_TABLE_OP_ARGUMENT_H_
