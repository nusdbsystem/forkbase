// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_TABLE_OP_ARGUMENTS_H_
#define USTORE_EXAMPLE_TABLE_OP_ARGUMENTS_H_

#include <string>
#include "utils/arguments.h"

namespace ustore {
namespace example {
namespace table_op {

class Arguments : public ::ustore::Arguments {
 public:
  std::string file;
  std::string update_ref_col;
  std::string update_ref_val;
  std::string update_eff_col;
  std::string aggregate_col;

  Arguments() {
    Add(&file, "file", "f",
        "path of input file");
    Positional("file");
    Add(&update_ref_col, "update-ref-col", "r",
        "name of update-referring column");
    Add(&update_ref_val, "update-ref-val", "v",
        "path of update-referring-value file");
    Add(&update_eff_col, "update-eff-col", "e",
        "name of update-effecting column");
    Add(&aggregate_col, "aggregate-col", "a",
        "name of aggregating column");
  }

  ~Arguments() = default;
};

}  // namespace table_op
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_TABLE_OP_ARGUMENTS_H_
