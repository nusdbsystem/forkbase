// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_ARGUMENTS_H_
#define USTORE_EXAMPLE_CA_ARGUMENTS_H_

#include <string>
#include "utils/arguments.h"

namespace ustore {
namespace example {
namespace ca {

class Arguments : public ::ustore::Arguments {
 public:
  int task_id;
  int64_t n_columns;
  int64_t n_records;
  double p;
  int64_t iters;

  Arguments() {
    Add(&task_id, "task", "t", "ID of analytics task", 0);
    Add(&n_columns, "columns", "c", "number of columns in a simple table", 3);
    Add(&n_records, "records", "n", "number of records in a simple table", 10);
    Add(&p, "probability", "p",
        "probability used in the analytical simulation", 0.01);
    Add(&iters, "iterations", "i",
        "number of iterations in the analytical simulation", 1000);
  }

  bool CheckArgs() override {
    GUARD(CheckGE(task_id, 0, "Task ID"));
    GUARD(CheckGE(n_columns, 3, "Number of columns"));
    GUARD(CheckGT(n_records, 0, "Number of records"));
    GUARD(CheckInRange(p, 0, 1, "Probability"));
    GUARD(CheckGT(iters, 0, "Number of iterations"));
    return true;
  }

  ~Arguments() = default;
};

}  // namespace ca
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_CA_ARGUMENTS_H_
