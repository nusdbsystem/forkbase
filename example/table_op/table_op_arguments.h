// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_TABLE_OP_TABLE_OP_ARGUMENTS_H_
#define USTORE_EXAMPLE_TABLE_OP_TABLE_OP_ARGUMENTS_H_

#include <string>
#include <vector>
#include "utils/arguments.h"

namespace ustore {
namespace example {
namespace table_op {

class TableOpArguments : public ::ustore::Arguments {
 public:
  std::string file;
  std::string update_ref_col;
  std::string update_ref_val;
  std::string update_eff_col;
  std::string aggregate_col;
  bool to_gen_data;
  int64_t num_gen_rows;
  int num_gen_update_refs;
  double prob_to_update;
  bool is_diff;
  bool is_at_svr;

  TableOpArguments() {
    AddPositional(&file, "file", "path of input file");
    Add(&update_ref_col, "update-ref-col", "r",
        "name of update-referring column");
    Add(&update_ref_val, "update-ref-val", "v",
        "path of update-referring-value file");
    Add(&update_eff_col, "update-eff-col", "e",
        "name of update-effecting column");
    Add(&aggregate_col, "aggregate-col", "a",
        "name of aggregating column");
    Add(&to_gen_data, "gen-data", "G", "generate synthetic data");
    Add(&num_gen_rows, "num-gen-rows", "N",
        "# of rows to generate", 100);
    Add(&num_gen_update_refs, "num-gen-refs", "R",
        "# of update-referring values to generate", 5);
    Add(&prob_to_update, "prob-update", "P",
        "probability of updating a referring value", 0.25);
    Add(&is_diff, "diff", "", "perform DIFF operation");
    Add(&is_at_svr, "at-server", "", "enable server-side operation");
  }

  ~TableOpArguments() = default;

  bool CheckArgs() override {
    GUARD(CheckGT(num_gen_update_refs, 0,
                  "Number of update-referring values"));
    GUARD(CheckGE(num_gen_rows, num_gen_update_refs,
                  "Number of rows"));
    GUARD(CheckInRange(prob_to_update, 0.0, 1.0,
                       "Probability"));
    return true;
  }

  std::string MoreHelpMessage() override {
    const std::string cmd = "./ustore_example_table_op";
    const std::vector<std::string> args = {
      "-G data.csv -v query.txt",
      "data.csv -r UpdateBy -v query.txt -e UpdateOn -a AggregateBy"
    };
    std::stringstream ss;
    ss << "Example:" << std::endl;
    for (auto& a : args) {
      ss << "  " << cmd << " " << a << std::endl;
    }
    ss << std::endl;
    return ss.str();
  }
};

}  // namespace table_op
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_TABLE_OP_TABLE_OP_ARGUMENTS_H_
