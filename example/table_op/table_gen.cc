// Copyright (c) 2017 The Ustore Authors.

#include <fstream>
#include <iomanip>
#include <vector>
#include "benchmark/bench_utils.h"
#include "utils/logging.h"

#include "table_gen.h"

namespace ustore {
namespace example {
namespace table_op {

const char kOutputDelimiter[] = ",";

ErrorCode TableGen::Run() {
  std::ofstream ofs_data(args_.file);
  USTORE_GUARD(ofs_data ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  std::ofstream ofs_update_refs(args_.update_ref_val);
  USTORE_GUARD(ofs_update_refs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  // schema
  ofs_data << "ID" << kOutputDelimiter
           << "Name" << kOutputDelimiter
           << "UpdateBy" << kOutputDelimiter
           << "AggregateBy" << kOutputDelimiter
           << "UpdateOn" << std::endl;
  // rows
  int n_rows_per_update_by = args_.num_gen_rows / args_.num_gen_update_refs;
  size_t id = 0;
  int prob_bar = static_cast<int>(10000.0 * args_.prob_to_update);
  RandomGenerator rand;
  for (int i = 0; i < args_.num_gen_update_refs; ++i) {
    const std::string val_update_by = rand.FixedString(8);
    for (int j = 0; j < n_rows_per_update_by; ++j) {
      ofs_data << std::setfill('0') << std::setw(12) << ++id << kOutputDelimiter
               << rand.RandomString(32) << kOutputDelimiter
               << val_update_by << kOutputDelimiter
               << rand.RandomInt(1, 20) << kOutputDelimiter
               << rand.FixedString(128) << std::endl;
    }
    if (rand.RandomInt(0, 10000) < prob_bar) {
      ofs_update_refs << val_update_by << std::endl;
    }
  }
  ofs_data.close();
  ofs_update_refs.close();
  return ErrorCode::kOK;
}

}  // namespace table_op
}  // namespace example
}  // namespace ustore
