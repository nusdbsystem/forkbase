// Copyright (c) 2017 The Ustore Authors.

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <string>
#include <vector>

#include "table_gen.h"

namespace ustore {
namespace example {
namespace table_op {

const char kOutputDelimiter[] = ",";

ErrorCode TableGen::Run() {
  std::vector<std::string> update_by_vals;
  for (int i = 0; i < args_.num_gen_update_refs; ++i) {
    update_by_vals.emplace_back(rand_.FixedString(12));
  }
  USTORE_GUARD(GenQueries(update_by_vals));
  USTORE_GUARD(GenData(update_by_vals));
  return ErrorCode::kOK;
}

ErrorCode TableGen::GenData(const std::vector<std::string>& vals) {
  std::ofstream ofs(args_.file);
  USTORE_GUARD(ofs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  // schema
  ofs << "ID" << kOutputDelimiter
      << "Name" << kOutputDelimiter
      << "UpdateBy" << kOutputDelimiter
      << "AggregateBy" << kOutputDelimiter
      << "UpdateOn" << std::endl;
  // rows
  int n_rows_per_update_by = args_.num_gen_rows / vals.size();
  size_t id = 0;
  for (auto& v : vals) {
    for (int i = 0; i < n_rows_per_update_by; ++i) {
      ofs << std::setfill('0') << std::setw(12) << ++id << kOutputDelimiter
          << rand_.RandomString(32) << kOutputDelimiter
          << v << kOutputDelimiter
          << rand_.RandomInt(1, 20) << kOutputDelimiter
          << rand_.FixedString(128) << std::endl;
    }
  }
  ofs.close();
  return ErrorCode::kOK;
}

ErrorCode TableGen::GenQueries(const std::vector<std::string>& vals) {
  std::ofstream ofs(args_.update_ref_val);
  USTORE_GUARD(ofs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  std::vector<size_t> all_indicies;
  for (size_t i = 0; i < vals.size(); ++i) all_indicies.emplace_back(i);
  rand_.Shuffle(&all_indicies);

  std::vector<size_t> query_indices;
  for (size_t i = 0; i < static_cast<size_t>(args_.num_gen_queries); ++i) {
    query_indices.emplace_back(all_indicies.at(i));
  }
  std::sort(query_indices.begin(), query_indices.end());

  for (auto& idx : query_indices) {
    ofs << vals.at(idx) << std::endl;
  }
  ofs.close();
  return ErrorCode::kOK;
}

}  // namespace table_op
}  // namespace example
}  // namespace ustore
