// Copyright (c) 2017 The Ustore Authors.

#include <utility>
#include <vector>
#include "spec/value.h"
#include "types/type.h"

#include "analytics.h"
#include "utils.h"

namespace ustore {
namespace example {
namespace ca {

StringSet PoissonAnalytics::Compute() {
  std::cout << ">>> Referring Columns <<<" << std::endl;
  const Value key = BranchAndLoad("Key", "master");
  Utils::Print("Key", branch_, worker_);
  const Value col1 = BranchAndLoad("C1", "master");
  Utils::Print("C1", branch_, worker_);

  StringSet affected_cols;
  const std::string rst_col_name("distr");
  std::vector<int> rst_col(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) {
    const auto n = NextRandom();
    if (n < Config::n_records) ++rst_col[n];
  }
  const auto rst_col_str = Utils::ToString(rst_col);
  worker_.Put(Slice(rst_col_name), Value(Slice(rst_col_str)), branch_);
  affected_cols.insert(std::move(rst_col_name));
  return affected_cols;
}

StringSet BinomialAnalytics::Compute() {
  std::cout << ">>> Referring Columns <<<" << std::endl;
  const Value key = BranchAndLoad("Key", "master");
  Utils::Print("Key", branch_, worker_);
  const Value col0 = BranchAndLoad("C0", "master");
  Utils::Print("C0", branch_, worker_);
  const Value col2 = BranchAndLoad("C2", "master");
  Utils::Print("C2", branch_, worker_);

  StringSet affected_cols;
  const std::string rst_col_name("distr");
  std::vector<int> rst_col(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) {
    ++rst_col[NextRandom()];
  }
  const auto rst_col_str = Utils::ToString(rst_col);
  worker_.Put(Slice(rst_col_name), Value(Slice(rst_col_str)), branch_);
  affected_cols.insert(std::move(rst_col_name));
  return affected_cols;
}

StringSet MergeAnalytics::Compute() {
  const Slice col_name("distr");
  const Slice branch_poi("poi_ana");
  const Slice branch_bin("bin_ana");

  std::cout << ">>> Referring Columns <<<" << std::endl;
  Utils::Print("Key", branch_, worker_);
  Utils::Print(col_name, branch_poi, worker_);
  Utils::Print(col_name, branch_bin, worker_);

  worker_.Branch(col_name, branch_poi, branch_);

  Value col;
  worker_.Get(col_name, branch_, &col);
  auto master_vec = Utils::ToIntVector(col.slice().to_string());
  worker_.Get(col_name, branch_bin, &col);
  const auto bin_vec = Utils::ToIntVector(col.slice().to_string());

  CHECK_EQ(master_vec.size(), bin_vec.size());
  for (size_t i = 0; i < master_vec.size(); ++i) {
    master_vec[i] = (master_vec[i] + bin_vec[i]) / 2;
  }

  const auto col_str = Utils::ToString(master_vec);
  worker_.Merge(col_name, Value(Slice(col_str)), branch_, branch_bin);

  StringSet affected_cols;
  affected_cols.insert(col_name.to_string());
  return affected_cols;
}

} // namespace ca
} // namespace example
} // namespace ustore
