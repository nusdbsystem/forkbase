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

Value Analytics::BranchAndLoad(const Slice& col_name,
                               const Slice& base_branch) {
  worker_.Branch(col_name, base_branch, branch_);

  Value col;
  worker_.Get(col_name, branch_, &col);
  DCHECK(col.type() == UType::kString);
  return col;
}

std::unordered_set<std::string> PoissonAnalytics::Compute() {
  const Value col0 = std::move(BranchAndLoad("C0", "master"));
  const Value col1 = std::move(BranchAndLoad("C1", "master"));

  std::unordered_set<std::string> affected_cols;
  const std::string rst_col_name("distr");
  std::vector<int> rst_col(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) {
    const auto n = NextRandom();
    if (n < Config::n_records) ++rst_col[n];
  }
  worker_.Put(Slice(rst_col_name), Value(Slice(Utils::ToString(rst_col))),
              branch_);
  affected_cols.insert(std::move(rst_col_name));
  return affected_cols;
}

std::unordered_set<std::string> BinomialAnalytics::Compute() {
  const Value col0 = std::move(BranchAndLoad("C0", "master"));
  const Value col2 = std::move(BranchAndLoad("C2", "master"));

  std::unordered_set<std::string> affected_cols;
  const std::string rst_col_name("distr");
  std::vector<int> rst_col(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) {
    ++rst_col[NextRandom()];
  }
  worker_.Put(Slice(rst_col_name), Value(Slice(Utils::ToString(rst_col))),
              branch_);
  affected_cols.insert(std::move(rst_col_name));
  return affected_cols;
}

} // namespace ca
} // namespace example
} // namespace ustore
