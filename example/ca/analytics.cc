// Copyright (c) 2017 The Ustore Authors.

#include <utility>
#include <vector>

#include "analytics.h"

namespace ustore {
namespace example {
namespace ca {

int DataLoading::Compute(StringSet* aff_cols) {
  const auto f_gen_col = [this, aff_cols](
  const std::string & col_name, const std::string& val_init = "") {
    DCHECK(!col_name.empty());
    const std::string init = (val_init.empty() ? col_name : val_init) + "-";
    StringList col;
    for (size_t i = 0; i < n_records_; ++i) {
      col.emplace_back(init + std::to_string(i));
    }
    const auto col_str = Utils::ToString(col);
    const auto col_value = Value(Slice(col_str));
    const auto col_name_slice = Slice(col_name);
    USTORE_GUARD(db_.Put(col_name_slice, col_value, branch_));
    aff_cols->insert(col_name);
    return ErrorCode::kOK;
  };

  USTORE_GUARD_INT(f_gen_col("Key", "K"));
  for (size_t i = 0; i < n_columns_; ++i) {
    const std::string col_name = "C" + std::to_string(i);
    USTORE_GUARD_INT(f_gen_col(col_name));
  }
  return 0;
}

int PoissonAnalytics::Compute(StringSet* aff_cols) {
  std::cout << ">>> Referring Columns <<<" << std::endl;
  Value key;
  USTORE_GUARD_INT(BranchAndLoad("Key", "master", &key));
  USTORE_GUARD_INT(Utils::Print("Key", branch_, db_));
  Value col1;
  USTORE_GUARD_INT(BranchAndLoad("C1", "master", &col1));
  USTORE_GUARD_INT(Utils::Print("C1", branch_, db_));

  const std::string rst_col_name("distr");
  std::vector<int> rst_col(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) {
    const auto n = NextRandom();
    if (n < Config::n_records) ++rst_col[n];
  }
  const auto rst_col_str = Utils::ToString(rst_col);
  USTORE_GUARD_INT(
    db_.Put(Slice(rst_col_name), Value(Slice(rst_col_str)), branch_));
  if (aff_cols != nullptr) aff_cols->insert(std::move(rst_col_name));
  return 0;
}

int BinomialAnalytics::Compute(StringSet* aff_cols) {
  std::cout << ">>> Referring Columns <<<" << std::endl;
  Value key;
  USTORE_GUARD_INT(BranchAndLoad("Key", "master", &key));
  USTORE_GUARD_INT(Utils::Print("Key", branch_, db_));
  Value col0;
  USTORE_GUARD_INT(BranchAndLoad("C0", "master", &col0));
  USTORE_GUARD_INT(Utils::Print("C0", branch_, db_));
  Value col2;
  USTORE_GUARD_INT(BranchAndLoad("C2", "master", &col2));
  USTORE_GUARD_INT(Utils::Print("C2", branch_, db_));

  const std::string rst_col_name("distr");
  std::vector<int> rst_col(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) {
    ++rst_col[NextRandom()];
  }
  const auto rst_col_str = Utils::ToString(rst_col);
  USTORE_GUARD_INT(
    db_.Put(Slice(rst_col_name), Value(Slice(rst_col_str)), branch_));
  if (aff_cols != nullptr) aff_cols->insert(std::move(rst_col_name));
  return 0;
}

int MergeAnalytics::Compute(StringSet* aff_cols) {
  const Slice col_name("distr");
  const Slice branch_poi("poi_ana");
  const Slice branch_bin("bin_ana");

  std::cout << ">>> Referring Columns <<<" << std::endl;
  Utils::Print("Key", branch_, db_);
  Utils::Print(col_name, branch_poi, db_);
  Utils::Print(col_name, branch_bin, db_);

  USTORE_GUARD_INT(db_.Branch(col_name, branch_poi, branch_));

  Value col;
  USTORE_GUARD_INT(db_.Get(col_name, branch_, &col));
  auto master_vec = Utils::ToIntVector(col.slice().ToString());
  USTORE_GUARD_INT(db_.Get(col_name, branch_bin, &col));
  const auto bin_vec = Utils::ToIntVector(col.slice().ToString());

  CHECK_EQ(master_vec.size(), bin_vec.size());
  for (size_t i = 0; i < master_vec.size(); ++i) {
    master_vec[i] = (master_vec[i] + bin_vec[i]) / 2;
  }

  const auto col_str = Utils::ToString(master_vec);
  USTORE_GUARD_INT(
    db_.Merge(col_name, Value(Slice(col_str)), branch_, branch_bin));
  if (aff_cols != nullptr) aff_cols->insert(col_name.ToString());
  return 0;
}

}  // namespace ca
}  // namespace example
}  // namespace ustore
