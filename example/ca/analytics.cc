// Copyright (c) 2017 The Ustore Authors.

#include "ca/analytics.h"

#include <vector>

namespace ustore {
namespace example {
namespace ca {

int SampleAnalytics::Compute(std::unordered_set<std::string>* aff_cols) {
  const std::string table_name("Test");
  USTORE_GUARD_INT(
    cs_.CreateTable(table_name, "master"));
  USTORE_GUARD_INT(
    cs_.PutColumn(table_name, "master", "col-1", {"1", "2", "3", "4"}));
  USTORE_GUARD_INT(
    cs_.PutColumn(table_name, "master", "col-2", {"4", "3", "2", "1", "0"}));
  USTORE_GUARD_INT(
    cs_.BranchTable(table_name, "master", branch_));

  Column col1, col2;
  USTORE_GUARD_INT(
    cs_.GetColumn(table_name, branch_, "col-1", &col1));
  USTORE_GUARD_INT(
    cs_.GetColumn(table_name, branch_, "col-2", &col2));

  std::vector<std::string> col;
  for (auto it = col1.Scan(); !it.end(); it.next()) {
    col.emplace_back("*" + it.value().ToString());
  }
  USTORE_GUARD_INT(
    cs_.PutColumn(table_name, branch_, "result", col));
  Column col_rst;
  USTORE_GUARD_INT(
    cs_.GetColumn(table_name, branch_, "result", &col1));

  auto it_diff = cs_.DiffColumn(col1, col_rst);
  std::cout << ">>> col-1 vs. result <<<" << std::endl;
  Utils::PrintListDiff(it_diff);
  std::cout << std::endl;
  return 0;
}

const std::string kTableName = "Sample";

int DataLoading::Compute(std::unordered_set<std::string>* aff_cols) {
  USTORE_GUARD_INT(
    cs_.CreateTable(kTableName, branch_));

  // generate a column of data and put it into the store
  const auto f_gen_col = [this, aff_cols](
  const std::string & col_name, const std::string& val_init = "") {
    const std::string init = (val_init.empty() ? col_name : val_init) + "_";
    std::vector<std::string> col_vals;
    for (size_t i = 0; i < n_records_; ++i) {
      col_vals.emplace_back(init + std::to_string(i));
    }
    USTORE_GUARD(
      cs_.PutColumn(kTableName, branch_, col_name, col_vals));
    aff_cols->insert(col_name);
    return ErrorCode::kOK;
  };
  USTORE_GUARD_INT(f_gen_col("Key", "K"));
  for (size_t i = 0; i < n_columns_; ++i) {
    auto idx_str = std::to_string(i);
    const std::string col_name = "Col-" + idx_str;
    USTORE_GUARD_INT(f_gen_col(col_name, idx_str));
  }
  return 0;
}

int PoissonAnalytics::Compute(std::unordered_set<std::string>* aff_cols) {
  USTORE_GUARD_INT(
    cs_.BranchTable(kTableName, "master", branch_));
  std::cout << ">>> Referring Columns <<<" << std::endl;
  Column keys, col1;
  USTORE_GUARD_INT(
    cs_.GetColumn(kTableName, branch_, "Key", &keys));
  Utils::Print(kTableName, branch_, "Key", keys);
  USTORE_GUARD_INT(
    cs_.GetColumn(kTableName, branch_, "Col-1", &col1));
  Utils::Print(kTableName, branch_, "Col-1", col1);

  // generate pseudo analytics results
  std::vector<int> ana_vals_int(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) {
    auto n = NextRandom();
    if (n < Config::n_records) ++ana_vals_int[n];
  }

  // store the results
  std::vector<std::string> ana_vals_str;
  for (auto& v : ana_vals_int) ana_vals_str.emplace_back(std::to_string(v));
  USTORE_GUARD_INT(
    cs_.PutColumn(kTableName, branch_, "distr", ana_vals_str));
  if (aff_cols != nullptr) aff_cols->insert("distr");
  return 0;
}

int BinomialAnalytics::Compute(std::unordered_set<std::string>* aff_cols) {
  USTORE_GUARD_INT(
    cs_.BranchTable(kTableName, "master", branch_));
  std::cout << ">>> Referring Columns <<<" << std::endl;
  Column keys, col0, col2;
  USTORE_GUARD_INT(
    cs_.GetColumn(kTableName, branch_, "Key", &keys));
  Utils::Print(kTableName, branch_, "Key", keys);
  USTORE_GUARD_INT(
    cs_.GetColumn(kTableName, branch_, "Col-0", &col0));
  Utils::Print(kTableName, branch_, "Col-0", col0);
  USTORE_GUARD_INT(
    cs_.GetColumn(kTableName, branch_, "Col-2", &col2));
  Utils::Print(kTableName, branch_, "Col-2", col2);

  // generate pseudo analytics results
  std::vector<int> ana_vals_int(Config::n_records);
  for (size_t i = 0; i < Config::iters; ++i) ++ana_vals_int[NextRandom()];

  // store the results
  std::vector<std::string> ana_vals_str;
  for (auto& v : ana_vals_int) ana_vals_str.emplace_back(std::to_string(v));
  USTORE_GUARD_INT(
    cs_.PutColumn(kTableName, branch_, "distr", ana_vals_str));
  if (aff_cols != nullptr) aff_cols->insert("distr");
  return 0;
}

int MergeAnalytics::Compute(std::unordered_set<std::string>* aff_cols) {
  std::cout << ">>> Referring Columns <<<" << std::endl;
  Column distr_poi, distr_bin;
  USTORE_GUARD_INT(
    cs_.GetColumn(kTableName, "poi_ana", "distr", &distr_poi));
  Utils::Print(kTableName, "poi_ana", "distr", distr_poi);
  USTORE_GUARD_INT(
    cs_.GetColumn(kTableName, "bin_ana", "distr", &distr_bin));
  Utils::Print(kTableName, "bin_ana", "distr", distr_bin);

  std::cout << ">>> Differences: distr, @poi_ana vs @bin_ana <<<" << std::endl;
  Utils::PrintListDiff(cs_.DiffColumn(distr_poi, distr_bin));
  std::cout << std::endl;

  // construct the merged result
  std::vector<int> ana_vals_int;
  for (auto it_poi = distr_poi.Scan(), it_bin = distr_bin.Scan();
       !it_poi.end() && !it_bin.end(); it_poi.next(), it_bin.next()) {
    auto val_poi = std::stoi(it_poi.value().ToString());
    auto val_bin = std::stoi(it_bin.value().ToString());
    ana_vals_int.push_back((val_poi + val_bin) / 2);
  }

  // store as the merged results
  std::vector<std::string> ana_vals_str;
  for (auto& v : ana_vals_int) ana_vals_str.emplace_back(std::to_string(v));
  USTORE_GUARD_INT(
    cs_.MergeTable(kTableName, branch_, "poi_ana", "distr", ana_vals_str));
  USTORE_GUARD_INT(
    cs_.MergeTable(kTableName, branch_, "bin_ana", "distr", ana_vals_str));
  if (aff_cols != nullptr) aff_cols->insert("distr");
  return 0;
}

}  // namespace ca
}  // namespace example
}  // namespace ustore
