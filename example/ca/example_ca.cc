// Copyright (c) 2017 The Ustore Authors.

#include <utility>
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "utils/logging.h"
#include "worker/worker.h"

#include "analytics.h"
#include "config.h"
#include "simple_dataset.h"
#include "utils.h"

namespace ustore {
namespace example {
namespace ca {

Worker worker(Config::kWorkID);

void LoadDataset() {
  std::cout << std::endl
            << "-------------[ Loading Dataset ]-------------" << std::endl;
  const auto data = SimpleDataset::GenerateTable(
                      Config::n_columns, Config::n_records);
  const Slice branch("master");
  for (const auto& cv : data) {
    const auto col_name = Slice(cv.first);
    const auto col_str = Utils::ToString(cv.second);
    const auto col_value = Value(Slice(col_str));
    worker.Put(col_name, col_value, branch);
    Utils::Print(col_name, branch, worker);
  }
  std::cout << "---------------------------------------------" << std::endl;
}

void RunPoissonAnalytics(const double mean) {
  std::cout << std::endl
            << "------------[ Poisson Analytics ]------------" << std::endl;
  const std::string branch("poi_ana");
  const auto aff_cols = PoissonAnalytics(branch, worker, mean).Compute();
  std::cout << ">>> Affected Columns <<<" << std::endl;
  for (const auto& c : aff_cols) Utils::Print(c, branch, worker);
  std::cout << "---------------------------------------------" << std::endl;
}

void RunBinomialAnalytics(const double p) {
  std::cout << std::endl
            << "-----------[ Binomial Analytics ]------------" << std::endl;
  const std::string branch("bin_ana");
  const auto aff_cols = BinomialAnalytics(branch, worker, p).Compute();
  std::cout << ">>> Affected Columns <<<" << std::endl;
  for (const auto& c : aff_cols) Utils::Print(c, branch, worker);
  std::cout << "---------------------------------------------" << std::endl;
}

void MergeResults() {
  std::cout << std::endl
            << "-------------[ Merging Results ]-------------" << std::endl;
  auto ana = MergeAnalytics("master", worker);
  std::cout << ">>> Before Merging <<<" << std::endl;
  Utils::Print("distr", "master", worker);
  ana.Compute();
  std::cout << ">>> After Merging <<<" << std::endl;
  Utils::Print("distr", "master", worker);
  Utils::Print("distr", "poi_ana", worker);
  Utils::Print("distr", "bin_ana", worker);
  std::cout << "---------------------------------------------" << std::endl;
}

static int main(int argc, char* argv[]) {
  if (Config::ParseCmdArgs(argc, argv)) {
    LoadDataset();
    RunPoissonAnalytics(Config::p * Config::n_records);
    RunBinomialAnalytics(Config::p);
    MergeResults();
  } else if (Config::is_help) {
    DLOG(INFO) << "Help messages have been printed";
  } else {
    LOG(ERROR) << "Invalid command option has been found";
    return 1;
  }
  return 0;
}

} // namespace ca
} // namespace example
} // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::ca::main(argc, argv);
}
