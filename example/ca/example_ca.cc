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

MAP_TYPE<KeyType, std::string> data;

Worker worker(Config::kWorkID);
const Slice branch_master("master");

void LoadDataset() {
  Hash version;
  ErrorCode ec;
  for (const auto& cv : data) {
    const auto col_name = Slice(cv.first);
    const auto col_value = Value(Slice(cv.second));
    ec = worker.Put(col_name, col_value, branch_master, &version);
    CHECK(ec == ErrorCode::kOK);
  }
}

void ScanBranchMaster() {
  ErrorCode ec;
  Value col_value;
  for (const auto& cv : data) {
    const auto col_name = Slice(cv.first);
    ec = worker.Get(col_name, branch_master, &col_value);
    CHECK(col_value.type() == UType::kString);
    CHECK_EQ(cv.second, col_value.slice());
  }
}

void RunPoissonAnalytics(const double mean) {
  std::cout << std::endl
            << "------------[ Poisson Analytics ]------------" << std::endl;
  const std::string branch("poi_ana");
  const auto aff_cols = PoissonAnalytics(branch, worker, mean).Compute();
  std::cout << "[Affected Columns]" << std::endl;
  for (const auto& c : aff_cols) {
    Utils::Print(c, branch, worker);
  }
  std::cout << "---------< End of Poisson Analytics >--------" << std::endl;
}

void RunBinomialAnalytics(const double p) {
  std::cout << std::endl
            << "-----------[ Binomial Analytics ]------------" << std::endl;
  const std::string branch("bin_ana");
  const auto aff_cols = BinomialAnalytics(branch, worker, p).Compute();
  std::cout << "[Affected Columns]" << std::endl;
  for (const auto& c : aff_cols) {
    Utils::Print(c, branch, worker);
  }
  std::cout << "--------< End of Binomial Analytics >--------" << std::endl;
}

static int main(int argc, char* argv[]) {
  if (Config::ParseCmdArgs(argc, argv)) {

    data = Utils::ToStringMap(SimpleDataset::GenerateTable(
                                Config::kDefaultNumColumns, Config::n_records));
    LoadDataset();
    ScanBranchMaster();

    RunPoissonAnalytics(Config::p * Config::n_records);
    RunBinomialAnalytics(Config::p);
    return 0;
  } else {
    return 1;
  }
}

} // namespace ca
} // namespace example
} // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::ca::main(argc, argv);
}
