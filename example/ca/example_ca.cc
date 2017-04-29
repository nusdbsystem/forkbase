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
#include "utils.h"

namespace ustore {
namespace example {
namespace ca {

Worker db(Config::kWorkID);

int LoadDataset() {
  std::cout << std::endl
            << "-------------[ Loading Dataset ]-------------" << std::endl;
  auto ana = DataLoading("master", db, Config::n_columns, Config::n_records);
  StringSet aff_cols;
  GUARD_INT(ana.Compute(&aff_cols));
  for (const auto& c : aff_cols)
    for (const auto& b : db.ListBranch(Slice(c)))
      USTORE_GUARD_INT(Utils::Print(c, b, db));
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int RunPoissonAnalytics(const double mean) {
  std::cout << std::endl
            << "------------[ Poisson Analytics ]------------" << std::endl;
  const std::string branch("poi_ana");
  StringSet aff_cols;
  GUARD_INT(PoissonAnalytics(branch, db, mean).Compute(&aff_cols));
  std::cout << ">>> Affected Columns <<<" << std::endl;
  for (const auto& c : aff_cols) {
    USTORE_GUARD_INT(Utils::Print(c, branch, db));
  }
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int RunBinomialAnalytics(const double p) {
  std::cout << std::endl
            << "-----------[ Binomial Analytics ]------------" << std::endl;
  const std::string branch("bin_ana");
  StringSet aff_cols;
  GUARD_INT(BinomialAnalytics(branch, db, p).Compute(&aff_cols));
  std::cout << ">>> Affected Columns <<<" << std::endl;
  for (const auto& c : aff_cols) {
    USTORE_GUARD_INT(Utils::Print(c, branch, db));
  }
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int MergeResults() {
  std::cout << std::endl
            << "-------------[ Merging Results ]-------------" << std::endl;
  auto ana = MergeAnalytics("master", db);
  const Slice col_name("distr");
  std::cout << ">>> Before Merging <<<" << std::endl;
  USTORE_GUARD_INT(Utils::Print(col_name, "master", db));
  StringSet aff_cols;
  GUARD_INT(ana.Compute(nullptr));
  std::cout << ">>> After Merging <<<" << std::endl;
  for (const auto& branch : db.ListBranch(col_name)) {
    USTORE_GUARD_INT(Utils::Print(col_name, branch, db));
  }
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

#define MAIN_GUARD(op) do { \
  auto ec = op; \
  if (ec != 0) { \
    std::cerr << "[FAILURE] Error code: " << ec << std::endl; \
    return ec; \
  } \
} while (0)

int main(int argc, char* argv[]) {
  if (Config::ParseCmdArgs(argc, argv)) {
    MAIN_GUARD(LoadDataset());
    MAIN_GUARD(RunPoissonAnalytics(Config::p * Config::n_records));
    MAIN_GUARD(RunBinomialAnalytics(Config::p));
    MAIN_GUARD(MergeResults());
  } else if (Config::is_help) {
    DLOG(INFO) << "Help messages have been printed";
  } else {
    std::cerr << "[FAILURE] Found invalid command-line option" << std::endl;
    return -1;
  }
  return 0;
}

} // namespace ca
} // namespace example
} // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::ca::main(argc, argv);
}
