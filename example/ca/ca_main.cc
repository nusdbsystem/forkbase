// Copyright (c) 2017 The Ustore Authors.
#include <chrono>
#include <thread>

#include "cluster/worker_client_service.h"
#include "spec/relational.h"

#include "ca/arguments.h"
#include "ca/analytics.h"
#include "ca/utils.h"

namespace ustore {
namespace example {
namespace ca {

ColumnStore* cs = nullptr;
Arguments args;

int RunSample() {
  std::cout << std::endl
            << "-------------[ Sample Analytics ]------------" << std::endl;
  GUARD_INT(SampleAnalytics("sample", args, *cs).Compute(nullptr));
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int LoadDataset() {
  std::cout << std::endl
            << "-------------[ Loading Dataset ]-------------" << std::endl;
  auto ana = DataLoading("master", args, *cs, args.n_columns, args.n_records);
  std::unordered_set<std::string> aff_cols;
  GUARD_INT(ana.Compute(&aff_cols));
  for (const auto& col_name : aff_cols) {
    Column col;
    USTORE_GUARD_INT(cs->GetColumn("Sample", "master", col_name, &col));
    Utils::Print("Sample", "master", col_name, col);
  }
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int RunPoissonAnalytics(const double mean) {
  std::cout << std::endl
            << "------------[ Poisson Analytics ]------------" << std::endl;
  std::unordered_set<std::string> aff_cols;
  GUARD_INT(PoissonAnalytics("poi_ana", args, *cs, mean).Compute(&aff_cols));
  std::cout << ">>> Affected Columns <<<" << std::endl;
  for (const auto& col_name : aff_cols) {
    Column col;
    USTORE_GUARD_INT(cs->GetColumn("Sample", "poi_ana", col_name, &col));
    Utils::Print("Sample", "poi_ana", col_name, col);
  }
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int RunBinomialAnalytics(const double p) {
  std::cout << std::endl
            << "-----------[ Binomial Analytics ]------------" << std::endl;
  std::unordered_set<std::string> aff_cols;
  GUARD_INT(BinomialAnalytics("bin_ana", args, *cs, p).Compute(&aff_cols));
  std::cout << ">>> Affected Columns <<<" << std::endl;
  for (const auto& col_name : aff_cols) {
    Column col;
    USTORE_GUARD_INT(cs->GetColumn("Sample", "bin_ana", col_name, &col));
    Utils::Print("Sample", "bin_ana", col_name, col);
  }
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int MergeResults() {
  std::cout << std::endl
            << "-------------[ Merging Results ]-------------" << std::endl;
  auto ana = MergeAnalytics("master", args, *cs);
  std::unordered_set<std::string> aff_cols;
  GUARD_INT(ana.Compute(&aff_cols));
  std::cout << ">>> Affected Columns <<<" << std::endl;
  for (const auto& col_name : aff_cols) {
    Column col;
    USTORE_GUARD_INT(cs->GetColumn("Sample", "master", col_name, &col));
    Utils::Print("Sample", "master", col_name, col);
  }
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

std::vector<std::function<int()>> task = {
  []() { return LoadDataset(); },
  []() { return RunPoissonAnalytics(args.p * args.n_records); },
  []() { return RunBinomialAnalytics(args.p); },
  []() { return MergeResults(); }
};

#define TASK_GUARD(op) do { \
  auto ec = op; \
  if (ec != 0) { \
    std::cerr << "[FAILURE] Error code: " << ec << std::endl; \
    return ec; \
  } \
} while (0)

int RunTask(const int task_id) {
  if (task_id < 0 || task_id > int(task.size())) {
    std::cerr << BOLD_RED("[FAILURE] ")
              << "Unrecognized task ID: " << task_id << std::endl;
    return -2;
  } else if (task_id == 0) {  // Run all the tasks as a batch
    for (auto& t : task) TASK_GUARD(t());
  } else {  // Run the specified task only
    TASK_GUARD(task[task_id - 1]());
  }
  return 0;
}

int main(int argc, char* argv[]) {
  SetStderrLogging(WARNING);
  if (!args.ParseCmdArgs(argc, argv)) {
    if (args.is_help) {
      DLOG(INFO) << "Help messages have been printed";
      return 0;
    } else {
      std::cerr << BOLD_RED("[FAILURE] ")
                << "Found invalid command-line option" << std::endl;
      return -1;
    }
  }
  // connect to UStore servcie
  WorkerClientService ustore_svc;
  ustore_svc.Run();
  WorkerClient client_db = ustore_svc.CreateWorkerClient();
  cs = new ColumnStore(&client_db);
  // run analytics task
  auto ec = RunTask(args.task_id);
  if (ec != 0) {
    std::cerr << "Fail to run Task " << args.task_id << std::endl;
  }
  // disconnect with UStore service
  ustore_svc.Stop();
  delete cs;
  return ec;
}

}  // namespace ca
}  // namespace example
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::ca::main(argc, argv);
}
