// Copyright (c) 2017 The Ustore Authors.
#include <chrono>
#include <thread>

#include "worker/worker_ext.h"

#include "ca/analytics.h"
#include "ca/config.h"
#include "ca/relational.h"
#include "ca/utils.h"

#include "cluster/remote_client_service.h"

namespace ustore {
namespace example {
namespace ca {


constexpr int kWaitForSvcReadyInMs = 75;
ColumnStore* cs = nullptr;

int RunSample() {
  std::cout << std::endl
            << "-------------[ Sample Analytics ]------------" << std::endl;
  GUARD_INT(SampleAnalytics("sample", *cs).Compute(nullptr));
  std::cout << "---------------------------------------------" << std::endl;
  return 0;
}

int LoadDataset() {
  std::cout << std::endl
            << "-------------[ Loading Dataset ]-------------" << std::endl;
  auto ana = DataLoading("master", *cs, Config::n_columns, Config::n_records);
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
  GUARD_INT(PoissonAnalytics("poi_ana", *cs, mean).Compute(&aff_cols));
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
  GUARD_INT(BinomialAnalytics("bin_ana", *cs, p).Compute(&aff_cols));
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
  auto ana = MergeAnalytics("master", *cs);
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
  []() { return RunPoissonAnalytics(Config::p * Config::n_records); },
  []() { return RunBinomialAnalytics(Config::p); },
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
  if (task_id < 0 || task_id > task.size()) {
    std::cerr << "[FAILURE] Unknown task ID: " << task_id << std::endl;
    return -2;
  } else if (task_id == 0) {
    for (auto& t : task) TASK_GUARD(t());
  } else {
    // Run the specified task only
    TASK_GUARD(task[task_id - 1]());
  }
  return 0;
}

int main(int argc, char* argv[]) {
  SetStderrLogging(WARNING);
  bool is_valid = Config::ParseCmdArgs(argc, argv);
  if (Config::is_help) {
    DLOG(INFO) << "Help messages have been printed";
    return 0;
  } else if (!is_valid) {
    std::cerr << "[FAILURE] Found invalid command-line option" << std::endl;
    return -1;
  }

  RemoteClientService ustore_svc("");
  ustore_svc.Init();
  std::thread ustore_svc_thread(&RemoteClientService::Start, &ustore_svc);
  std::this_thread::sleep_for(
      std::chrono::milliseconds(kWaitForSvcReadyInMs));
  ClientDb* client_db = ustore_svc.CreateClientDb();
  cs = new ColumnStore(client_db);

  if (RunTask(Config::task_id) != 0) {
    LOG(WARNING) << "Fail to Run Task " << Config::task_id;
  }

  ustore_svc.Stop();
  ustore_svc_thread.join();

  delete client_db;
  delete cs;

  return 0;
}

}  // namespace ca
}  // namespace example
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::ca::main(argc, argv);
}
