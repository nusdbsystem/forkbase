// Copyright (c) 2017 The Ustore Authors.

#include <unistd.h>
#include <cstdio>
#include <thread>
#include "benchmark/benchmark.h"
#include "benchmark/bench_config.h"
#include "cluster/worker_client_service.h"
#include "spec/object_db.h"
#include "utils/env.h"

using namespace ustore;

constexpr int kSleepTime = 100000;

void BenchmarkClient() {
  // create client service
  WorkerClientService service;
  service.Run();

  // create client
  size_t n_client = BenchmarkConfig::num_clients;
  std::vector<WorkerClient> clientdbs;
  std::vector<ObjectDB*> dbs;
  for (size_t i = 0; i < n_client; ++i)
    clientdbs.push_back(service.CreateWorkerClient());
  for (auto& db : clientdbs)
    dbs.push_back(new ObjectDB(&db));
  Benchmark bm(dbs);
  std::cout << "============================\n";
  std::cout << "Benchmarking " << n_client
            << " clients connected to ustore service.......\n";
  bm.Run();

  service.Stop();
  for (auto& p : dbs) delete p;
}

int main(int argc, char* argv[]) {
  if (BenchmarkConfig::ParseCmdArgs(argc, argv)) {
    if (BenchmarkConfig::is_help) return 0;
  } else {
    std::cerr << BOLD_RED("[FAILURE] ")
              << "Found invalid command-line option" << std::endl;
    return -1;
  }
  BenchmarkClient();
  return 0;
}
