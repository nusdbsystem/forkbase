// Copyright (c) 2017 The Ustore Authors.

#include <unistd.h>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include "benchmark/benchmark.h"
#include "benchmark/bench_config.h"
#include "cluster/worker_client_service.h"
#include "cluster/worker_service.h"
#include "spec/object_db.h"
#include "utils/env.h"
#include "worker/worker.h"

using namespace ustore;

constexpr int kSleepTime = 100000;

void BenchmarkWorker() {
  Worker worker {2018, nullptr, false};
  ObjectDB db(&worker);
  std::vector<ObjectDB*> dbs;
  dbs.push_back(&db);
  Benchmark bm(dbs);
  std::cout << "============================\n";
  std::cout << "Benchmarking worker (single-threaded) .......\n";
  bm.Run();
}

void BenchmarkClient() {
  // create worker service
  std::ifstream fin_worker(Env::Instance()->config().worker_file());
  std::string worker_addr;
  std::vector<WorkerService*> workers;
  while (fin_worker >> worker_addr)
    workers.push_back(new WorkerService(worker_addr, false));
  for (auto& worker : workers) worker->Run();
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
  std::cout << "Benchmarking "
            << n_client << " clients to in-proc worker service .......\n";
  bm.Run();

  service.Stop();
  for (auto& p : dbs) delete p;
  for (auto& worker : workers) delete worker;
}

int main(int argc, char* argv[]) {
  if (BenchmarkConfig::ParseCmdArgs(argc, argv)) {
    if (BenchmarkConfig::is_help) return 0;
  } else {
    std::cerr << BOLD_RED("[FAILURE] ")
              << "Found invalid command-line option" << std::endl;
    return -1;
  }
  Env::Instance()->m_config().set_worker_file("conf/test_single_worker.lst");
  BenchmarkWorker();
  BenchmarkClient();
  return 0;
}
