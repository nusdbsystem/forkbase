// Copyright (c) 2017 The Ustore Authors.

#include <unistd.h>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include "benchmark/benchmark.h"
#include "cluster/remote_client_service.h"
#include "cluster/worker_service.h"
#include "spec/object_db.h"
#include "utils/env.h"
#include "worker/worker.h"

using namespace ustore;

constexpr int kSleepTime = 100000;

void BenchmarkWorker() {
  Worker worker {2018, false};
  ObjectDB db(&worker);
  std::vector<ObjectDB *> dbs;
  dbs.push_back(&db);
  Benchmark bm(dbs);
  bm.RunAll();
}

void BenchmarkClient() {
  // create worker service
  std::ifstream fin_worker(Env::Instance()->config().worker_file());
  std::string worker_addr;
  std::vector<WorkerService *> workers;
  while (fin_worker >> worker_addr)
    workers.push_back(new WorkerService(worker_addr, "", false));
  std::vector<std::thread> worker_threads;
  for (int i = 0; i < workers.size(); ++i) workers[i]->Init();
  for (int i = 0; i < workers.size(); ++i)
    worker_threads.push_back(std::thread(&WorkerService::Start, workers[i]));

  // create client service
  RemoteClientService *service = new RemoteClientService("");
  service->Init();
  std::thread client_service_thread(&RemoteClientService::Start, service);
  sleep(1);

  // create client
  size_t n_client = Env::Instance()->config().n_clients();
  std::vector<ClientDb*> clientdbs;
  std::vector<ObjectDB *> dbs;
  for (size_t i = 0; i < n_client; ++i) {
    ClientDb *cdb = new ClientDb();
    *cdb = service->CreateClientDb();
    clientdbs.push_back(cdb);
    dbs.push_back(new ObjectDB(cdb));
  }
  Benchmark bm(dbs);
  bm.RunAll();

  service->Stop();
  client_service_thread.join();
  delete service;
  usleep(kSleepTime);

  for (int i = 0; i < worker_threads.size(); ++i) {
    workers[i]->Stop();
    worker_threads[i].join();
    delete workers[i];
    usleep(kSleepTime);
  }
  for (auto &p : dbs) {
      delete p;
  }
  for (auto p : clientdbs)
    delete p;
}

int main() {
  // set num_segments large enough for all test cases
  Env::Instance()->m_config().set_num_segments(64);

  /*
  std::cout << "============================\n";
  std::cout << "Benchmarking worker.......\n";
  BenchmarkWorker();
  */

  std::cout << "============================\n";
  std::cout << "Benchmarking client.......\n";
  BenchmarkClient();
  return 0;
}
