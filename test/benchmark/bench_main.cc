// Copyright (c) 2017 The Ustore Authors.

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

constexpr int val_size = 100;
constexpr int max_str_len = 32;
constexpr int fixed_str_len = 16;
constexpr int max_blob_size = 4096;
constexpr int fixed_blob_size = 4096;
constexpr int kSleepTime = 100000;

void BenchmarkWorker() {
  Worker worker {27};
  ObjectDB db(&worker);
  Benchmark bm(&db, max_str_len, fixed_str_len);

  bm.SliceValidation(val_size);
  bm.BlobValidation(val_size);
  bm.FixedString(fixed_str_len);
  bm.FixedBlob(max_blob_size);
  bm.RandomString(max_str_len);
  bm.RandomBlob(fixed_blob_size);
}

void BenchmarkClient() {
  std::ifstream fin_worker(Env::Instance()->config()->worker_file());
  std::string worker_addr;
  std::vector<WorkerService*> workers;
  while (fin_worker >> worker_addr)
    workers.push_back(new WorkerService(worker_addr, ""));

  std::vector<std::thread> worker_threads;
  for (int i = 0; i < workers.size(); ++i)
    workers[i]->Init();

  for (int i = 0; i< workers.size(); ++i)
    worker_threads.push_back(std::thread(&WorkerService::Start, workers[i]));

  std::ifstream fin_client(Env::Instance()->config()->clientservice_file());
  std::string clientservice_addr;
  fin_client >> clientservice_addr;
  RemoteClientService *service
    = new RemoteClientService(clientservice_addr, "");
  service->Init();
  std::thread client_service_thread(&RemoteClientService::Start, service);
  sleep(1);

  ClientDb *client = service->CreateClientDb();
  ObjectDB db(client);
  Benchmark bm(&db, max_str_len, fixed_str_len);
  bm.SliceValidation(val_size);
  bm.BlobValidation(val_size);
  bm.FixedString(fixed_str_len);
  bm.FixedBlob(max_blob_size);
  bm.RandomString(max_str_len);
  bm.RandomBlob(fixed_blob_size);

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
}

int main() {
  // remove ustore data before benchmark
  std::remove("ustore.dat");

  std::cout << "============================\n";
  std::cout << "Benchmarking worker.......\n";
  BenchmarkWorker();

  std::cout << "============================\n";
  std::cout << "Benchmarking client.......\n";
  BenchmarkClient();
  return 0;
}

