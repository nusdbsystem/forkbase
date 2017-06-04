// Copyright (c) 2017 The Ustore Authors.

#include <unistd.h>
#include <cstdio>
#include <thread>
#include "benchmark/benchmark.h"
#include "cluster/remote_client_service.h"
#include "spec/object_db.h"
#include "utils/env.h"

using namespace ustore;

constexpr int kSleepTime = 100000;

void BenchmarkClient() {
  // create client service
  RemoteClientService service("");
  service.Init();
  std::thread client_service_thread(&RemoteClientService::Start, &service);
  sleep(1);

  // create client
  size_t n_client = Env::Instance()->config().n_clients();
  std::vector<ClientDb*> clientdbs;
  std::vector<ObjectDB *> dbs;
  for (size_t i = 0; i < n_client; ++i) {
    ClientDb *cdb = new ClientDb();
    *cdb = service.CreateClientDb();
    clientdbs.push_back(cdb);
    dbs.push_back(new ObjectDB(cdb));
  }
  Benchmark bm(dbs);
  bm.RunAll();

  for (auto d : clientdbs)
    delete d;

  service.Stop();
  client_service_thread.join();
  usleep(kSleepTime);
}

int main() {
  std::cout << "============================\n";
  std::cout << "Benchmarking client connected to ustore service.......\n";
  BenchmarkClient();
  return 0;
}
