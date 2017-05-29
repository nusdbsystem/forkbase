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
  RemoteClientService *service = new RemoteClientService("");
  service->Init();
  std::thread client_service_thread(&RemoteClientService::Start, service);
  sleep(1);

  // create client
  ClientDb *client = service->CreateClientDb();
  ObjectDB db(client);
  Benchmark bm(&db);
  bm.RunAll();

  service->Stop();
  client_service_thread.join();
  delete service;
  usleep(kSleepTime);
}

int main() {
  std::cout << "============================\n";
  std::cout << "Benchmarking client connected to ustore service.......\n";
  BenchmarkClient();
  return 0;
}

