// Copyright (c) 2017 The Ustore Authors.
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <set>
#include <string>
#include "gtest/gtest.h"
#include "proto/messages.pb.h"
#include "types/type.h"
#include "utils/env.h"
#include "cluster/worker_service.h"
#include "cluster/remote_client_service.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/blob.h"
#include "utils/logging.h"

using ustore::UStoreMessage;
using ustore::byte_t;
using ustore::WorkerService;
using ustore::RemoteClientService;
using ustore::Config;
using ustore::Slice;
using ustore::Hash;
using ustore::Blob;
using ustore::Env;
using ustore::ErrorCode;
using ustore::ClientDb;
using ustore::Value;
using std::thread;
using std::vector;
using std::set;
using std::ifstream;
using std::string;

const int NREQUESTS = 4;
const string keys[] = {"aaa", "bbb", "ccc", "ddd"};
const string values[] = {"where is the wisdome in knowledge",
                         "where is the knowledge in information",
                         "the brown fox",
                         "jump over"};

// i^th thread issue requests from i*(nthreads/nreqs) to
// (i+1)*(nthreads/nreqs)
void TestClientRequest(ClientDb* client, int idx, int len) {
  Hash HEAD_VERSION = Hash::ComputeFrom((const byte_t*)("head"), 4);

  // put
  Hash version;
  EXPECT_EQ(client->Put(Slice(keys[idx]),
        Value(Blob((const byte_t*)values[idx].data(),
        values[idx].length())), HEAD_VERSION, &version), ErrorCode::kOK);
  LOG(INFO) << "PUT version : " << version.ToBase32();

  // get it back
  Value value;
  EXPECT_EQ(client->Get(Slice(keys[idx]), version, &value), ErrorCode::kOK);

  LOG(INFO) << "GET value : " << string((const char*)value.blob().data(),
                                            value.blob().size());
  // branch from head
  string new_branch = "branch_"+std::to_string(idx);
  EXPECT_EQ(client->Branch(Slice(keys[idx]),
                              version, Slice(new_branch)), ErrorCode::kOK);

  // put on the new branch
  Hash branch_version;
  EXPECT_EQ(client->Put(Slice(keys[idx]),
      Value(Blob((const byte_t *)values[idx].data(),
   values[idx].length())), Slice(new_branch), &branch_version), ErrorCode::kOK);

  LOG(INFO) << "PUT version: " << branch_version.ToBase32() << std::endl;

  // merge
  Hash merge_version;
  EXPECT_EQ(client->Merge(Slice(keys[idx]),
        Value(Blob((const byte_t *)values[idx].data(), values[idx].length())),
                Slice(new_branch), version, &merge_version), ErrorCode::kOK);
  LOG(INFO) << "MERGE version: " << merge_version.ToBase32() << std::endl;

  EXPECT_EQ(client->Merge(Slice(keys[idx]),
        Value(Blob((const byte_t *)values[idx].data(), values[idx].length())),
                version, branch_version, &merge_version),
                ErrorCode::kOK);
  LOG(INFO) << "MERGE version: " << merge_version.ToBase32() << std::endl;
}

TEST(TestMessage, TestClient1Thread) {
  ustore::SetStderrLogging(ustore::WARNING);
  // launch workers
  ifstream fin(Env::Instance()->config()->worker_file());
  string worker_addr;
  vector<WorkerService*> workers;
  while (fin >> worker_addr)
    workers.push_back(new WorkerService(worker_addr, ""));

  vector<thread> worker_threads;
  for (int i = 0; i < workers.size(); i++)
    workers[i]->Init();

  for (int i = 0; i < workers.size(); i++)
      worker_threads.push_back(thread(&WorkerService::Start, workers[i]));

  // launch clients
  ifstream fin_client(Env::Instance()->config()->clientservice_file());
  string clientservice_addr;
  fin_client >> clientservice_addr;
  RemoteClientService *service
    = new RemoteClientService(clientservice_addr, "");
  service->Init();
  // service->Start();
  thread client_service_thread(&RemoteClientService::Start, service);
  sleep(1);

  // 1 thread
  ClientDb *client = service->CreateClientDb();
  TestClientRequest(client, 0, NREQUESTS);

  service->Stop();
  client_service_thread.join();

  // then stop workers
  for (WorkerService *ws : workers) {
    ws->Stop();
  }
  for (int i = 0; i < worker_threads.size(); i++)
    worker_threads[i].join();

  // delete workers and client
  for (int i=0; i< workers.size(); i++)
    delete workers[i];
  delete service;
}

/*
TEST(TestMessage, TestClient2Threads) {
  ustore::SetStderrLogging(ustore::WARNING);
  // launch workers
  ifstream fin(Env::Instance()->config()->worker_file());
  string worker_addr;
  vector<WorkerService*> workers;
  while (fin >> worker_addr)
    workers.push_back(new WorkerService(worker_addr, ""));

  vector<thread> worker_threads;
  vector<thread> client_threads;
  for (int i = 0; i < workers.size(); i++)
    workers[i]->Init();

  for (int i = 0; i < workers.size(); i++)
      worker_threads.push_back(thread(&WorkerService::Start, workers[i]));

  // launch clients
  ifstream fin_client(Env::Instance()->config()->clientservice_file());
  string clientservice_addr;
  fin_client >> clientservice_addr;
  RemoteClientService *service
    = new RemoteClientService(clientservice_addr, "");
  service->Init();
  // service->Start();
  thread client_service_thread(&RemoteClientService::Start, service);
  sleep(1);

  // 2 clients thread
  for (int i = 0; i < 2; i++) {
    ClientDb *client = service->CreateClientDb();
    client_threads.push_back(thread(
                        &TestClientRequest, client, i*2, NREQUESTS/2));
  }

  // wait for them to join
  for (int i = 0; i < 2; i++)
    client_threads[i].join();

  service->Stop();
  client_service_thread.join();

  // then stop workers
  for (WorkerService *ws : workers) {
    ws->Stop();
  }
  for (int i = 0; i < worker_threads.size(); i++)
    worker_threads[i].join();

  // clean up
  for (int i=0; i< workers.size(); i++)
    delete workers[i];
  delete service;
}
*/
