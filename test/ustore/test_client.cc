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
#include "types/ucell.h"
#include "utils/env.h"
#include "cluster/worker_service.h"
#include "cluster/remote_client_service.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "utils/logging.h"

using ustore::byte_t;
using ustore::WorkerService;
using ustore::RemoteClientService;
using ustore::Config;
using ustore::Slice;
using ustore::Hash;
using ustore::Env;
using ustore::ErrorCode;
using ustore::ClientDb;
using ustore::Value;
using ustore::UType;
using ustore::UCell;
using ustore::StoreInfo;
using std::thread;
using std::vector;
using std::set;
using std::ifstream;
using std::string;

const int NREQUESTS = 4;
const string keys[] = {"aaa", "bbb", "ccc", "ddd"};
const string random_key = "eee";
const string values[] = {"where is the wisdome in knowledge",
                         "where is the knowledge in information",
                         "the brown fox",
                         "jump over"};

const int kSleepTime = 100000;

// i^th thread issue requests from i*(nthreads/nreqs) to
// (i+1)*(nthreads/nreqs)
void TestClientRequest(ClientDb* client, int idx, int len) {
  int start_idx = idx;
  for (int k = 0; k < len; k++) {
    idx = (start_idx + k) % NREQUESTS;
    Hash HEAD_VERSION = Hash::kNull;

    // put a string
    Value string_val;
    string_val.type = UType::kString;
    string_val.base = Hash::kNull;
    string_val.vals.push_back(Slice(values[idx]));
    Hash version;
    EXPECT_EQ(client->Put(Slice(keys[idx]), string_val, HEAD_VERSION, &version),
              ErrorCode::kOK);
    DLOG(INFO) << "PUT version (string): " << version.ToBase32();

    // put a list of 2 values
    Value list_val;
    list_val.type = UType::kList;
    list_val.base = Hash::kNull;
    list_val.vals.push_back(Slice(values[0]));
    list_val.vals.push_back(Slice(values[idx]));
    Hash version_list;
    EXPECT_EQ(client->Put(Slice(keys[idx]), list_val, HEAD_VERSION,
              &version_list), ErrorCode::kOK);
    DLOG(INFO) << "PUT version (list): " << version_list.ToBase32();

    // get the string back
    UCell string_value;
    EXPECT_EQ(client->Get(Slice(keys[idx]), version, &string_value),
              ErrorCode::kOK);
    EXPECT_EQ(string_value.type(), UType::kString);
    DLOG(INFO) << "GET datahash (string): "
               <<  string_value.dataHash().ToBase32();
    // get the list back
    UCell list_value;
    EXPECT_EQ(client->Get(Slice(keys[idx]), version_list, &list_value),
              ErrorCode::kOK);
    EXPECT_EQ(list_value.type(), UType::kList);
    DLOG(INFO) << "GET datahash (list): " <<  list_value.dataHash().ToBase32();

    // check GetChunk
    ustore::Chunk chunk;
    EXPECT_EQ(client->GetChunk(Slice(keys[idx]), version_list, &chunk),
              ErrorCode::kOK);
    EXPECT_EQ(chunk.numBytes(), list_value.chunk().numBytes());

    // branch from head
    string new_branch = "branch_"+std::to_string(idx);
    EXPECT_EQ(client->Branch(Slice(keys[idx]),
              version, Slice(new_branch)), ErrorCode::kOK);

    // put on the new branch (string value)
    Hash branch_version;
    EXPECT_EQ(client->Put(Slice(keys[idx]), string_val, Slice(new_branch),
              &branch_version), ErrorCode::kOK);

    DLOG(INFO) << "PUT version (new branch): " << branch_version.ToBase32()
               << std::endl;

    Hash q_branch;
    client->GetBranchHead(Slice(keys[idx]), Slice(new_branch), &q_branch);
    EXPECT_EQ(branch_version, q_branch);
    // merge
    Hash merge_version;
    EXPECT_EQ(client->Merge(Slice(keys[idx]), string_val, Slice(new_branch),
              version, &merge_version), ErrorCode::kOK);
    DLOG(INFO) << "MERGE version (w/o branch): " << merge_version.ToBase32()
               << std::endl;

    // Check branch head
    bool is_head;
    client->IsBranchHead(Slice(keys[idx]), Slice(new_branch), branch_version,
                         &is_head);
    EXPECT_TRUE(!is_head);
    client->IsBranchHead(Slice(keys[idx]), Slice(new_branch), merge_version,
                         &is_head);
    EXPECT_TRUE(is_head);
    EXPECT_EQ(client->Merge(Slice(keys[idx]), string_val, version,
              branch_version, &merge_version), ErrorCode::kOK);
    DLOG(INFO) << "MERGE version (with branch): " << merge_version.ToBase32()
               << std::endl;

    // Get latest versions
    vector<Hash> versions;
    client->GetLatestVersions(Slice(keys[idx]), &versions);
    EXPECT_EQ(versions.size(), 3);
    bool is_latest;
    client->IsLatestVersion(Slice(keys[idx]), versions[0], &is_latest);
    EXPECT_TRUE(is_latest);

    client->Delete(Slice(keys[idx]), Slice(new_branch));
  }
  // list keys
  vector<string> q_keys;
  client->ListKeys(&q_keys);
  EXPECT_EQ(q_keys.size(), len);

  // list branches
  vector<string> branches;
  client->ListBranches(Slice(keys[idx]), &branches);
  EXPECT_EQ(branches.size(), 0);  // branch was deleted

  // check exist
  bool exists;
  client->Exists(Slice(keys[(idx + 1) % NREQUESTS]), &exists);
  EXPECT_TRUE(exists);
  client->Exists(Slice(random_key), &exists);
  EXPECT_TRUE(!exists);

  // check info
  std::vector<StoreInfo> info;
  ErrorCode err = client->GetStorageInfo(&info);
  EXPECT_EQ(err, ErrorCode::kOK);
}

TEST(TestMessage, TestClient1Thread) {
  // launch workers
  ifstream fin(Env::Instance()->config().worker_file());
  string worker_addr;
  vector<WorkerService*> workers;
  while (fin >> worker_addr)
    workers.push_back(new WorkerService(worker_addr, "", false));

  vector<thread> worker_threads;
  for (int i = 0; i < workers.size(); i++)
    workers[i]->Init();

  for (int i = 0; i < workers.size(); i++)
    worker_threads.push_back(thread(&WorkerService::Start, workers[i]));

  // launch clients
  RemoteClientService service("");

  service.Init();
  // service->Start();
  thread client_service_thread(&RemoteClientService::Start, &service);
  usleep(kSleepTime);

  // 1 thread
  ClientDb client = service.CreateClientDb();
  TestClientRequest(&client, 0, NREQUESTS);

  // stop the client service
  service.Stop();
  client_service_thread.join();
  usleep(kSleepTime);

  // stop workers
  for (int i = 0; i < worker_threads.size(); i++) {
    workers[i]->Stop();
    worker_threads[i].join();
    delete workers[i];
    usleep(kSleepTime);
  }
}

/*
TEST(TestMessage, TestClient2Threads) {
  ustore::SetStderrLogging(ustore::WARNING);
  // launch workers
  ifstream fin(Env::Instance()->config().worker_file());
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
  ifstream fin_client(Env::Instance()->config().clientservice_file());
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
