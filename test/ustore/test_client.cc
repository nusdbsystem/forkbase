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
#include "utils/config.h"
#include "cluster/worker_service.h"
#include "cluster/client_service.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/blob.h"
#include "utils/logging.h"

using ustore::UStoreMessage;
using ustore::byte_t;
using ustore::WorkerService;
using ustore::ClientService;
using ustore::Config;
using ustore::Slice;
using ustore::Hash;
using ustore::Blob;

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
#ifdef MOCK_TEST
const vector<string> PUT_VERSIONS = {"V3IF4YAWUNEXGCVSJWXKKVBBQ6FPT6FN",
                             "PRZKYN2N4DXGGPER6AQUL4YN5OLIRJHB",
                             "2WJJWZGMUUAYFR6XR7FOZ75LFUZYRUOC",
                             "XTEZHUXGH5CBAF75ZQ537KVOA6EQZOD4",
                             "HFZZEM2WDNAG357XTCHGYOFHZNHPFENA",
                             "NPLDYND2PVBYMTNXZBKYV377WLHVVKIL",
                             "XODFC75WABMWZEQSGPSBE4DEGLWNCNTZ",
                             "34E222W7XF35LSHFOJE66OM7TOZX32D4"};
const vector<string> MERGE_VERSIONS = {"34E222W7XF35LSHFOJE66OM7TOZX32D4",
                               "V3IF4YAWUNEXGCVSJWXKKVBBQ6FPT6FN",
                               "PRZKYN2N4DXGGPER6AQUL4YN5OLIRJHB",
                               "2WJJWZGMUUAYFR6XR7FOZ75LFUZYRUOC"};
const string GET_VALUE =
                "I am a spy, a sleeper, a spook, a man of two faces";

bool check_results(const vector<string>& accepted, const string& result) {
  for (int i = 0; i < accepted.size(); i++) {
    if (accepted[i] == result)
      return true;
  }
  return false;
}
#endif

ustore::TestWorkload::TestWorkload(const int nthreads, const int nreqs) :
                            nthreads_(nthreads), nrequests_(nreqs) {
  for (int i = 0; i < nthreads; i++)
    req_idx_.push_back(i*nreqs/nthreads);
}

// i^th thread issue requests from i*(nthreads/nreqs) to
// (i+1)*(nthreads/nreqs)
bool ustore::TestWorkload::NextRequest(ustore::RequestHandler *reqhl) {
  Hash HEAD_VERSION;
  HEAD_VERSION.Compute((const byte_t*)("head"), 4);
  int tid = reqhl->id();
  int idx = req_idx_[tid];
  if (idx >= (tid+1)*(nrequests_/nthreads_))
    return false;

  // put
  UStoreMessage *msg = reinterpret_cast<UStoreMessage *>
                (reqhl->Put(Slice(keys[idx]), Slice(values[idx]),
                      HEAD_VERSION));

  Hash version((const byte_t*)(msg->put_response_payload())
                                .new_version().data());
#ifdef MOCK_TEST
  EXPECT_EQ(check_results(PUT_VERSIONS, version.ToBase32()), true);
#else
  DLOG(INFO) << "PUT version : " << version.ToBase32();
#endif
  delete msg;

  // get it back
  msg = reinterpret_cast<UStoreMessage *>
              (reqhl->Get(Slice(keys[idx]), version));
  Blob value((const byte_t*)msg->get_response_payload().value().data(),
              msg->get_response_payload().value().length());
#ifdef MOCK_TEST
  EXPECT_EQ(string((const char*)value.data(), value.size()), GET_VALUE);
#else
  DLOG(INFO) << "GET value : " << string((const char*)value.data(),
                                            value.size());
#endif
  delete msg;

  // branch from head
  string new_branch = "branch_"+std::to_string(idx);
  msg = reinterpret_cast<UStoreMessage *>
              (reqhl->Branch(Slice(keys[idx]),
                              version, Slice(new_branch)));
  EXPECT_EQ(msg->status(), UStoreMessage::SUCCESS);
  delete msg;

  // put on the new branch
  msg = reinterpret_cast<UStoreMessage *>
              (reqhl->Put(Slice(keys[idx]), Slice(values[idx]),
                              Slice(new_branch)));
  Hash branch_version((const byte_t*)msg->put_response_payload()
                                                .new_version().data());
#ifdef MOCK_TEST
  EXPECT_EQ(check_results(PUT_VERSIONS, branch_version.ToBase32()), true);
#else
  DLOG(INFO) << "PUT version: " << branch_version.ToBase32() << std::endl;
#endif
  delete msg;

  // merge
  msg = reinterpret_cast<UStoreMessage *>
              (reqhl->Merge(Slice(keys[idx]), Slice(values[idx]),
                    Slice(new_branch), version));
  Hash merge_version((const byte_t*)msg->merge_response_payload()
                                                  .new_version().data());
#ifdef MOCK_TEST
  EXPECT_EQ(check_results(MERGE_VERSIONS, merge_version.ToBase32()), true);
#else
  DLOG(INFO) << "MERGE version: " << merge_version.ToBase32() << std::endl;
#endif
  delete msg;

  req_idx_[reqhl->id()]++;
  return true;
}

TEST(TestMessage, TestClient1Thread) {
  ustore::SetStderrLogging(ustore::WARNING);
  // launch workers
  ifstream fin(Config::WORKER_FILE);
  string worker_addr;
  vector<WorkerService*> workers;
  while (fin >> worker_addr)
    workers.push_back(new WorkerService(worker_addr, ""));

  vector<thread> worker_threads;
  for (int i = 0; i < workers.size(); i++)
    workers[i]->Init();

  // launch clients
  ifstream fin_client(Config::CLIENTSERVICE_FILE);
  string clientservice_addr;
  fin_client >> clientservice_addr;
  ClientService *client = new ClientService(clientservice_addr, "",
                              new ustore::TestWorkload(1, NREQUESTS));
  client->Init();

  thread client_thread(thread(&ClientService::Start, client));
  for (int i = 0; i < workers.size(); i++)
    worker_threads.push_back(thread(&WorkerService::Start, workers[i]));


  // wait for client to finish, then stop
  client_thread.join();
  client->Stop();

  // then stop workers
  for (WorkerService *ws : workers)
    ws->Stop();
  for (int i = 0; i < worker_threads.size(); i++)
    worker_threads[i].join();
}
