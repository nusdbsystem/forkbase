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
#include "utils/env.h"
#include "cluster/chunk_service.h"
#include "cluster/remote_chunk_client_service.h"
#include "hash/hash.h"
#include "utils/logging.h"

using ustore::byte_t;
using ustore::ChunkService;
using ustore::RemoteChunkClientService;
using ustore::Config;
using ustore::Slice;
using ustore::Hash;
using ustore::Env;
using ustore::ErrorCode;
using ustore::ChunkDb;
using ustore::Chunk;
using ustore::ChunkType;
using ustore::Value;
using ustore::UType;
using ustore::UCell;
using ustore::StoreInfo;
using std::thread;
using std::vector;
using std::set;
using std::ifstream;
using std::string;

const int kValues = 10;
const string values[] = {"where is the wisdome in knowledge",
                         "where is the knowledge in information",
                         "the brown fox jump over",
                         "be hungry, be foolish, but not judgmental",
                         "four blind men and an elephant",
                         "what would Jesus do",
                         "just a dream within a dream",
                         "where this train terminates",
                         "wings and beaks admire us from uncotested heights",
                         "the dead are using Twitter which amuses as it bites"};
const byte_t raw_data[] = "where is the wisdom in knowledge";

const int kSleepTime = 100000;

bool check_raw_data(const byte_t *data1, const byte_t *data2, int size) {
  for (int i = 0; i<size; i++)
    if (data1[i] != data2[i]) return false;
  return true;
}

Hash put_chunk(const byte_t *raw_data, ChunkDb* chunkdb) {
  Chunk chunk(ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  chunkdb->Put(chunk.hash(), chunk);
  return chunk.hash().Clone();
}

void RequestThread(int starting_idx, int size, ChunkDb* chunkdb) {
  vector<Hash> hashes;
  for (int i = 0; i < size; i++) {
    hashes.push_back(put_chunk(reinterpret_cast<const byte_t*>
                          (values[i+starting_idx].c_str()), chunkdb));
    usleep(kSleepTime);
  }

  for (int i = 0; i < size; i++) {
    Chunk result;
    chunkdb->Get(hashes[i], &result);
    EXPECT_TRUE(check_raw_data(result.data(),
              reinterpret_cast<const byte_t*>
              (values[i+starting_idx].c_str()), result.capacity()));
  }
}

vector<ChunkService*> ChunkServiceInit(string test_file) {
  vector<ChunkService*> servers;
  Env::Instance()->m_config().set_chunk_server_file(test_file);
  
  ifstream fin(Env::Instance()->config().chunk_server_file());
  string server_addr;
  while (fin >> server_addr) {
    servers.push_back(new ChunkService(server_addr, "", false)); 
  }
  return servers;
}

TEST(TestChunkService, ChunkService1Service) {
  vector<ChunkService*> services = ChunkServiceInit("conf/chunk_server_test");
  for (auto cs : services)
    cs->Init();

  vector<thread> service_threads;
  for (auto cs : services)
    service_threads.push_back(thread(&ChunkService::Start, cs));

  // clients
  RemoteChunkClientService clientservice("");
  clientservice.Init();
  thread clientservice_thread(&RemoteChunkClientService::Start, &clientservice);
  usleep(kSleepTime);

  ChunkDb chunkdb = clientservice.CreateChunkDb();
  usleep(kSleepTime);
  
  Hash hash = put_chunk(raw_data, &chunkdb);
  usleep(kSleepTime);
  
  Chunk result;
  EXPECT_EQ(chunkdb.Get(hash, &result), ErrorCode::kOK);
  EXPECT_TRUE(check_raw_data(result.data(), raw_data, result.capacity()));

  clientservice.Stop();
  clientservice_thread.join();
  usleep(kSleepTime);
  for (size_t i = 0; i < services.size(); i++) {
    services[i]->Stop();
    service_threads[i].join();
    delete services[i];
    usleep(kSleepTime);
  }
}

TEST(TestChunkService, ChunkService2Services) {
  vector<ChunkService*> services = ChunkServiceInit("conf/chunk_server_test2");
  for (auto cs : services)
    cs->Init();

  vector<thread> service_threads;
  for (auto cs : services)
    service_threads.push_back(thread(&ChunkService::Start, cs));

  // clients
  RemoteChunkClientService clientservice("");
  clientservice.Init();
  thread clientservice_thread(&RemoteChunkClientService::Start, &clientservice);
  usleep(kSleepTime);

  ChunkDb chunkdb = clientservice.CreateChunkDb();
  usleep(kSleepTime);
  
  vector<Hash> hashes;
  for (int i = 0; i < kValues; i++) {
    hashes.push_back(put_chunk(reinterpret_cast<const byte_t*>(values[i].c_str()), &chunkdb));
    usleep(kSleepTime);
  }

  for (int i = 0; i < kValues; i++) {
    Chunk result;
    chunkdb.Get(hashes[i], &result);
    EXPECT_TRUE(check_raw_data(result.data(),
              reinterpret_cast<const byte_t*>(values[i].c_str()), result.capacity()));
  }
  clientservice.Stop();
  clientservice_thread.join();
  usleep(kSleepTime);
  for (size_t i = 0; i < services.size(); i++) {
    services[i]->Stop();
    service_threads[i].join();
    delete services[i];
    usleep(kSleepTime);
  }
}

TEST(TestChunkService, ChunkService2Services2ClientThreads) {
  vector<ChunkService*> services = ChunkServiceInit("conf/chunk_server_test2");
  for (auto cs : services)
    cs->Init();

  vector<thread> service_threads;
  for (auto cs : services)
    service_threads.push_back(thread(&ChunkService::Start, cs));

  // clients
  RemoteChunkClientService clientservice("");
  clientservice.Init();
  thread clientservice_thread(&RemoteChunkClientService::Start, &clientservice);
  usleep(kSleepTime);

  ChunkDb chunkdb1 = clientservice.CreateChunkDb();
  ChunkDb chunkdb2 = clientservice.CreateChunkDb();
  usleep(kSleepTime);
  
  vector<thread> threads;
  threads.push_back(thread(&RequestThread, 0, kValues/2, &chunkdb1));
  threads.push_back(thread(&RequestThread, 5, kValues/2, &chunkdb2));

  threads[0].join();
  threads[1].join();

  clientservice.Stop();
  clientservice_thread.join();
  usleep(kSleepTime);
  for (size_t i = 0; i < services.size(); i++) {
    services[i]->Stop();
    service_threads[i].join();
    delete services[i];
    usleep(kSleepTime);
  }
}

