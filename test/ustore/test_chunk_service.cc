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
#include "cluster/chunk_client_service.h"
#include "hash/hash.h"
#include "utils/logging.h"

using ustore::byte_t;
using ustore::ChunkService;
using ustore::ChunkClientService;
using ustore::Config;
using ustore::Slice;
using ustore::Hash;
using ustore::Env;
using ustore::ErrorCode;
using ustore::ChunkClient;
using ustore::Chunk;
using ustore::ChunkType;
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

bool check_raw_data(const byte_t *data1, const byte_t *data2, int size) {
  for (int i = 0; i<size; i++)
    if (data1[i] != data2[i]) return false;
  return true;
}

Hash put_chunk(const byte_t *raw_data, ChunkClient* chunkdb) {
  Chunk chunk(ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  chunkdb->Put(chunk.hash(), chunk);
  return chunk.hash().Clone();
}

void RequestThread(int starting_idx, int size, ChunkClient* chunkdb) {
  vector<Hash> hashes;
  for (int i = 0; i < size; i++) {
    hashes.push_back(put_chunk(reinterpret_cast<const byte_t*>
                          (values[i+starting_idx].c_str()), chunkdb));
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
  Env::Instance()->m_config().set_worker_file(test_file);
  ifstream fin(Env::Instance()->config().worker_file());
  CHECK(fin) << "Failed to open: " << test_file;
  string server_addr;
  while (fin >> server_addr) {
    servers.push_back(new ChunkService(server_addr));
  }
  return servers;
}

TEST(TestChunkService, ChunkService1Service) {
  vector<ChunkService*> services = ChunkServiceInit("conf/test_single_worker.lst");
  for (auto& cs : services) cs->Run();

  // clients
  ChunkClientService clientservice;
  clientservice.Run();
  ChunkClient chunkdb = clientservice.CreateChunkClient();

  Hash hash = put_chunk(raw_data, &chunkdb);

  Chunk result;
  EXPECT_EQ(chunkdb.Get(hash, &result), ErrorCode::kOK);
  EXPECT_TRUE(check_raw_data(result.data(), raw_data, result.capacity()));

  clientservice.Stop();
  for (auto& cs : services) delete cs;
}

TEST(TestChunkService, ChunkService2Services) {
  vector<ChunkService*> services = ChunkServiceInit("conf/test_multi_worker.lst");
  for (auto& cs : services) cs->Run();

  // clients
  ChunkClientService clientservice;
  clientservice.Run();
  ChunkClient chunkdb = clientservice.CreateChunkClient();

  vector<Hash> hashes;
  for (int i = 0; i < kValues; i++) {
    hashes.push_back(put_chunk(reinterpret_cast<const byte_t*>(values[i].c_str()), &chunkdb));
  }

  for (int i = 0; i < kValues; i++) {
    Chunk result;
    chunkdb.Get(hashes[i], &result);
    EXPECT_TRUE(check_raw_data(result.data(),
              reinterpret_cast<const byte_t*>(values[i].c_str()), result.capacity()));
  }
  clientservice.Stop();
  for (auto& cs : services) delete cs;
}

TEST(TestChunkService, ChunkService2Services2ClientThreads) {
  vector<ChunkService*> services = ChunkServiceInit("conf/test_multi_worker.lst");
  for (auto& cs : services) cs->Run();

  // clients
  ChunkClientService clientservice;
  clientservice.Run();

  ChunkClient chunkdb1 = clientservice.CreateChunkClient();
  ChunkClient chunkdb2 = clientservice.CreateChunkClient();

  vector<thread> threads;
  threads.push_back(thread(&RequestThread, 0, kValues/2, &chunkdb1));
  threads.push_back(thread(&RequestThread, 5, kValues/2, &chunkdb2));

  threads[0].join();
  threads[1].join();

  clientservice.Stop();
  for (auto& cs : services) delete cs;
}

