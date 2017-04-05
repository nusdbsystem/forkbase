// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include <chrono>
#include <thread>
#include "gtest/gtest.h"
#include "store/lst_store.h"

const int NUMBER = 100000;
const int LEN = 100;
ustore::byte_t raw_data[LEN];
ustore::byte_t hash[NUMBER][ustore::Hash::kByteLength];

TEST(LSTStore, Put) {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  for (int i = 0; i < NUMBER; ++i, (*val)++) {
    ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    cs->Put(chunk.hash(), chunk);
  }
  cs->Sync();
}

TEST(LSTStore, Get) {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  for (int i = 0; i < NUMBER; ++i, (*val)++) {
    ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    std::copy(chunk.hash().value(), chunk.hash().value()
              + ustore::Hash::kByteLength, hash[i]);
  }

  auto tp = std::chrono::steady_clock::now();
  for (int i = 0; i < NUMBER; ++i) {
    // load from stroage
    const ustore::Chunk* c =
      cs->Get(ustore::Hash(reinterpret_cast<ustore::byte_t*>(hash[i])));
    // EXPECT_EQ(c->type(), ustore::ChunkType::kBlob);
    EXPECT_EQ(c->numBytes(), LEN + ::ustore::Chunk::kMetaLength);
    // EXPECT_EQ(c->capacity(), chunk.capacity());
    // EXPECT_EQ(c->hash(), chunk.hash());
    delete c;
  }
  DLOG(INFO) << std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - tp).count();
}
