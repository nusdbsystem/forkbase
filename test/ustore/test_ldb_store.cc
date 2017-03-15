// Copyright (c) 2017 The Ustore Authors.
#ifdef USE_LEVELDB
#include <algorithm>
#include <cstring>
#include <string>
#include "gtest/gtest.h"
#include "utils/singleton.h"
#include "store/ldb_store.h"

static ustore::Singleton<ustore::LDBStore> ldb;
const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(LDBStore, PutChunk) {
  ustore::Chunk chunk(ustore::kBlobChunk, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  EXPECT_TRUE(ldb.Instance()->Put(chunk.hash(), chunk));
}

TEST(LDBStore, GetChunk) {
  ustore::Chunk chunk(ustore::kBlobChunk, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  const ustore::Chunk* c = ldb.Instance()->Get(chunk.hash());
  EXPECT_EQ(c->forceHash(), chunk.hash());
  EXPECT_EQ(c->type(), ustore::kBlobChunk);
  EXPECT_EQ(c->numBytes(), chunk.numBytes());
  EXPECT_EQ(c->capacity(), chunk.capacity());
  delete c;
}
#endif // USE_LEVELDB
