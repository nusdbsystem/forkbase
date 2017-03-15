// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include "gtest/gtest.h"
#include "node/chunk_loader.h"
#include "utils/singleton.h"
#ifdef USE_LEVELDB
#include "store/ldb_store.h"
#endif  // USE_LEVELDB

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

#ifdef USE_LEVELDB
static ustore::Singleton<ustore::LDBStore> ldb;
#endif  // USE_LEVELDB

TEST(ChunkLoader, GetChunk) {
  ustore::Chunk chunk(ustore::kBlobChunk, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  #ifdef USE_LEVELDB
  ustore::ChunkStore* cs = ldb.Instance();
  #endif  // USE_LEVELDB
  EXPECT_TRUE(cs->Put(chunk.hash(), chunk));
  ustore::ChunkLoader cl(cs);
  // load from stroage
  const ustore::Chunk* c = cl.Load(chunk.hash());
  EXPECT_EQ(c->hash(), chunk.hash());
  EXPECT_EQ(c->type(), ustore::kBlobChunk);
  EXPECT_EQ(c->numBytes(), chunk.numBytes());
  EXPECT_EQ(c->capacity(), chunk.capacity());
  // load from cache
  c = cl.Load(chunk.hash());
  EXPECT_EQ(c->hash(), chunk.hash());
  EXPECT_EQ(c->type(), ustore::kBlobChunk);
  EXPECT_EQ(c->numBytes(), chunk.numBytes());
  EXPECT_EQ(c->capacity(), chunk.capacity());
}
