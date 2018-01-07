// Copyright (c) 2017 The Ustore Authors.

#if defined(USE_ROCKSDB)

#include "gtest/gtest.h"

#include "store/rocks_store.h"

static ustore::RocksStore* rs = ustore::RocksStore::Instance();
const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(RocksStore, PutChunk) {
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  EXPECT_TRUE(rs->Put(chunk.hash(), chunk));
}

TEST(RocksStore, GetChunk) {
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  ustore::Chunk c = rs->Get(chunk.hash());
  EXPECT_EQ(c.forceHash(), chunk.hash());
  EXPECT_EQ(c.type(), ustore::ChunkType::kBlob);
  EXPECT_EQ(c.numBytes(), chunk.numBytes());
  EXPECT_EQ(c.capacity(), chunk.capacity());
}

#endif  // USE_ROCKSDB
