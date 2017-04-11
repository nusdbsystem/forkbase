// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include "gtest/gtest.h"
#include "store/chunk_loader.h"
#include "store/chunk_store.h"

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(ChunkLoader, GetChunk) {
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  ustore::ChunkStore* cs = ustore::store::GetChunkStore();
  EXPECT_TRUE(cs->Put(chunk.hash(), chunk));
  ustore::ChunkLoader cl;
  // load from stroage
  const ustore::Chunk* c = cl.Load(chunk.hash());
  EXPECT_EQ(c->hash(), chunk.hash());
  EXPECT_EQ(c->type(), ustore::ChunkType::kBlob);
  EXPECT_EQ(c->numBytes(), chunk.numBytes());
  EXPECT_EQ(c->capacity(), chunk.capacity());
  // load from cache
  c = cl.Load(chunk.hash());
  EXPECT_EQ(c->hash(), chunk.hash());
  EXPECT_EQ(c->type(), ustore::ChunkType::kBlob);
  EXPECT_EQ(c->numBytes(), chunk.numBytes());
  EXPECT_EQ(c->capacity(), chunk.capacity());
}
