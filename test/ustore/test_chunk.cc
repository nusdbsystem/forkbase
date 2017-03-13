// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include "gtest/gtest.h"
#include "chunk/chunk.h"
#include "hash/hash.h"

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(Chunk, CreateNewChunk) {
  ustore::Chunk chunk(ustore::kBlobChunk, sizeof(raw_data));
  memcpy(chunk.m_data(), raw_data, sizeof(raw_data));
  // check chunk type
  EXPECT_EQ(ustore::kBlobChunk, chunk.type());
  // check chunk size
  size_t expected_size = sizeof(raw_data) + ustore::Chunk::META_SIZE;
  EXPECT_EQ(expected_size, chunk.numBytes());
  // check capacity
  EXPECT_EQ(sizeof(raw_data), chunk.capacity());
  // check content
  EXPECT_EQ(0, memcmp(raw_data, chunk.data(), sizeof(raw_data)));
  // check chunk hash
  ustore::Hash h;
  h.Compute(chunk.head(), chunk.numBytes());
  EXPECT_EQ(h.ToString(), chunk.hash().ToString());
}

TEST(Chunk, LoadChunk) {
  ustore::Chunk origin(ustore::kBlobChunk, sizeof(raw_data));
  memcpy(origin.m_data(), raw_data, sizeof(raw_data));
  ustore::byte_t* buffer = new ustore::byte_t[origin.numBytes()];
  memcpy(buffer, origin.head(), origin.numBytes());
  ustore::Chunk chunk(buffer);
  // check chunk type
  EXPECT_EQ(ustore::kBlobChunk, chunk.type());
  // check chunk size
  size_t expected_size = sizeof(raw_data) + ustore::Chunk::META_SIZE;
  EXPECT_EQ(expected_size, chunk.numBytes());
  // check capacity
  EXPECT_EQ(sizeof(raw_data), chunk.capacity());
  // check content
  EXPECT_EQ(0, memcmp(raw_data, chunk.data(), sizeof(raw_data)));
  // check chunk hash
  ustore::Hash h;
  h.Compute(chunk.head(), chunk.numBytes());
  EXPECT_EQ(h.ToString(), chunk.hash().ToString());
}
