// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include "gtest/gtest.h"
#include "chunk/chunk.h"
#include "hash/hash.h"

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(Chunk, CreateNewChunk) {
  const ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::memcpy(chunk.m_data(), raw_data, sizeof(raw_data));
  // check chunk type
  EXPECT_EQ(ustore::ChunkType::kBlob, chunk.type());
  // check chunk size
  size_t expected_size = sizeof(raw_data) + ustore::Chunk::kMetaLength;
  EXPECT_EQ(expected_size, chunk.numBytes());
  // check capacity
  EXPECT_EQ(sizeof(raw_data), chunk.capacity());
  // check content
  EXPECT_EQ(0, std::memcmp(raw_data, chunk.data(), sizeof(raw_data)));
  // check chunk hash
  ustore::Hash h = ustore::Hash::ComputeFrom(chunk.head(), chunk.numBytes());
  EXPECT_EQ(h.ToBase32(), chunk.hash().ToBase32());
}

TEST(Chunk, LoadChunk) {
  const ustore::Chunk origin(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::memcpy(origin.m_data(), raw_data, sizeof(raw_data));
  ustore::byte_t* buffer = new ustore::byte_t[origin.numBytes()];
  std::memcpy(buffer, origin.head(), origin.numBytes());
  const ustore::Chunk chunk(buffer);
  // check chunk type
  EXPECT_EQ(ustore::ChunkType::kBlob, chunk.type());
  // check chunk size
  size_t expected_size = sizeof(raw_data) + ustore::Chunk::kMetaLength;
  EXPECT_EQ(expected_size, chunk.numBytes());
  // check capacity
  EXPECT_EQ(sizeof(raw_data), chunk.capacity());
  // check content
  EXPECT_EQ(0, std::memcmp(raw_data, chunk.data(), sizeof(raw_data)));
  // check chunk hash
  ustore::Hash h = ustore::Hash::ComputeFrom(chunk.head(), chunk.numBytes());
  EXPECT_EQ(h.ToBase32(), chunk.hash().ToBase32());
}
