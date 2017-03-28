// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "chunk/chunk.h"
#include "node/blob_node.h"
#include "gtest/gtest.h"

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(BlobNode, Basic) {
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::memcpy(chunk.m_data(), raw_data, sizeof(raw_data));

  ustore::BlobNode bnode(&chunk);

  EXPECT_TRUE(bnode.isLeaf());
  EXPECT_EQ(bnode.numEntries(), sizeof(raw_data));
  EXPECT_EQ(bnode.numElements(), sizeof(raw_data));
  EXPECT_EQ(bnode.GetLength(10, 25), 15);
  EXPECT_EQ(bnode.len(10), 1);
  EXPECT_EQ(0, std::memcmp(bnode.data(15), raw_data + 15, 1));

  ustore::byte_t* buffer = new ustore::byte_t[15];
  // Check normal copy
  ASSERT_EQ(bnode.Copy(10, 15, buffer), 15);
  ASSERT_EQ(0, std::memcmp(buffer, raw_data + 10, 15));

  // Check num_bytes to copy exceed the end of chunk
  size_t start_idx = sizeof(raw_data) - 5;
  ASSERT_EQ(bnode.Copy(start_idx, 10, buffer), 5);
  ASSERT_EQ(0, std::memcmp(buffer, raw_data + start_idx, 5));
  delete[] buffer;
}

TEST(BlobNode, MakeChunk) {
  const ustore::byte_t r1[] = "aa";
  const ustore::byte_t r2[] = "bbb";

  ustore::ChunkInfo chunk_info = ustore::BlobNode
                                       ::MakeChunk({r1, r2},
                                                   {2, 3});

  const ustore::Chunk* chunk = chunk_info.first;

  const ustore::byte_t r[] = "aabbb";
  EXPECT_EQ(0, memcmp(chunk->data(), r, 5));

  const ustore::byte_t* me_data = chunk_info.second.first;
  ustore::MetaEntry me(me_data);
  EXPECT_EQ(me.targetHash(), chunk->hash());
  EXPECT_EQ(me.numBytes(), chunk_info.second.second);
  EXPECT_EQ(me.numElements(), 5);
  EXPECT_EQ(me.numLeaves(), 1);

  delete[] me_data;
  delete chunk;
}
