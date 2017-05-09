// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "chunk/chunk.h"
#include "node/blob_node.h"

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(BlobNode, Basic) {
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::memcpy(chunk.m_data(), raw_data, sizeof(raw_data));

  ustore::BlobNode bnode(&chunk);

  EXPECT_TRUE(bnode.isLeaf());
  EXPECT_EQ(sizeof(raw_data), bnode.numEntries());
  EXPECT_EQ(sizeof(raw_data), bnode.numElements());
  EXPECT_EQ(15, bnode.GetLength(10, 25));
  EXPECT_EQ(1, bnode.len(10));
  EXPECT_EQ(0, std::memcmp(bnode.data(15), raw_data + 15, 1));

  ustore::byte_t* buffer = new ustore::byte_t[15];
  // Check normal copy
  ASSERT_EQ(15, bnode.Copy(10, 15, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, raw_data + 10, 15));

  // Check num_bytes to copy exceed the end of chunk
  size_t start_idx = sizeof(raw_data) - 5;
  ASSERT_EQ(5, bnode.Copy(start_idx, 10, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, raw_data + start_idx, 5));
  delete[] buffer;
}

TEST(BlobChunker, Basic) {
  const ustore::byte_t r1[] = "aa";
  const ustore::byte_t r2[] = "bbb";

  ustore::FixedSegment seg1(r1, 2, 1);
  ustore::FixedSegment seg2(r2, 3, 1);
  ustore::ChunkInfo chunk_info =
    ustore::BlobChunker::Instance()->Make({&seg1, &seg2});

  const ustore::byte_t r[] = "aabbb";
  EXPECT_EQ(0, memcmp(chunk_info.chunk.data(), r, 5));

  ASSERT_EQ(1, chunk_info.meta_seg->numEntries());
  const ustore::byte_t* me_data = chunk_info.meta_seg->entry(0);
  ustore::MetaEntry me(me_data);

  EXPECT_EQ(chunk_info.chunk.hash(), me.targetHash());
  EXPECT_EQ(chunk_info.meta_seg->numBytes(), me.numBytes());
  EXPECT_EQ(5, me.numElements());
  EXPECT_EQ(1, me.numLeaves());
}
