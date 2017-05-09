// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "types/uiterator.h"
#include "node/blob_node.h"

#include "store/chunk_loader.h"
#include "store/chunk_store.h"

TEST(UIterator, Basic) {
  const ustore::byte_t c[] = "abcdeg";

  ustore::FixedSegment seg(c, 6, 1);
  ustore::ChunkInfo chunk_info =
    ustore::BlobChunker::Instance()->Make({&seg});

  const ustore::Chunk* chunk = &chunk_info.chunk;
  const ustore::Hash hash = chunk_info.chunk.hash();

  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();
  // Write the constructed chunk to storage
  EXPECT_TRUE(chunk_store->Put(hash, *chunk));

  // This loader must load chunk from the above chunk_store
  ustore::ChunkLoader loader;

  // c[0], c[1], c[3] and c[5] are selected

  ustore::IndexRange range1{0, 2};
  ustore::IndexRange range2{3, 1};
  ustore::IndexRange range3{5, 1};

// Test on next()
  ustore::UIterator it(hash, {range1, range2, range3}, &loader);
  ASSERT_EQ(0, it.index());

  ustore::Slice actual_c0 = it.value();
  ASSERT_EQ(c[0], *actual_c0.data());
  ASSERT_EQ(1, actual_c0.len());

  ASSERT_TRUE(it.next());
  ASSERT_EQ(1, it.index());

  ustore::Slice actual_c1 = it.value();
  ASSERT_EQ(c[1], *actual_c1.data());
  ASSERT_EQ(1, actual_c1.len());

  ASSERT_TRUE(it.next());
  ASSERT_EQ(3, it.index());

  ustore::Slice actual_c3 = it.value();
  ASSERT_EQ(c[3], *actual_c3.data());
  ASSERT_EQ(1, actual_c3.len());

  ASSERT_TRUE(it.next());
  ASSERT_EQ(5, it.index());

  ustore::Slice actual_c5 = it.value();
  ASSERT_EQ(c[5], *actual_c5.data());
  ASSERT_EQ(1, actual_c5.len());

  ASSERT_FALSE(it.next());
  ASSERT_TRUE(it.end());

  // Fail to next further
  ASSERT_FALSE(it.next());
  ASSERT_TRUE(it.end());

// test on previous()
  ASSERT_TRUE(it.previous());
  ASSERT_EQ(5, it.index());

  actual_c5 = it.value();
  ASSERT_EQ(c[5], *actual_c5.data());


  ASSERT_TRUE(it.previous());
  ASSERT_EQ(3, it.index());

  actual_c3 = it.value();
  ASSERT_EQ(c[3], *actual_c3.data());


  ASSERT_TRUE(it.previous());
  ASSERT_EQ(1, it.index());

  actual_c1 = it.value();
  ASSERT_EQ(c[1], *actual_c1.data());


  ASSERT_TRUE(it.previous());
  ASSERT_EQ(0, it.index());

  actual_c0 = it.value();
  ASSERT_EQ(c[0], *actual_c0.data());


  ASSERT_FALSE(it.previous());
  ASSERT_TRUE(it.head());

  // Fail to next further
  ASSERT_FALSE(it.previous());
  ASSERT_TRUE(it.head());


// perform next and previous accross two continuous index ranges
  ASSERT_TRUE(it.next());
  ASSERT_TRUE(it.next());

  ASSERT_TRUE(it.next());
  actual_c3 = it.value();
  ASSERT_EQ(c[3], *actual_c3.data());

  ASSERT_TRUE(it.previous());
  actual_c1 = it.value();
  ASSERT_EQ(c[1], *actual_c1.data());

  ASSERT_TRUE(it.next());
  actual_c3 = it.value();
  ASSERT_EQ(c[3], *actual_c3.data());
}
