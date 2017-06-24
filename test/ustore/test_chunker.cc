// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "chunk/chunker.h"

TEST(FixedChunker, Basic) {
  const ustore::byte_t data[] = "abcde";

  ustore::FixedSegment seg1(data, 5, 1);

  EXPECT_EQ(*(data + 2), *(seg1.entry(2)));
  EXPECT_EQ(size_t(1), seg1.entryNumBytes(1));
  EXPECT_EQ(size_t(5), seg1.numBytes());
  EXPECT_FALSE(seg1.empty());
  EXPECT_EQ(data, seg1.data());

  ustore::byte_t* buffer = new ustore::byte_t[seg1.numBytes()];
  seg1.AppendForChunk(buffer);
  EXPECT_EQ(0, memcmp(buffer, seg1.data(), seg1.numBytes()));
  delete[] buffer;

  ustore::FixedSegment seg2(data, 2);
  EXPECT_TRUE(seg2.empty());
  ASSERT_EQ(size_t(1), seg2.prolong(2));
  EXPECT_EQ(size_t(1), seg2.numEntries());
  EXPECT_EQ(size_t(2), seg2.numBytes());
}

TEST(FixedChunker, Split) {
  const ustore::byte_t data[] = "abcde";
  ustore::FixedSegment seg1(data, 5, 1);

  // Split at start
  auto splits = seg1.Split(0);
  EXPECT_TRUE(splits.first->empty());
  EXPECT_EQ(size_t(5), splits.second->numBytes());
  EXPECT_EQ(size_t(5), splits.second->numEntries());
  EXPECT_EQ(0, memcmp(data, splits.second->data(), 5));

  // Split at Middle
  splits = seg1.Split(2);
  EXPECT_EQ(size_t(2), splits.first->numBytes());
  EXPECT_EQ(size_t(2), splits.first->numEntries());
  EXPECT_EQ(0, memcmp(data, splits.first->data(), 2));

  EXPECT_EQ(size_t(3), splits.second->numBytes());
  EXPECT_EQ(size_t(3), splits.second->numEntries());
  EXPECT_EQ(0, memcmp(data + 2, splits.second->data(), 3));

  // Split at End
  splits = seg1.Split(5);
  EXPECT_EQ(size_t(5), splits.first->numBytes());
  EXPECT_EQ(size_t(5), splits.first->numEntries());
  EXPECT_EQ(0, memcmp(data, splits.first->data(), 5));
  EXPECT_TRUE(splits.second->empty());

}

TEST(VarChunker, Basic) {
  const ustore::byte_t data[] = "abcde";

  ustore::VarSegment seg1(data, 5, {0, 2});

  EXPECT_EQ(*(data + 2), *(seg1.entry(1)));
  EXPECT_EQ(size_t(2), seg1.entryNumBytes(0));
  EXPECT_EQ(size_t(3), seg1.entryNumBytes(1));
  EXPECT_EQ(size_t(5), seg1.numBytes());
  EXPECT_FALSE(seg1.empty());
  EXPECT_EQ(data, seg1.data());

  ustore::byte_t* buffer = new ustore::byte_t[seg1.numBytes()];
  seg1.AppendForChunk(buffer);
  EXPECT_EQ(0, memcmp(buffer, seg1.data(), seg1.numBytes()));
  delete[] buffer;

  ustore::VarSegment seg2(data);
  EXPECT_TRUE(seg2.empty());

  ASSERT_EQ(size_t(1), seg2.prolong(2));
  EXPECT_EQ(size_t(1), seg2.numEntries());
  EXPECT_EQ(size_t(2), seg2.entryNumBytes(0));
  EXPECT_EQ(size_t(2), seg2.numBytes());

  ASSERT_EQ(size_t(2), seg2.prolong(1));
  EXPECT_EQ(size_t(2), seg2.numEntries());
  EXPECT_EQ(size_t(1), seg2.entryNumBytes(1));
  EXPECT_EQ(size_t(3), seg2.numBytes());
}

TEST(VarChunker, Split) {
  const ustore::byte_t data[] = "abcde";
  ustore::VarSegment seg1(data, 5, {0, 1, 3});

  // Split at start
  auto splits = seg1.Split(0);
  EXPECT_TRUE(splits.first->empty());
  EXPECT_EQ(size_t(5), splits.second->numBytes());
  EXPECT_EQ(size_t(3), splits.second->numEntries());
  EXPECT_EQ(0, memcmp(data, splits.second->data(), 5));

  // Split at Middle
  splits = seg1.Split(2);
  EXPECT_EQ(size_t(3), splits.first->numBytes());
  EXPECT_EQ(size_t(2), splits.first->numEntries());
  EXPECT_EQ(0, memcmp(data, splits.first->data(), 3));

  EXPECT_EQ(size_t(2), splits.second->numBytes());
  EXPECT_EQ(size_t(1), splits.second->numEntries());
  EXPECT_EQ(0, memcmp(data + 3, splits.second->data(), 2));

  // Split at End
  splits = seg1.Split(3);
  EXPECT_EQ(size_t(5), splits.first->numBytes());
  EXPECT_EQ(size_t(3), splits.first->numEntries());
  EXPECT_EQ(0, memcmp(data, splits.first->data(), 5));
  EXPECT_TRUE(splits.second->empty());

}
