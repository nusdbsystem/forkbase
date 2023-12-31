// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "node/meta_node.h"

TEST(MetaNode, Basic) {
  // Construct first MetaEntry
  uint32_t num_leaves1 = 1;
  uint64_t num_elements1 = 10;

  const char encoded_hash_value1[] = "36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
  ustore::Hash data_hash1 = ustore::Hash::FromBase32(encoded_hash_value1);

  ustore::OrderedKey key1(5);

  size_t encode_len1 = 0;
  const ustore::byte_t* me_bytes1 = ustore::MetaEntry::Encode(
      num_leaves1, num_elements1, data_hash1, key1, &encode_len1);

  // construct second MetaEntry
  uint32_t num_leaves2 = 2;
  uint64_t num_elements2 = 20;
  const char encoded_hash_value2[] = "46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
  ustore::Hash data_hash2 = ustore::Hash::FromBase32(encoded_hash_value2);

  ustore::OrderedKey key2(10);

  size_t encode_len2 = 0;
  const ustore::byte_t* me_bytes2 = ustore::MetaEntry::Encode(
      num_leaves2, num_elements2, data_hash2, key2, &encode_len2);

  // construct third MetaEntry
  uint32_t num_leaves3 = 3;
  uint64_t num_elements3 = 25;
  const char encoded_hash_value3[] = "56UPXMYH36AJI3OKTK6LACBOJ6GVMUPE";
  ustore::Hash data_hash3 = ustore::Hash::FromBase32(encoded_hash_value3);

  ustore::OrderedKey key3(15);

  size_t encode_len3 = 0;
  const ustore::byte_t* me_bytes3 = ustore::MetaEntry::Encode(
      num_leaves3, num_elements3, data_hash3, key3, &encode_len3);

  // Create the first VarSegment on MetaEntry 1 and 2
  size_t seg1_num_bytes = encode_len1 + encode_len2;
  ustore::byte_t* seg_data1 = new ustore::byte_t[seg1_num_bytes];
  std::memcpy(seg_data1, me_bytes1, encode_len1);
  std::memcpy(seg_data1 + encode_len1, me_bytes2, encode_len2);


  ustore::VarSegment seg1(seg_data1, seg1_num_bytes, {0, encode_len1});

  // Create the second VarSegment on MetaEntry 3
  size_t seg2_num_bytes = encode_len3;
  ustore::byte_t* seg_data2 = new ustore::byte_t[seg2_num_bytes];
  std::memcpy(seg_data2, me_bytes3, encode_len3);

  ustore::VarSegment seg2(seg_data2, seg2_num_bytes, {0});

  ustore::ChunkInfo chunk_info =
    ustore::MetaChunker::Instance()->Make({&seg1, &seg2});

  // test on the created chunk
  ustore::MetaNode mnode(&chunk_info.chunk);

  EXPECT_FALSE(mnode.isLeaf());
  EXPECT_EQ(num_leaves1 + num_leaves2 + num_leaves3, mnode.numLeaves());
  EXPECT_EQ(size_t(3), mnode.numEntries());
  EXPECT_EQ(size_t(55), mnode.numElements());
  EXPECT_EQ(size_t(30), mnode.numElementsUntilEntry(2));

  EXPECT_EQ(encode_len1, mnode.len(0));
  EXPECT_EQ(0, std::memcmp(mnode.data(0), me_bytes1, encode_len1));
  EXPECT_EQ(encode_len2, mnode.len(1));

  EXPECT_EQ(0, std::memcmp(mnode.data(1), me_bytes2, encode_len2));
  EXPECT_EQ(encode_len3, mnode.len(2));
  EXPECT_EQ(0, std::memcmp(mnode.data(2), me_bytes3, encode_len3));

  size_t idx;

  EXPECT_EQ(data_hash1, mnode.GetChildHashByIndex(9, &idx));
  ASSERT_EQ(size_t(0), idx);

  EXPECT_EQ(data_hash2, mnode.GetChildHashByIndex(10, &idx));
  ASSERT_EQ(size_t(1), idx);

  EXPECT_EQ(data_hash3, mnode.GetChildHashByEntry(2));

  ustore::OrderedKey ckey1(9);
  EXPECT_EQ(data_hash2, mnode.GetChildHashByKey(ckey1, &idx));
  ASSERT_EQ(size_t(1), idx);

  ustore::OrderedKey ckey2(20);
  EXPECT_TRUE(mnode.GetChildHashByKey(ckey2, &idx).empty());
  ASSERT_EQ(size_t(3), idx);

  // Test on the created metaentry
  auto meta_seg = std::move(chunk_info.meta_seg);
  ASSERT_EQ(size_t(1), meta_seg->numEntries());

  const ustore::byte_t* me_data = meta_seg->entry(0);
  size_t me_num_bytes = meta_seg->entryNumBytes(0);

  ustore::MetaEntry me(me_data);
  EXPECT_EQ(me_num_bytes, me.numBytes());
  EXPECT_EQ(num_leaves1 + num_leaves2 + num_leaves3, me.numLeaves());
  EXPECT_EQ(num_elements1 + num_elements2 + num_elements3, me.numElements());
  EXPECT_EQ(me.orderedKey(), key3);


  // Test on GetSegment
  auto seg = mnode.GetSegment(1, 2);
  EXPECT_EQ(size_t(2), seg->numEntries());

  EXPECT_EQ(encode_len2, seg->entryNumBytes(0));
  EXPECT_EQ(0, std::memcmp(me_bytes2, seg->entry(0), encode_len2));

  EXPECT_EQ(encode_len3, seg->entryNumBytes(1));
  EXPECT_EQ(0, std::memcmp(me_bytes3, seg->entry(1), encode_len3));

  delete[] me_bytes1;
  delete[] me_bytes2;
  delete[] me_bytes3;

  delete[] seg_data1;
  delete[] seg_data2;
}

