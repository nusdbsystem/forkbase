// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "hash/hash.h"
#include "node/node.h"
#include "node/orderedkey.h"
#include "utils/debug.h"
#include "utils/logging.h"

TEST(MetaEntry, EncodeDecode) {
  uint32_t num_leaves = 5;
  uint64_t num_elements = 10;

  ustore::Hash data_hash;
  const char encoded_hash_value[] = "26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
  data_hash.FromString(encoded_hash_value);

  // construct order key from above hash
  // padding a false bool in front of hash value to indicate not by value
  size_t key_num_bytes = sizeof(bool) + ustore::Hash::kByteLength;
  ustore::byte_t* key_value = new ustore::byte_t[key_num_bytes];
  *(reinterpret_cast<bool*>(key_value)) = false;
  std::memcpy(key_value + sizeof(bool), data_hash.value(),
              ustore::Hash::kByteLength);
  ustore::OrderedKey key(key_value, key_num_bytes);

  size_t encode_len = 0;
  const ustore::byte_t* me_bytes = ustore::MetaEntry::Encode(
      num_leaves, num_elements, data_hash, key, &encode_len);
  EXPECT_EQ(encode_len, 2 * sizeof(uint32_t) + sizeof(uint64_t) +
                            ustore::Hash::kByteLength + key_num_bytes);

  const ustore::MetaEntry me(me_bytes);

  EXPECT_EQ(me.numBytes(), encode_len);
  EXPECT_EQ(me.numLeaves(), num_leaves);
  EXPECT_EQ(me.numElements(), num_elements);
  EXPECT_EQ(me.targetHash(), data_hash);
  EXPECT_EQ(me.orderedKey(), key);

  delete[] key_value;
  delete[] me_bytes;
}

TEST(MetaNode, Basic) {
  // Construct first MetaEntry
  uint32_t num_leaves1 = 1;
  uint64_t num_elements1 = 10;

  ustore::Hash data_hash1;
  const char encoded_hash_value1[] = "36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
  data_hash1.FromString(encoded_hash_value1);

  ustore::OrderedKey key1(5);

  size_t encode_len1 = 0;
  const ustore::byte_t* me_bytes1 = ustore::MetaEntry::Encode(
      num_leaves1, num_elements1, data_hash1, key1, &encode_len1);
  uint32_t num_leaves2 = 2;
  uint64_t num_elements2 = 20;

  // construct second MetaEntry
  ustore::Hash data_hash2;
  const char encoded_hash_value2[] = "46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
  data_hash2.FromString(encoded_hash_value2);

  ustore::OrderedKey key2(10);

  size_t encode_len2 = 0;
  const ustore::byte_t* me_bytes2 = ustore::MetaEntry::Encode(
      num_leaves2, num_elements2, data_hash2, key2, &encode_len2);

  std::vector<const ustore::byte_t*> entry_data = {me_bytes1, me_bytes2};
  std::vector<size_t> entry_num_bytes = {encode_len1, encode_len2};

  ustore::ChunkInfo chunk_info = ustore::MetaNode::MakeChunk(entry_data,
                                                             entry_num_bytes);

  // test on the created chunk
  const ustore::Chunk* chunk = chunk_info.first;
  ustore::MetaNode mnode(chunk);

  EXPECT_FALSE(mnode.isLeaf());
  EXPECT_EQ(mnode.numEntries(), 2);
  EXPECT_EQ(mnode.numElements(), 30);
  EXPECT_EQ(mnode.numElementsUntilEntry(1), 10);
  EXPECT_EQ(mnode.len(0), encode_len1);
  EXPECT_EQ(0, std::memcmp(mnode.data(0), me_bytes1, encode_len1));
  EXPECT_EQ(mnode.len(1), encode_len2);
  EXPECT_EQ(0, std::memcmp(mnode.data(1), me_bytes2, encode_len2));

  ustore::OrderedKey key3(9);
  size_t idx;

  EXPECT_EQ(mnode.GetChildHashByIndex(9, &idx), data_hash1);
  ASSERT_EQ(idx, 0);

  EXPECT_EQ(mnode.GetChildHashByIndex(10, &idx), data_hash2);
  ASSERT_EQ(idx, 1);

  EXPECT_EQ(mnode.GetChildHashByEntry(0), data_hash1);

  EXPECT_EQ(mnode.GetChildHashByKey(key3, &idx), data_hash2);
  ASSERT_EQ(idx, 1);

  ustore::OrderedKey key4(20);
  EXPECT_TRUE(mnode.GetChildHashByKey(key4, &idx).empty());
  ASSERT_EQ(idx, 2);
  delete chunk;

  // Test on the created metaentry
  const ustore::byte_t* me_data = chunk_info.second.first;
  size_t me_num_bytes = chunk_info.second.second;

  ustore::MetaEntry me(me_data);
  EXPECT_EQ(me.numBytes(), me_num_bytes);
  EXPECT_EQ(me.numLeaves(), num_leaves1 + num_leaves2);
  EXPECT_EQ(me.numElements(), num_elements1 + num_elements2);
  EXPECT_EQ(me.orderedKey(), key2);

  delete[] me_data;
}
