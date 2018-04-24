// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "node/node.h"

TEST(MetaEntry, EncodeDecode) {
  uint32_t num_leaves = 5;
  uint64_t num_elements = 10;

  const char encoded_hash_value[] = "26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
  ustore::Hash data_hash = ustore::Hash::FromBase32(encoded_hash_value);

  // construct order key from above hash
  // padding a false bool in front of hash value to indicate not by value
  size_t key_num_bytes = ustore::Hash::kByteLength;
  ustore::byte_t* key_value = new ustore::byte_t[key_num_bytes];
  std::memcpy(key_value, data_hash.value(),
              ustore::Hash::kByteLength);
  ustore::OrderedKey key(false, key_value, key_num_bytes);

  size_t encode_len = 0;
  const ustore::byte_t* me_bytes = ustore::MetaEntry::Encode(
      num_leaves, num_elements, data_hash, key, &encode_len);

  EXPECT_EQ(encode_len, 2 * sizeof(uint32_t)
                          + sizeof(uint64_t)
                          + ustore::Hash::kByteLength
                          + key_num_bytes
                          + sizeof(bool));

  const ustore::MetaEntry me(me_bytes);

  EXPECT_EQ(encode_len, me.numBytes());
  EXPECT_EQ(num_leaves, me.numLeaves());
  EXPECT_EQ(num_elements, me.numElements());
  EXPECT_EQ(data_hash, me.targetHash());
  EXPECT_EQ(key, me.orderedKey());

  delete[] key_value;
  delete[] me_bytes;
}
