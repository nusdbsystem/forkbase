// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "node/orderedkey.h"

TEST(OrderedKey, CompareValueKey) {
  ustore::OrderedKey k1(10);
  ustore::OrderedKey k2(5);
  ustore::OrderedKey k3(15);
  ustore::OrderedKey kk2(5);

  EXPECT_TRUE(k1 > k2);
  EXPECT_TRUE(k1 < k3);
  EXPECT_TRUE(k1 <= k3);
  EXPECT_TRUE(k2 == kk2);
  EXPECT_TRUE(k2 >= kk2);
}

TEST(OrderedKey, CompareByte) {
  ustore::byte_t d1[] = "abc";
  ustore::byte_t d2[] = "efg";
  ustore::byte_t dd2[] = "efg";
  ustore::byte_t d3[] = "aaaa";

  bool by_value = false;
  ustore::OrderedKey k1(by_value, d1, 3);
  ustore::OrderedKey k2(by_value, d2, 3);
  ustore::OrderedKey kk2(by_value, dd2, 3);
  ustore::OrderedKey k3(by_value, d3, 4);

  EXPECT_TRUE(k1 > k3);
  EXPECT_TRUE(k1 < k2);
  EXPECT_TRUE(k3 <= k2);
  EXPECT_TRUE(k2 == kk2);
  EXPECT_TRUE(k2 >= kk2);
}

TEST(OrderedKey, Encode) {
  ustore::byte_t* buffer = new ustore::byte_t[10];
  ustore::OrderedKey k1(10);
  size_t len = k1.Encode(buffer);

  EXPECT_EQ(len, sizeof(uint64_t));
  uint64_t key = *(reinterpret_cast<uint64_t*>(buffer));
  EXPECT_EQ(size_t(10), key);

  delete[] buffer;

  buffer = new ustore::byte_t[10];
  ustore::byte_t d[] = "efg";
  ustore::OrderedKey k2(false, d, 3);
  len = k2.Encode(buffer);

  EXPECT_EQ(size_t(3), len);
  EXPECT_EQ(0, std::memcmp(buffer, d, 3));

  ustore::OrderedKey kk2(false, buffer, 3);
  EXPECT_EQ(k2.numBytes(), kk2.numBytes());
  delete[] buffer;
}

