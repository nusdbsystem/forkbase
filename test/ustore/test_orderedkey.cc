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
  ustore::byte_t d1[] = " abc";
  ustore::byte_t d2[] = " efg";
  ustore::byte_t dd2[] = " efg";
  ustore::byte_t d3[] = " aaaa";

  // Set the first byte to be false
  *(reinterpret_cast<bool*>(d1)) = false;
  *(reinterpret_cast<bool*>(d2)) = false;
  *(reinterpret_cast<bool*>(dd2)) = false;
  *(reinterpret_cast<bool*>(d3)) = false;

  ustore::OrderedKey k1(d1, 4);
  ustore::OrderedKey k2(d2, 4);
  ustore::OrderedKey kk2(dd2, 4);
  ustore::OrderedKey k3(d3, 5);

  EXPECT_TRUE(k1 > k3);
  EXPECT_TRUE(k1 < k2);
  EXPECT_TRUE(k3 <= k2);
  EXPECT_TRUE(k2 == kk2);
  EXPECT_TRUE(k2 >= kk2);
}

TEST(OrderedKey, Encode) {
  ustore::byte_t* buffer = new ustore::byte_t[10];
  ustore::OrderedKey k1(10);
  size_t len = k1.encode(buffer);

  EXPECT_EQ(len, sizeof(bool) + sizeof(uint64_t));
  EXPECT_TRUE(*(reinterpret_cast<bool*>(buffer)));
  uint64_t key = *(reinterpret_cast<uint64_t*>(buffer + sizeof(bool)));
  EXPECT_EQ(key, 10);

  delete[] buffer;

  buffer = new ustore::byte_t[10];
  ustore::byte_t d[] = " efg";
  *(reinterpret_cast<bool*>(d)) = false;
  ustore::OrderedKey k2(d, 4);
  len = k2.encode(buffer);

  EXPECT_EQ(len, 4);
  EXPECT_FALSE(*(reinterpret_cast<bool*>(buffer)));
  EXPECT_EQ(0, memcmp(buffer + sizeof(bool), d + 1, 3));

  ustore::OrderedKey kk2(buffer, 4);
  EXPECT_EQ(k2.numBytes(), kk2.numBytes());
  delete[] buffer;
}

