// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include <sstream>
#include <utility>
#include "gtest/gtest.h"
#include "hash/hash.h"

const ustore::byte_t raw_str[] = "The quick brown fox jumps over the lazy dog";
const char base32_encoded[] = "26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
const char hash_hex_str[] = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e4";

TEST(Hash, FromBase32) {
  ustore::Hash h = ustore::Hash::FromBase32(base32_encoded);
  EXPECT_EQ(base32_encoded, h.ToBase32());
}

TEST(Hash, ComputeHash) {
  ustore::Hash h= ustore::Hash::ComputeFrom(raw_str, 43);
  EXPECT_EQ(h.ToBase32(), base32_encoded);
  std::ostringstream stm;
  for (size_t i = 0; i < ustore::Hash::kByteLength; ++i) {
    stm << std::hex << std::setfill('0') << std::setw(2)
        << uint32_t(*(h.value() + i));
  }
  EXPECT_EQ(hash_hex_str, stm.str());
}

TEST(Hash, IsEmpty) {
  ustore::Hash h;
  EXPECT_TRUE(h.empty());
  h = ustore::Hash::kNull;
  EXPECT_FALSE(h.empty());
  EXPECT_EQ(h, ustore::Hash::kNull);
}

TEST(Hash, Clone) {
  ustore::Hash h = ustore::Hash::kNull.Clone();
  EXPECT_FALSE(h.empty());
  EXPECT_EQ(h, ustore::Hash::kNull);
}

TEST(Hash, Movable) {
  ustore::Hash old = ustore::Hash::FromBase32(base32_encoded);
  EXPECT_TRUE(old.own());
  ustore::Hash h = std::move(old);
  EXPECT_FALSE(old.own());
  EXPECT_TRUE(h.own());
  EXPECT_EQ(base32_encoded, h.ToBase32());
}
