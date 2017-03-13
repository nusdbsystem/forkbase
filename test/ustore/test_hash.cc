// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include <sstream>
#include "gtest/gtest.h"
#include "hash/hash.h"

const ustore::byte_t raw_str[] = "The quick brown fox jumps over the lazy dog";
const char base32_encoded[] = "26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
const char hash_hex_str[] = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e4";

TEST(Hash, FromString) {
  ustore::Hash h;
  h.FromString(base32_encoded);
  EXPECT_EQ(base32_encoded, h.ToString());
}

TEST(Hash, HashTest) {
  ustore::Hash h;
  h.Compute(raw_str, 43);
  EXPECT_EQ(h.ToString(), base32_encoded);
  std::ostringstream stm;
  for (size_t i = 0; i < ustore::HASH_BYTE_LEN; ++i) {
    stm << std::hex << std::setfill('0') << std::setw(2)
        << uint32_t(*(h.value() + i));
  }
  EXPECT_EQ(hash_hex_str, stm.str());
}
