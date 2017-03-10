// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include "gtest/gtest.h"
#include "hash/sha2.h"

const ustore::byte_t raw_str[] = "The quick brown fox jumps over the lazy dog";
const std::string base32_encoded = "26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE";
const std::string hash_hex_str =
    "d7a8fbb307d7809469ca9abcb0082e4f8d5651e4";  // no use for this moment

TEST(SHA2, FromString) {
  ustore::SHA256 h;
  h.FromString(base32_encoded);
  EXPECT_EQ(h.ToString(), base32_encoded);
}

TEST(SHA2, HashTest) {
  ustore::SHA256 h;
  h.Compute(raw_str, 43);
  EXPECT_EQ(h.ToString(), base32_encoded);
}
