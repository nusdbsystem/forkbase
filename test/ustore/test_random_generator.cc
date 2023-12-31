// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"
#include "benchmark/bench_utils.h"

using namespace ustore;
using Vec = std::vector<std::string>;

size_t test_size = 10;
size_t str_length = 32;
size_t blob_size = 4096;

TEST(RandomGenerator, RG_SequentialNumString) {
  RandomGenerator rg;
  Vec keys = rg.SequentialNumString("", test_size);
  EXPECT_EQ(test_size, keys.capacity());
  EXPECT_EQ(size_t(1), keys[1].length());
}

TEST(RandomGenerator, RG_NFixedString) {
  RandomGenerator rg;
  Vec keys = rg.NFixedString(test_size, str_length);
  EXPECT_EQ(test_size, keys.capacity());
  EXPECT_EQ(str_length, keys[1].length());
}

TEST(RandomGenerator, RG_NRandomString) {
  RandomGenerator rg;
  Vec keys = rg.NRandomString(test_size, str_length);
  EXPECT_EQ(test_size, keys.capacity());
  EXPECT_GE(str_length, keys[1].length());
}

TEST(RandomGenerator, RG_FixedString) {
  RandomGenerator rg;
  std::string str = rg.FixedString(str_length);
  EXPECT_EQ(str_length, str.length());
}

TEST(RandomGenerator, RG_RandomString) {
  RandomGenerator rg;
  std::string str = rg.RandomString(str_length);
  EXPECT_GE(str_length, str.length());
}
