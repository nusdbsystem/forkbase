// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"
#include "benchmark/random_generator.h"

using namespace ustore;
using Vec = std::vector<std::string>;

TEST(RandomGenerator, RG_SequentialNumString) {
  RandomGenerator rg;
  Vec keys;
  rg.SequentialNumString(10, &keys);
  EXPECT_EQ(10, keys.capacity());
  EXPECT_EQ(1, keys[1].length());
}

TEST(RandomGenerator, RG_NFixedString) {
  RandomGenerator rg;
  Vec keys;
  rg.NFixedString(10, 32, &keys);
  EXPECT_EQ(10, keys.capacity());
  EXPECT_EQ(32, keys[1].length());
}

TEST(RandomGenerator, RG_NRandomString) {
  RandomGenerator rg;
  Vec keys;
  rg.NRandomString(10, 32, &keys);
  EXPECT_EQ(10, keys.capacity());
  EXPECT_GE(32, keys[1].length());
}

TEST(RandomGenerator, RG_FixedString) {
  RandomGenerator rg;
  std::string str;
  rg.FixedString(32, &str);
  EXPECT_EQ(32, str.length());
}

TEST(RandomGenerator, RG_RandomString) {
  RandomGenerator rg;
  std::string str;
  rg.RandomString(32, &str);
  EXPECT_GE(32, str.length());
}
