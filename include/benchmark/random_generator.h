// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_RANDOM_GENERATOR_H_
#define USTORE_BENCHMARK_RANDOM_GENERATOR_H_

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <string>
#include "spec/slice.h"

static const char alphabet[] =
  "0123456789"
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz";

namespace ustore {

class RandomGenerator {
 public:
  RandomGenerator();
  ~RandomGenerator() {}

  std::string FixedString(int length);
  std::vector<std::string> NFixedString(int size, int length);
  std::string RandomString(int maxLength);
  std::vector<std::string> NRandomString(int size, int maxLength);
  std::vector<std::string> SequentialNumString(int size);

 private:
  std::default_random_engine engine;
  std::uniform_int_distribution<> alph_dist_;
};

}  // namespace ustore

#endif  // USTORE_BENCHMARK_RANDOM_GENERATOR_H_

