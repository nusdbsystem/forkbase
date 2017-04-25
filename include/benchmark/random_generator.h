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
 private:
  std::default_random_engine engine;
  std::uniform_int_distribution<> alph_dist_;

 public:
  RandomGenerator();
  ~RandomGenerator() {}

  void FixedString(const int length, std::string *str);
  void NFixedString(const int size, const int length,
    std::vector<std::string> *strs);
  void RandomString(const int maxLength, std::string *str);
  void NRandomString(const int size, const int maxLength,
    std::vector<std::string> *strs);
  void SequentialNumString(const int size, std::vector<std::string> *keys);
  void FixedSlice(const int length, Slice *sli);
};

}  // namespace ustore

#endif  // USTORE_BENCHMARK_RANDOM_GENERATOR_H_

