// Copyright (c) 2017 The Ustore Authors.

#include "benchmark/random_generator.h"

namespace ustore {
  RandomGenerator::RandomGenerator() {
    std::random_device device;
    engine = std::default_random_engine(device());
    alph_dist_ = std::uniform_int_distribution<>(0,
      sizeof(alphabet)/sizeof(*alphabet)-2);
  }

  void RandomGenerator::FixedString(const int length, std::string *str) {
    std::generate_n(std::back_inserter(*str), length, [&]() {
      return alphabet[alph_dist_(engine)]; });
    return;
  }

  void RandomGenerator::NFixedString(const int size, const int length,
    std::vector<std::string> *strs) {
    (*strs).reserve(size);
    std::generate_n(std::back_inserter(*strs), (*strs).capacity(), [&]() {
      std::string str;
      str.reserve(length);
      std::generate_n(std::back_inserter(str), length, [&]() {
        return alphabet[alph_dist_(engine)];});
      return str; });
    return;
  }

  void RandomGenerator::RandomString(const int maxLength, std::string *str) {
    std::uniform_int_distribution<> len_dist_(1, maxLength);

    int length = len_dist_(engine);
    std::generate_n(std::back_inserter(*str), length, [&]() {
      return alphabet[alph_dist_(engine)]; });
    return;
  }

  void RandomGenerator::NRandomString(const int size, const int maxLength,
    std::vector<std::string> *strs) {
    std::uniform_int_distribution<> len_dist_(1, maxLength);

    (*strs).reserve(size);
    std::generate_n(std::back_inserter(*strs), (*strs).capacity(), [&]() {
      std::string str;
      int length = len_dist_(engine);
      str.reserve(length);
      std::generate_n(std::back_inserter(str), length, [&]() {
        return alphabet[alph_dist_(engine)];});
      return str; });
    return;
  }

  void RandomGenerator::SequentialNumString(const int size,
    std::vector<std::string> *keys) {
    (*keys).reserve(size);
    int k = 0;
    std::generate_n(std::back_inserter(*keys), (*keys).capacity(), [&k]() {
      return std::to_string(++k); });
    return;
  }

  void RandomGenerator::FixedSlice(const int length, Slice *sli) {
    std::string str;
    std::generate_n(std::back_inserter(str), length, [&]() {
      return alphabet[alph_dist_(engine)]; });
    (*sli) = Slice(str);
    return;
  }
}  // namespace ustore
