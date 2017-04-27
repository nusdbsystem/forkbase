// Copyright (c) 2017 The Ustore Authors.

#include "benchmark/random_generator.h"

namespace ustore {

RandomGenerator::RandomGenerator() {
  std::random_device device;
  engine_ = std::default_random_engine(device());
  alph_dist_ = std::uniform_int_distribution<>(0,
    sizeof(alphabet)/sizeof(*alphabet)-2);
}

std::string RandomGenerator::FixedString(int length) {
  std::string str;
  std::generate_n(std::back_inserter(str), length, [&]() {
    return alphabet[alph_dist_(engine_)]; });
  return str;
}

std::vector<std::string> RandomGenerator::NFixedString(
  int size, int length) {
  std::vector<std::string> strs;
  strs.reserve(size);
  std::generate_n(std::back_inserter(strs), (strs).capacity(), [&]() {
    std::string str;
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() {
      return alphabet[alph_dist_(engine_)];});
    return str; });
  return strs;
}

std::string RandomGenerator::RandomString(int maxLength) {
  std::uniform_int_distribution<> len_dist_(1, maxLength);
  std::string str;

  int length = len_dist_(engine_);
  std::generate_n(std::back_inserter(str), length, [&]() {
    return alphabet[alph_dist_(engine_)]; });
  return str;
}

std::vector<std::string> RandomGenerator::NRandomString(
  int size, int maxLength) {
  std::uniform_int_distribution<> len_dist_(1, maxLength);
  std::vector<std::string> strs;

  strs.reserve(size);
  std::generate_n(std::back_inserter(strs), (strs).capacity(), [&]() {
    std::string str;
    int length = len_dist_(engine_);
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() {
      return alphabet[alph_dist_(engine_)];});
    return str; });
  return strs;
}

std::vector<std::string> RandomGenerator::SequentialNumString(
  int size) {
  std::vector<std::string> keys;

  keys.reserve(size);
  int k = 0;
  std::generate_n(std::back_inserter(keys), (keys).capacity(), [&k]() {
    return std::to_string(++k); });
  return keys;
}

}  // namespace ustore
