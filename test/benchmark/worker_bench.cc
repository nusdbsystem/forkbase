// Copyright (c) 2017 The Ustore Authors.

#include <vector>
#include <unordered_set>
#include "gtest/gtest.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "worker/worker.h"

using namespace ustore;

Worker worker {27};

const Slice key1("KeyOne");
const Slice key2("KeyTwo");

const Slice branch1("BranchFirst");
const Slice branch2("BranchSecond");
const Slice branch3("BranchThird");
const Slice branch4("BranchFourth");

const Slice slice1("The quick brown fox jumps over the lazy dog");
const Slice slice2("Edge of tomorrow");
const Slice slice3("Pig can fly!");
const Slice slice4("Have you ever seen the rain?");
const Slice slice5("Once upon a time");
const Slice slice6("Good good study, day day up!");

const Blob blob1(reinterpret_cast<const byte_t*>(slice1.data()), slice1.len());
const Blob blob2(reinterpret_cast<const byte_t*>(slice2.data()), slice2.len());
const Blob blob3(reinterpret_cast<const byte_t*>(slice3.data()), slice3.len());
const Blob blob4(reinterpret_cast<const byte_t*>(slice4.data()), slice4.len());

static const char alphabet[] =
  "0123456789"
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz";

void GenerateFixedString(std::string *str, const int length) {
  std::random_device device;
  std::default_random_engine engine(device());
  std::uniform_int_distribution<> dist(0, sizeof(alphabet)/sizeof(*alphabet)-2);

  std::generate_n(std::back_inserter(*str), length, [&]() {
    return alphabet[dist(engine)]; });
  return;
}

void GenerateNFixedString(std::vector<std::string> *strs,
  const int size, const int length) {
  std::random_device device;
  std::default_random_engine engine(device());
  std::uniform_int_distribution<> dist(0, sizeof(alphabet)/sizeof(*alphabet)-2);

  (*strs).reserve(size);
  std::generate_n(std::back_inserter(*strs), (*strs).capacity(), [&] { 
    std::string str;
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() { 
      return alphabet[dist(engine)];});
    return str; });
  return;
}

void GenerateRandomString(std::string *str, const int maxLength) {
  std::random_device device;
  std::default_random_engine engine(device());
  std::uniform_int_distribution<> a_dist(0, sizeof(alphabet)/sizeof(*alphabet)-2);
  std::uniform_int_distribution<> l_dist(1, maxLength);

  int length = l_dist(engine);
  std::generate_n(std::back_inserter(*str), length, [&]() {
    return alphabet[a_dist(engine)]; });
  return;
}

void GenerateNRandomString(std::vector<std::string> *strs,
  const int size, const int maxLength) {
  std::random_device device;
  std::default_random_engine engine(device());
  std::uniform_int_distribution<> a_dist(0, sizeof(alphabet)/sizeof(*alphabet)-2);
  std::uniform_int_distribution<> l_dist(1, maxLength);

  (*strs).reserve(size);
  std::generate_n(std::back_inserter(*strs), (*strs).capacity(), [&] { 
    std::string str;
    int length = l_dist(engine);
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() { 
      return alphabet[a_dist(engine)];});
    return str; });
  return;
}

void GenerateKey(std::vector<Slice> *keys, const int size) {
  (*keys).reserve(size);
  int k=0;
  std::generate_n(std::back_inserter(*keys), (*keys).capacity(), [&] () {
    std::string s = std::to_string(++k);
    const Slice key(s);
    return key; });
  return;
}

void SliceValidation(int n) {
  int i;
  std::vector<std::string> slices;
  GenerateNRandomString(&slices, n, 1000);
  for(auto s : slices) {
    Hash ver; 
    // worker.Put(
  }
}


int main() {
  // SliceValidation(100);
  std::vector<Slice> keys;
  GenerateKey(&keys, 10);
  for(auto k : keys) {
    std::cout << k << "\n";  
  }
}

