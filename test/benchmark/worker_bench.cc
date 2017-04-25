// Copyright (c) 2017 The Ustore Authors.

#include <vector>
#include <unordered_set>
#include "gtest/gtest.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "spec/db.h"
#include "worker/worker.h"
#include "benchmark/random_generator.h"
#include "benchmark/benchmark.h"

using namespace ustore;

/*
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

void GenerateKey(std::vector<std::string> *keys, const int size) {
  (*keys).reserve(size);
  int k=0;
  std::generate_n(std::back_inserter(*keys), (*keys).capacity(), [&k] () {
    // return Slice(std::to_string(++k)); });
    return std::to_string(++k); });
  return;
}

void SliceValidation(DB *worker, const int n) {
  std::vector<std::string> slices;
  std::vector<std::string> keys;
  GenerateNRandomString(&slices, n, 1000);
  GenerateKey(&keys, n);
  const Slice branch("Branch");
  for(auto k = keys.begin(), s = slices.begin();
      k != keys.end() && s != slices.end(); ++k, ++s) {
    Hash ver; 
    const Slice s_a(*s);
    Value v_b;
    (*worker).Put(Slice(*k), Value(s_a), branch, &ver);
    (*worker).Get(Slice(*k), branch, &v_b);
    assert(s_a == v_b.slice());
    v_b.Release();
  }
}
*/

int main() {
  /*
  Worker worker {27};
  SliceValidation(&worker,10);
  std::vector<std::string> keys;
  GenerateKey(&keys, 10);
  for(auto k : keys) {
  // std::cout << k << "\n";  
  }
  std::cout << "done\n";
  RandomGenerator rg;
  
  std::vector<std::string> keys;
  rg.SequentialNumString(10, &keys);
  for(auto k : keys) {
   std::cout << k << "\n";  
  }
  keys.clear();
  rg.NFixedString(10, 32, &keys);
  for(auto k : keys) {
   std::cout << k << "\n";  
  }
  keys.clear();
  string str;
  rg.FixedString(16, &str);
  std::cout << str << "\n";
  str = "";
  rg.NRandomString(10, 32, &keys);
  for(auto k : keys) {
   std::cout << k << "\n";  
  }
  rg.RandomString(16, &str);
  std::cout << str << "\n";
  */
 
  Worker worker {27};
  Benchmark bm(&worker, 32, 16, 1000);
  bm.SliceValidation(100);
  bm.BlobValidation(10);
  return 0;
}

