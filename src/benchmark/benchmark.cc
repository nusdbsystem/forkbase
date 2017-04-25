// Copyright (c) 2017 The Ustore Authors.

#include <vector>
#include <iterator>
#include <iostream>
#include <assert.h>
#include "benchmark/benchmark.h"


using namespace ustore;

void Benchmark::SliceValidation(const int n) {
  std::cout << "Validating Slice put/get APIs......\n";

  std::vector<std::string> slices;
  std::vector<std::string> keys;
  rg_.NRandomString(n, 32, &slices);
  rg_.SequentialNumString(n, &keys);
  const Slice branch("Branch");
  for(auto k = keys.begin(), s = slices.begin();
      k != keys.end() && s != slices.end(); ++k, ++s) {
    Hash ver; 
    const Slice s_a(*s);
    Value v_b;
    (*db_).Put(Slice(*k), Value(s_a), branch, &ver);
    (*db_).Get(Slice(*k), branch, &v_b);
    assert(s_a == v_b.slice());
    v_b.Release();
  }

  std::cout << "Validated Slice put/get APIs on " << n << " instances!\n";
}

void Benchmark::BlobValidation(const int n) {
  std::cout << "Validating Slice put/get APIs......\n";

  std::vector<std::string> slices;
  std::vector<std::string> keys;
  rg_.NRandomString(n, 4096, &slices);
  rg_.SequentialNumString(n, &keys);
  for(auto i : slices)
    std::cout << i << "\n";
  const Slice branch("Branch");
  for(auto k = keys.begin(), s = slices.begin();
      k != keys.end() && s != slices.end(); ++k, ++s) {
    Hash ver; 
    const Slice s_a(*s);
    const Blob b_a(reinterpret_cast<const byte_t*>(s_a.data()), s_a.len());
    Value v_b;
    (*db_).Put(Slice(*k), Value(b_a), branch, &ver);
    (*db_).Get(Slice(*k), branch, &v_b);
    assert(b_a == v_b.blob());
    v_b.Release();
  }

  std::cout << "Validated Slice put/get APIs on " << n << " instances!\n";
}
