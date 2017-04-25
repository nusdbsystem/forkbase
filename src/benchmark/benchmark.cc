// Copyright (c) 2017 The Ustore Authors.

#include "benchmark/benchmark.h"
#include <assert.h>
#include <vector>
#include <iterator>
#include <iostream>

#define NUM_OF_INSTANCES 100000

using namespace ustore;

void Benchmark::SliceValidation(const int n) {
  std::cout << "Validating Slice put/get APIs......\n";

  std::vector<std::string> slices;
  std::vector<std::string> keys;
  rg_.NRandomString(n, 32, &slices);
  rg_.SequentialNumString(n, &keys);
  const Slice branch("Branch");
  for (auto k = keys.begin(), s = slices.begin();
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
  std::cout << "Validating Blob put/get APIs......\n";

  std::vector<std::string> blobs;
  std::vector<std::string> keys;
  rg_.NRandomString(n, 4096, &blobs);
  rg_.SequentialNumString(n, &keys);
  const Slice branch("Branch");
  for (auto k = keys.begin(), b = blobs.begin();
      k != keys.end() && b != blobs.end(); ++k, ++b) {
    Hash ver;
    const Slice s_a(*b);
    const Blob b_a(reinterpret_cast<const byte_t*>(s_a.data()), s_a.len());
    Value v_b;
    (*db_).Put(Slice(*k), Value(b_a), branch, &ver);
    (*db_).Get(Slice(*k), branch, &v_b);
    assert(b_a == v_b.blob());
    v_b.Release();
  }

  std::cout << "Validated Blob put/get APIs on " << n << " instances!\n";
}

void Benchmark::FixedString(const int length) {
  std::cout << "Benchmarking put/get APIs with Fixed Length (" <<
    length << ") Strings\n";

  int nins = NUM_OF_INSTANCES;
  std::vector<std::string> slices;
  std::vector<std::string> keys;
  rg_.NFixedString(nins, length, &slices);
  rg_.SequentialNumString(nins, &keys);
  const Slice branch("Branch");
  Timer timer;
  timer.Reset();
  for (auto k = keys.begin(), s = slices.begin();
      k != keys.end() && s != slices.end(); ++k, ++s) {
    Hash ver;
    const Slice s_a(*s);
    (*db_).Put(Slice(*k), Value(s_a), branch, &ver);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (auto k = keys.begin(), s = slices.begin();
      k != keys.end() && s != slices.end(); ++k, ++s) {
    const Slice s_a(*s);
    Value v_b;
    (*db_).Get(Slice(*k), branch, &v_b);
    v_b.Release();
  }
  std::cout << "Get Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::FixedBlob(const int size) {
  std::cout << "Benchmarking put/get APIs with Fixed Size (" <<
    size << " bytes) Blobs\n";

  int nins = NUM_OF_INSTANCES;
  std::vector<std::string> blobs;
  std::vector<std::string> keys;
  rg_.NFixedString(nins, size, &blobs);
  rg_.SequentialNumString(nins, &keys);
  const Slice branch("Branch");
  Timer timer;
  timer.Reset();
  for (auto k = keys.begin(), b = blobs.begin();
      k != keys.end() && b != blobs.end(); ++k, ++b) {
    Hash ver;
    const Slice s_a(*b);
    const Blob b_a(reinterpret_cast<const byte_t*>(s_a.data()), s_a.len());
    Value v_b;
    (*db_).Put(Slice(*k), Value(b_a), branch, &ver);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (auto k = keys.begin(), b = blobs.begin();
      k != keys.end() && b != blobs.end(); ++k, ++b) {
    Value v_b;
    (*db_).Get(Slice(*k), branch, &v_b);
    v_b.Release();
  }
  std::cout << "Get Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::RandomString(const int length) {
  std::cout << "Benchmarking put/get APIs with Random Length (max=" <<
    length << ") Strings\n";

  int nins = NUM_OF_INSTANCES;
  std::vector<std::string> slices;
  std::vector<std::string> keys;
  rg_.NRandomString(nins, length, &slices);
  rg_.SequentialNumString(nins, &keys);
  const Slice branch("Branch");
  Timer timer;
  timer.Reset();
  for (auto k = keys.begin(), s = slices.begin();
      k != keys.end() && s != slices.end(); ++k, ++s) {
    Hash ver;
    const Slice s_a(*s);
    (*db_).Put(Slice(*k), Value(s_a), branch, &ver);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (auto k = keys.begin(), s = slices.begin();
      k != keys.end() && s != slices.end(); ++k, ++s) {
    const Slice s_a(*s);
    Value v_b;
    (*db_).Get(Slice(*k), branch, &v_b);
    v_b.Release();
  }
  std::cout << "Get Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::RandomBlob(const int size) {
  std::cout << "Benchmarking put/get APIs with Random Size (max=" <<
    size << " bytes) Blobs\n";

  int nins = NUM_OF_INSTANCES;
  std::vector<std::string> blobs;
  std::vector<std::string> keys;
  rg_.NRandomString(nins, size, &blobs);
  rg_.SequentialNumString(nins, &keys);
  const Slice branch("Branch");
  Timer timer;
  timer.Reset();
  for (auto k = keys.begin(), b = blobs.begin();
      k != keys.end() && b != blobs.end(); ++k, ++b) {
    Hash ver;
    const Slice s_a(*b);
    const Blob b_a(reinterpret_cast<const byte_t*>(s_a.data()), s_a.len());
    Value v_b;
    (*db_).Put(Slice(*k), Value(b_a), branch, &ver);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (auto k = keys.begin(), b = blobs.begin();
      k != keys.end() && b != blobs.end(); ++k, ++b) {
    Value v_b;
    (*db_).Get(Slice(*k), branch, &v_b);
    v_b.Release();
  }
  std::cout << "Get Time: " << timer.Elapse() << " ms\n";
}
