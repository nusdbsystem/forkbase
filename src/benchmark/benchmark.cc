// Copyright (c) 2017 The Ustore Authors.

#include "benchmark/benchmark.h"
#include <assert.h>
#include <vector>
#include <iterator>
#include <iostream>

constexpr int kNumOfInstances = 10000;
constexpr int kValidationStrLen = 32;
constexpr int kValidationBlobSize = 4096;

using namespace ustore;

void Benchmark::SliceValidation(int n) {
  std::cout << "Validating Slice put/get APIs......\n";

  std::vector<std::string> slices = rg_.NRandomString(n, kValidationStrLen);
  std::vector<std::string> keys = rg_.SequentialNumString(n);

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

void Benchmark::BlobValidation(int n) {
  std::cout << "Validating Blob put/get APIs......\n";

  std::vector<std::string> blobs = rg_.NRandomString(n, kValidationBlobSize);
  std::vector<std::string> keys = rg_.SequentialNumString(n);

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

void Benchmark::FixedString(int length) {
  std::cout << "Benchmarking put/get APIs with Fixed Length (" <<
    length << ") Strings\n";

  std::vector<std::string> slices = rg_.NFixedString(kNumOfInstances, length);
  std::vector<std::string> keys = rg_.SequentialNumString(kNumOfInstances);

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

void Benchmark::FixedBlob(int size) {
  std::cout << "Benchmarking put/get APIs with Fixed Size (" <<
    size << " bytes) Blobs\n";

  std::vector<std::string> blobs = rg_.NFixedString(kNumOfInstances, size);
  std::vector<std::string> keys = rg_.SequentialNumString(kNumOfInstances);

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

void Benchmark::RandomString(int length) {
  std::cout << "Benchmarking put/get APIs with Random Length (max=" <<
    length << ") Strings\n";

  std::vector<std::string> slices = rg_.NRandomString(kNumOfInstances, length);
  std::vector<std::string> keys = rg_.SequentialNumString(kNumOfInstances);

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

void Benchmark::RandomBlob(int size) {
  std::cout << "Benchmarking put/get APIs with Random Size (max=" <<
    size << " bytes) Blobs\n";

  std::vector<std::string> blobs = rg_.NRandomString(kNumOfInstances, size);
  std::vector<std::string> keys = rg_.SequentialNumString(kNumOfInstances);

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
