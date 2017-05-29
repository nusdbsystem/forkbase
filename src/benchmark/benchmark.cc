// Copyright (c) 2017 The Ustore Authors.

#include <vector>
#include <iterator>
#include <iostream>
#include "benchmark/benchmark.h"
#include "store/chunk_store.h"
#include "types/client/vblob.h"
#include "types/client/vstring.h"
#include "utils/logging.h"

namespace ustore {

void Benchmark::RunAll() {
  // Validation
  StringValidation(kNumValidations, kStringLength);
  BlobValidation(kNumValidations, kBlobSize);
  // String
  FixedString(kNumStrings, kStringLength);
  RandomString(kNumStrings, kStringLength);
  // Blob
  FixedBlob(kNumBlobs, kBlobSize);
  RandomBlob(kNumBlobs, kBlobSize);
}

// String Benchmarks

std::vector<Hash> Benchmark::PutString(const std::vector<std::string>& keys,
    const Slice& branch, const std::vector<std::string>& values) {
  Timer timer;
  std::vector<Hash> versions;
  timer.Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    versions.push_back(
        db_->Put(Slice(keys[i]), VString(Slice(values[i])), branch).version());
  }
  std::cout << "Put String Time: " << timer.Elapse() << " ms" << std::endl;
  return versions;
}

void Benchmark::GetString(const std::vector<std::string>& keys,
                          const std::vector<Hash>& versions) {
  Timer timer;
  timer.Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    VString s = db_->Get(Slice(keys[i]), versions[i]).String();
  }
  std::cout << "Get String Time: " << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::ValidateString(const std::vector<std::string>& keys,
                               const std::vector<Hash>& versions,
                               const std::vector<std::string>& values) {
  Timer timer;
  timer.Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    VString s = db_->Get(Slice(keys[i]), versions[i]).String();
    CHECK(Slice(values[i]) == s.slice());
  }
  std::cout << "Validate String Time: " << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::StringValidation(int n, int len) {
  std::cout << "Validating " << n << " Strings with Fixed Length (" << len
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("String", n);
  Slice branch("Validation");
  std::vector<std::string> values = rg_.NRandomString(n, len);
  auto versions = PutString(keys, branch, values);
  ValidateString(keys, versions, values);
}

void Benchmark::FixedString(int n, int len) {
  std::cout << "Benchmarking " << n << " Strings with Fixed Length (" << len
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("String", n);
  Slice branch("Fixed");
  std::vector<std::string> values = rg_.NFixedString(n, len);
  auto versions = PutString(keys, branch, values);
  GetString(keys, versions);
}

void Benchmark::RandomString(int n, int len) {
  std::cout << "Benchmarking " << n
            << " Strings with Random Length (max=" << len << ")......"
            << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("String", n);
  Slice branch("Random");
  std::vector<std::string> values = rg_.NRandomString(n, len);
  auto versions = PutString(keys, branch, values);
  GetString(keys, versions);
}

// Blob Benchmarks

std::vector<Hash> Benchmark::PutBlob(const std::vector<std::string>& keys,
    const Slice& branch, const std::vector<std::string>& values) {
  Timer timer;
  std::vector<Hash> versions;
  timer.Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    versions.push_back(
        db_->Put(Slice(keys[i]), VBlob(Slice(values[i])), branch).version());
  }
  std::cout << "Put Blob Time: " << timer.Elapse() << " ms\n";
  return versions;
}

void Benchmark::GetBlobMeta(const std::vector<std::string>& keys,
                            const std::vector<Hash>& versions) {
  Timer timer;
  timer.Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    VBlob b = db_->Get(Slice(keys[i]), versions[i]).Blob();
  }
  std::cout << "Get Blob Meta Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::GetBlob(const std::vector<std::string>& keys,
                        const std::vector<Hash>& versions) {
  Timer timer;
  timer.Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    VBlob b = db_->Get(Slice(keys[i]), versions[i]).Blob();
    for (auto it = b.ScanChunk(); !it.end(); it.next())
      it.value();
  }
  std::cout << "Get Blob Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::ValidateBlob(const std::vector<std::string>& keys,
                             const std::vector<Hash>& versions,
                             const std::vector<std::string>& values) {
  Timer timer;
  timer.Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    VBlob b = db_->Get(Slice(keys[i]), versions[i]).Blob();
    byte_t* buf = new byte_t[b.size()];
    b.Read(0, b.size(), buf);
    CHECK(Slice(values[i]) == Slice(buf, b.size()));
    delete[] buf;
  }
  std::cout << "Validate Blob Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::BlobValidation(int n, int size) {
  std::cout << "Validating " << n << " Blobs with Fixed Size (" << size
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
  Slice branch("Validation");
  std::vector<std::string> values = rg_.NRandomString(n, size);
  auto versions = PutBlob(keys, branch, values);
  ValidateBlob(keys, versions, values);
}

void Benchmark::FixedBlob(int n, int size) {
  std::cout << "Benchmarking " << n << " Blobs with Fixed Size (" << size
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
  Slice branch("Fixed");
  std::vector<std::string> values = rg_.NFixedString(n, size);
  auto versions = PutBlob(keys, branch, values);
  GetBlobMeta(keys, versions);
  GetBlob(keys, versions);
}

void Benchmark::RandomBlob(int n, int size) {
  std::cout << "Benchmarking " << n << " Blobs with Random Size (max=" << size
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
  Slice branch("Random");
  std::vector<std::string> values = rg_.NRandomString(n, size);
  auto versions = PutBlob(keys, branch, values);
  GetBlobMeta(keys, versions);
  GetBlob(keys, versions);
}
}  // namespace ustore
