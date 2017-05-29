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

std::vector<Hash> Benchmark::PutString(const Slice& key, const Slice& branch,
                                       const std::vector<std::string>& values) {
  Timer timer;
  std::vector<Hash> versions;
  timer.Reset();
  for (auto& value : values) {
    versions.push_back(db_->Put(key, VString(Slice(value)), branch).version());
  }
  std::cout << "Put String Time: " << timer.Elapse() << " ms" << std::endl;
  return versions;
}

void Benchmark::GetString(const Slice& key, const std::vector<Hash>& versions) {
  Timer timer;
  timer.Reset();
  for (auto& version : versions) {
    VString s = db_->Get(key, version).String();
  }
  std::cout << "Get String Time: " << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::ValidateString(const Slice& key,
                               const std::vector<Hash>& versions,
                               const std::vector<std::string>& values) {
  Timer timer;
  timer.Reset();
  for (int i = 0; i < versions.size(); ++i) {
    VString s = db_->Get(key, versions[i]).String();
    CHECK(Slice(values[i]) == s.slice());
  }
  std::cout << "Validate String Time: " << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::StringValidation(int n, int len) {
  std::cout << "Validating " << n << " Strings with Fixed Length ("
            << len << ")......" << std::endl;
  Slice key("String");
  Slice branch("Validation");
  std::vector<std::string> values = rg_.NRandomString(n, len);
  auto versions = PutString(key, branch, values);
  ValidateString(key, versions, values);
}

void Benchmark::FixedString(int n, int len) {
  std::cout << "Benchmarking " << n << " Strings with Fixed Length ("
            << len << ")......" << std::endl;
  Slice key("String");
  Slice branch("Fixed");
  std::vector<std::string> values = rg_.NFixedString(n, len);
  auto versions = PutString(key, branch, values);
  GetString(key, versions);
}

void Benchmark::RandomString(int n, int len) {
  std::cout << "Benchmarking " << n << " Strings with Random Length (max="
            << len << ")......" << std::endl;
  Slice key("String");
  Slice branch("Random");
  std::vector<std::string> values = rg_.NRandomString(n, len);
  auto versions = PutString(key, branch, values);
  GetString(key, versions);
}

// Blob Benchmarks

std::vector<Hash> Benchmark::PutBlob(const Slice& key, const Slice& branch,
                                     const std::vector<std::string>& values) {
  Timer timer;
  std::vector<Hash> versions;
  timer.Reset();
  for (auto& value : values) {
    versions.push_back(db_->Put(key, VBlob(Slice(value)), branch).version());
  }
  std::cout << "Put Blob Time: " << timer.Elapse() << " ms\n";
  return versions;
}

void Benchmark::GetBlobMeta(const Slice& key,
                            const std::vector<Hash>& versions) {
  Timer timer;
  timer.Reset();
  for (auto& version : versions) {
    VBlob b = db_->Get(key, version).Blob();
  }
  std::cout << "Get Blob Meta Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::GetBlob(const Slice& key, const std::vector<Hash>& versions) {
  Timer timer;
  timer.Reset();
  for (auto& version : versions) {
    VBlob b = db_->Get(key, version).Blob();
    byte_t* buf = new byte_t[b.size()];
    b.Read(0, b.size(), buf);
    delete[] buf;
  }
  std::cout << "Get Blob Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::ValidateBlob(const Slice& key,
                             const std::vector<Hash>& versions,
                             const std::vector<std::string>& values) {
  Timer timer;
  timer.Reset();
  for (int i = 0; i < versions.size(); ++i) {
    VBlob b = db_->Get(key, versions[i]).Blob();
    byte_t* buf = new byte_t[b.size()];
    b.Read(0, b.size(), buf);
    CHECK(Slice(values[i]) == Slice(buf, b.size()));
    delete[] buf;
  }
  std::cout << "Validate Blob Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::BlobValidation(int n, int size) {
  std::cout << "Validating " << n << " Blobs with Fixed Size ("
            << size << ")......" << std::endl;
  Slice key("Blob");
  Slice branch("Validation");
  std::vector<std::string> values = rg_.NRandomString(n, size);
  auto versions = PutBlob(key, branch, values);
  ValidateBlob(key, versions, values);
}

void Benchmark::FixedBlob(int n, int size) {
  std::cout << "Benchmarking " << n << " Blobs with Fixed Size ("
            << size << ")......" << std::endl;
  Slice key("Blob");
  Slice branch("Fixed");
  std::vector<std::string> values = rg_.NFixedString(n, size);
  auto versions = PutBlob(key, branch, values);
  GetBlobMeta(key, versions);
  GetBlob(key, versions);
}

void Benchmark::RandomBlob(int n, int size) {
  std::cout << "Benchmarking " << n << " Blobs with Random Size (max="
            << size << ")......" << std::endl;
  Slice key("Blob");
  Slice branch("Random");
  std::vector<std::string> values = rg_.NRandomString(n, size);
  auto versions = PutBlob(key, branch, values);
  GetBlobMeta(key, versions);
  GetBlob(key, versions);
}
}  // namespace ustore
