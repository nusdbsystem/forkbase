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

void Benchmark::SliceValidation(int n) {
  std::cout << "Validating Slice put/get APIs......\n";

  std::vector<std::string> slices = rg_.NRandomString(n, kValidationStrLen);
  std::vector<std::string> keys = rg_.SequentialNumString(n);

  const Slice branch("Branch");
  for (int i = 0; i < keys.size(); ++i) {
    const Slice s_a(slices[i]);
    auto meta = db_->Put(Slice(keys[i]), VString(s_a), branch);
    meta = db_->Get(Slice(keys[i]), branch);
    VString s = meta.String();
    CHECK(s_a == s.slice());
  }

  std::cout << "Validated Slice put/get APIs on " << n << " instances!\n";
}

void Benchmark::BlobValidation(int n) {
  std::cout << "Validating Blob put/get APIs......\n";

  std::vector<std::string> blobs = rg_.NRandomString(n, kValidationBlobSize);
  std::vector<std::string> keys = rg_.SequentialNumString(n);

  const Slice branch("Branch");
  for (int i = 0; i < keys.size(); ++i) {
    const Slice b_a(blobs[i]);
    auto meta = db_->Put(Slice(keys[i]), VBlob(b_a), branch);
    meta = db_->Get(Slice(keys[i]), branch);
    VBlob b = meta.Blob();
    byte_t* buf = new byte_t[b.size()];
    b.Read(0, b.size(), buf);
    CHECK(b_a == Slice(buf, b.size()));
    delete[] buf;
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
  for (int i = 0; i < keys.size(); ++i) {
    db_->Put(Slice(keys[i]), VString(Slice(slices[i])), branch);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  // TODO(zhongle): it seems that Get always get the latest version
  for (int i = 0; i < keys.size(); ++i) {
    auto meta = db_->Get(Slice(keys[i]), branch);
    VString b = meta.String();
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
  for (int i = 0; i < keys.size(); ++i) {
    db_->Put(Slice(keys[i]), VBlob(Slice(blobs[i])), branch);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    auto meta = db_->Get(Slice(keys[i]), branch);
    VBlob b = meta.Blob();
  }
  std::cout << "Get Meta Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    auto meta = db_->Get(Slice(keys[i]), branch);
    VBlob b = meta.Blob();
    byte_t* buf = new byte_t[b.size()];
    b.Read(0, b.size(), buf);
    delete[] buf;
  }
  std::cout << "Get Data Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::RandomString(int length) {
  std::cout << "Benchmarking put/get APIs with Random Length (max=" <<
    length << ") Strings\n";

  std::vector<std::string> slices = rg_.NRandomString(kNumOfInstances, length);
  std::vector<std::string> keys = rg_.SequentialNumString(kNumOfInstances);

  const Slice branch("Branch");
  Timer timer;
  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    db_->Put(Slice(keys[i]), VString(Slice(slices[i])), branch);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    auto meta = db_->Get(Slice(keys[i]), branch);
    VString b = meta.String();
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
  for (int i = 0; i < keys.size(); ++i) {
    db_->Put(Slice(keys[i]), VBlob(Slice(blobs[i])), branch);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    auto meta = db_->Get(Slice(keys[i]), branch);
    VBlob b = meta.Blob();
  }
  std::cout << "Get Meta Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    auto meta = db_->Get(Slice(keys[i]), branch);
    VBlob b = meta.Blob();
    byte_t* buf = new byte_t[b.size()];
    b.Read(0, b.size(), buf);
    delete[] buf;
  }
  std::cout << "Get Data Time: " << timer.Elapse() << " ms\n";
}

}  // namespace ustore
