// Copyright (c) 2017 The Ustore Authors.

#include <vector>
#include <iterator>
#include <iostream>
#include "benchmark/benchmark.h"
#include "utils/logging.h"
#include "store/chunk_store.h"

namespace ustore {

void Benchmark::SliceValidation(int n) {
  std::cout << "Validating Slice put/get APIs......\n";

  std::vector<std::string> slices = rg_.NRandomString(n, kValidationStrLen);
  std::vector<std::string> keys = rg_.SequentialNumString(n);

  const Slice branch("Branch");
  for (int i = 0; i < keys.size(); ++i) {
    Hash ver;
    const Slice s_a(slices[i]);
    Value v_b;
    db_->Put(Slice(keys[i]), Value(s_a), branch, &ver);
    db_->Get(Slice(keys[i]), branch, &v_b);
    CHECK(s_a == v_b.slice());
    v_b.Release();
  }

  std::cout << "Validated Slice put/get APIs on " << n << " instances!\n";
}

void Benchmark::BlobValidation(int n) {
  std::cout << "Validating Blob put/get APIs......\n";

  std::vector<std::string> blobs = rg_.NRandomString(n, kValidationBlobSize);
  std::vector<std::string> keys = rg_.SequentialNumString(n);

  const Slice branch("Branch");
  for (int i = 0; i < keys.size(); ++i) {
    Hash ver;
    const Blob b_a(blobs[i]);
    Value v_b;
    db_->Put(Slice(keys[i]), Value(b_a), branch, &ver);
    db_->Get(Slice(keys[i]), branch, &v_b);
    CHECK(b_a == v_b.blob());
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
  for (int i = 0; i < keys.size(); ++i) {
    Hash ver;
    db_->Put(Slice(keys[i]), Value(Slice(slices[i])), branch, &ver);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    Value v_b;
    db_->Get(Slice(keys[i]), branch, &v_b);
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
  for (int i = 0; i < keys.size(); ++i) {
    Hash ver;
    Value v_b;
    db_->Put(Slice(keys[i]), Value(Blob(blobs[i])), branch, &ver);
    // Print storage status
    // if(i % 2000 == 0) 
    //   store::GetChunkStore()->GetInfo().Print();
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    Value v_b;
    db_->Get(Slice(keys[i]), branch, &v_b);
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
  for (int i = 0; i < keys.size(); ++i) {
    Hash ver;
    db_->Put(Slice(keys[i]), Value(Slice(slices[i])), branch, &ver);
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    Value v_b;
    db_->Get(Slice(keys[i]), branch, &v_b);
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
  for (int i = 0; i < keys.size(); ++i) {
    Hash ver;
    Value v_b;
    db_->Put(Slice(keys[i]), Value(Blob(blobs[i])), branch, &ver);
    // Print storage status
    // if(i % 2000 == 0)
    //   store::GetChunkStore()->GetInfo().Print();
  }
  std::cout << "Put Time: " << timer.Elapse() << " ms\n";

  timer.Reset();
  for (int i = 0; i < keys.size(); ++i) {
    Value v_b;
    db_->Get(Slice(keys[i]), branch, &v_b);
    v_b.Release();
  }
  std::cout << "Get Time: " << timer.Elapse() << " ms\n";
}

}  // namespace ustore
