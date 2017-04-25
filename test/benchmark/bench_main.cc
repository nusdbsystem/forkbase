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

int main() {
  Worker worker {27};
  Benchmark bm(&worker, 32, 16, 1000);
  bm.SliceValidation(100);
  bm.BlobValidation(10);
  bm.FixedString(32);
  bm.FixedBlob(4096);
  bm.RandomString(32);
  bm.RandomBlob(4096);
  return 0;
}

