// Copyright (c) 2017 The Ustore Authors.

#include "worker/worker.h"
#include "benchmark/benchmark.h"

using namespace ustore;

int main() {
  const int val_size = 100;
  const int max_str_len = 32;
  const int fixed_str_len = 16;
  const int max_blob_size = 4096;
  const int fixed_blob_size = 4096;
  Worker worker {27};
  Benchmark bm(&worker, max_str_len, fixed_str_len);

  bm.SliceValidation(val_size);
  bm.BlobValidation(val_size);
  bm.FixedString(fixed_str_len);
  bm.FixedBlob(max_blob_size);
  bm.RandomString(max_str_len);
  bm.RandomBlob(fixed_blob_size);
  return 0;
}

