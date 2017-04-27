// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_BENCHMARK_H_
#define USTORE_BENCHMARK_BENCHMARK_H_

#include <chrono>
#include "benchmark/random_generator.h"
#include "spec/db.h"
#include "spec/slice.h"
#include "spec/value.h"

namespace ustore {

class Timer {
 public:
  Timer() : t_begin_(std::chrono::steady_clock::now()) {}
  ~Timer() {}
  inline void Reset() {
    t_begin_ = std::chrono::steady_clock::now();
  }
  inline int64_t Elapse() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - t_begin_).count();
  }
  inline int64_t ElapseMicro() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - t_begin_).count();
  }
  inline int64_t ElapseNano() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - t_begin_).count();
  }
 private:
  std::chrono::time_point<std::chrono::steady_clock> t_begin_;
};

static const char CASES_benchmark[] =
  "stringvalidation,"
  "blobvalidation,"
  "fixedstring,"
  "fixedblob,"
  "randomstring,"
  "randomblob,";

class Benchmark {
 public:
  Benchmark(DB *db, int max_len, int fix_len)
    : db_(db), str_max_length_(max_len), str_fix_length_(fix_len) {}
  ~Benchmark() {}

  void SliceValidation(int n);
  void BlobValidation(int n);
  void FixedString(int length);
  void FixedBlob(int size);
  void RandomString(int length);
  void RandomBlob(int size);

 private:
  static constexpr int kNumOfInstances = 10000;
  static constexpr int kValidationStrLen = 32;
  static constexpr int kValidationBlobSize = 8192;  // ensure blob are chunked

  DB* db_;
  int str_max_length_;
  int str_fix_length_;
  Timer timer_;
  RandomGenerator rg_;
};
}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCHMARK_H_
