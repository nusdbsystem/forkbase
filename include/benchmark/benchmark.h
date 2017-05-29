// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_BENCHMARK_H_
#define USTORE_BENCHMARK_BENCHMARK_H_

#include <chrono>
#include <string>
#include <vector>
#include "benchmark/random_generator.h"
#include "spec/object_db.h"
#include "spec/slice.h"

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
  explicit Benchmark(ObjectDB *db) : db_(db) {}
  ~Benchmark() = default;

  void RunAll();
  // Correctness Validation
  void StringValidation(int n, int len);
  void BlobValidation(int n, int size);
  // String
  void FixedString(int n, int len);
  void RandomString(int n, int len);
  // Blob
  void RandomBlob(int n, int size);
  void FixedBlob(int n, int size);

 private:
  static constexpr int kNumValidations = 100;
  static constexpr int kNumStrings = 100000;
  static constexpr int kNumBlobs = 5000;
  static constexpr int kStringLength = 64;
  static constexpr int kBlobSize = 8192;  // ensure blob are chunked

  std::vector<Hash> PutString(const Slice& key, const Slice& branch,
                              const std::vector<std::string>& values);
  std::vector<Hash> PutBlob(const Slice& key, const Slice& branch,
                            const std::vector<std::string>& values);
  void GetBlobMeta(const Slice& key, const std::vector<Hash>& versions);
  void GetString(const Slice& key, const std::vector<Hash>& versions);
  void GetBlob(const Slice& key, const std::vector<Hash>& versions);
  void ValidateString(const Slice& key, const std::vector<Hash>& versions,
                      const std::vector<std::string>& values);
  void ValidateBlob(const Slice& key, const std::vector<Hash>& versions,
                    const std::vector<std::string>& values);

  ObjectDB* db_;
  Timer timer_;
  RandomGenerator rg_;
};
}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCHMARK_H_
