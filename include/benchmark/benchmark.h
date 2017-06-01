// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_BENCHMARK_H_
#define USTORE_BENCHMARK_BENCHMARK_H_

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include "benchmark/random_generator.h"
#include "spec/object_db.h"
#include "spec/slice.h"

namespace ustore {

class Timer {
 public:
  Timer() : t_begin_(std::chrono::steady_clock::now()) {}
  ~Timer() {}
  inline void Reset() { t_begin_ = std::chrono::steady_clock::now(); }
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

class Profiler {
 public:
  static const unsigned kSamplingInterval; // milliseconds
  explicit Profiler(size_t);
  ~Profiler();
  void SamplerThread();
  double PeakThroughput();
  inline void Terminate() { finished_.store(true, std::memory_order_release); }
  inline void IncCounter(size_t n) {
    counters_[n].fetch_add(1, std::memory_order_acq_rel);
  }

 private:
  size_t n_thread_;
  std::atomic<unsigned>* counters_;
  std::vector<std::vector<unsigned>> samples_;
  std::atomic<bool> finished_;
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
  explicit Benchmark(const std::vector<ObjectDB*>& dbs) : dbs_(dbs) {
    n_threads_ = dbs.size();
  }
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
  // static constexpr int kNumValidations = 10;
  // static constexpr int kNumStrings = 10;
  // static constexpr int kNumBlobs = 10;
  // static constexpr int kStringLength = 10;
  // static constexpr int kBlobSize = 10;  // ensure blob are chunked

  std::vector<Hash> PutString(const std::vector<std::string>& keys,
                              const Slice& branch,
                              const std::vector<std::string>& values);
  std::vector<Hash> PutBlob(const std::vector<std::string>& keys,
                            const Slice& branch,
                            const std::vector<std::string>& values);
  void GetBlobMeta(const std::vector<std::string>& keys,
                   const std::vector<Hash>& versions);
  void GetBlob(const std::vector<std::string>& keys,
               const std::vector<Hash>& versions);
  void GetString(const std::vector<std::string>& keys,
                 const std::vector<Hash>& versions);
  void ValidateString(const std::vector<std::string>& keys,
                      const std::vector<Hash>& versions,
                      const std::vector<std::string>& values);
  void ValidateBlob(const std::vector<std::string>& keys,
                    const std::vector<Hash>& versions,
                    const std::vector<std::string>& values);

  std::vector<ObjectDB*> dbs_;
  Timer timer_;
  RandomGenerator rg_;
  size_t n_threads_;
};
}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCHMARK_H_
