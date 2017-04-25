// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_BENCHMARK_H_
#define USTORE_BENCHMARK_BENCHMARK_H_

#include <chrono>
#include "spec/db.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "benchmark/random_generator.h"

static const char CASES_benchmark[] = 
    "stringvalidation,"
    "blobvalidation,"
    "fixedstringput,"
    "fixedstringget,"
    "fixedblobput,"
    "fixedblobget,"
    "randomstringput,"
    "randomstringget,"
    "randomblobput,"
    "randomblobget,"
    ;

namespace ustore {

class Timer {
  private:
    std::chrono::time_point<std::chrono::steady_clock> t_begin_;
  public:
    Timer() : t_begin_(std::chrono::steady_clock::now()) {}
    ~Timer() {}
    inline void Reset() {
      t_begin_ = std::chrono::steady_clock::now();
    }
    inline int64_t Elapse() const {
      return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t_begin_).count();
    }
    inline int64_t ElapseMicro() const {
      return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t_begin_).count();
    }
    inline int64_t ElapseNano() const {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t_begin_).count();
    }
};

class Benchmark{
  private:
    DB* db_;
    int str_max_length_;
    int str_fix_length_;
    int test_size_;
    Timer timer_;
    RandomGenerator rg_;
    
  public:
    explicit Benchmark(DB *db, int mlength, int flength, int size):
       db_(db), str_max_length_(mlength), str_fix_length_(flength), test_size_(size) {
    }
    ~Benchmark() {}
    
    void SliceValidation(const int n);
    void BlobValidation(const int n);
};

} 

#endif
