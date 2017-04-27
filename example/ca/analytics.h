// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_ANALYTICS_H_
#define USTORE_EXAMPLE_CA_ANALYTICS_H_

#include <ctime>
#include <random>
#include <string>
#include <unordered_set>
#include "spec/slice.h"
#include "spec/value.h"
#include "worker/worker.h"

#include "config.h"

namespace ustore {
namespace example {
namespace ca {

class Analytics {
 public:
  template<class T>
  Analytics(const T& branch, Worker& worker);

  inline const Slice branch() { return branch_; }

  virtual std::unordered_set<std::string> Compute() = 0;

 protected:
  const Slice branch_;
  Worker& worker_;

  Value BranchAndLoad(const Slice& col_name, const Slice& base_branch);

  inline Value BranchAndLoad(const std::string& col_name_str,
                             const std::string& base_branch_str) {
    return BranchAndLoad(Slice(col_name_str), Slice(base_branch_str));
  }
};

template<class T>
Analytics::Analytics(const T& branch, Worker& worker)
  : branch_(Slice(branch)), worker_(worker) {}

class Random {
 public:
  Random() : rand_gen_(std::time(0)) {}
 protected:
  std::mt19937 rand_gen_;
  virtual const uint32_t NextRandom() = 0;
};

class PoissonAnalytics : public Analytics, private Random {
 public:
  template<class T>
  PoissonAnalytics(const T& branch, Worker& worker, const double mean)
    : Analytics(branch, worker), distr_(mean) {}

  std::unordered_set<std::string> Compute() override;

 private:
  std::poisson_distribution<uint32_t> distr_;
  inline const uint32_t NextRandom() override { return distr_(rand_gen_); }
};

class BinomialAnalytics : public Analytics, private Random {
 public:
  template<class T>
  BinomialAnalytics(const T& branch, Worker& worker, const double p)
    : Analytics(branch, worker), distr_(Config::n_records - 1, p) {}

  std::unordered_set<std::string> Compute() override;

 private:
  std::binomial_distribution<uint32_t> distr_;
  inline const uint32_t NextRandom() override { return distr_(rand_gen_); }
};

class MergeAnalytics : public Analytics {
 public:
  template<class T>
  MergeAnalytics(const T& branch, Worker& worker)
    : Analytics(branch, worker) {}

  std::unordered_set<std::string> Compute() override;
};

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_ANALYTICS_H_
