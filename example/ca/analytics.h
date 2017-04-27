// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_ANALYTICS_H_
#define USTORE_EXAMPLE_CA_ANALYTICS_H_

#include <ctime>
#include <iostream>
#include <random>
#include <string>
#include "spec/slice.h"
#include "spec/value.h"
#include "utils/logging.h"
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
  virtual StringSet Compute() = 0;

 protected:
  const Slice branch_;
  Worker& worker_;

  template<class T1, class T2>
  Value BranchAndLoad(const T1& col_name, const T2& base_branch);
};

template<class T1, class T2>
Value Analytics::BranchAndLoad(const T1& col_name, const T2& base_branch) {
  const Slice col_name_slice(col_name);
  const Slice base_branch_slice(base_branch);
  worker_.Branch(col_name_slice, base_branch_slice, branch_);
  Value col;
  worker_.Get(col_name_slice, branch_, &col);
  DCHECK(col.type() == UType::kString);
  return col;
}

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
    : Analytics(branch, worker), distr_(mean) {
    std::cout << "[Parameters]"
              << " branch=\"" << branch_ << '\"'
              << ", lambda=" << mean << std::endl;
  }

  StringSet Compute() override;

 private:
  std::poisson_distribution<uint32_t> distr_;
  inline const uint32_t NextRandom() override { return distr_(rand_gen_); }
};

class BinomialAnalytics : public Analytics, private Random {
 public:
  template<class T>
  BinomialAnalytics(const T& branch, Worker& worker, const double p)
    : Analytics(branch, worker), distr_(Config::n_records - 1, p) {
    std::cout << "[Parameters]"
              << " branch=\"" << branch_ << '\"'
              << ", p=" << p
              << ", n=" << Config::n_records << std::endl;
  }

  StringSet Compute() override;

 private:
  std::binomial_distribution<uint32_t> distr_;
  inline const uint32_t NextRandom() override { return distr_(rand_gen_); }
};

class MergeAnalytics : public Analytics {
 public:
  template<class T>
  MergeAnalytics(const T& branch, Worker& worker)
    : Analytics(branch, worker) {
    std::cout << "[Parameters]"
              << " branch=\"" << branch_ << '\"' << std::endl;
  }

  StringSet Compute() override;
};

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_ANALYTICS_H_
