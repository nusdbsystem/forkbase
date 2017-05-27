// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_ANALYTICS_H_
#define USTORE_EXAMPLE_CA_ANALYTICS_H_

#include <ctime>
#include <iostream>
#include <random>
#include <string>
#include <unordered_set>
#include "ca/config.h"
#include "ca/relational.h"
#include "ca/utils.h"
#include "types/type.h"

namespace ustore {
namespace example {
namespace ca {

class Random {
 public:
  Random() : rand_gen_(std::time(0)) {}
 protected:
  std::mt19937 rand_gen_;
  virtual uint32_t NextRandom() = 0;
};

class ColumnStoreAnalytics {
 public:
  ColumnStoreAnalytics(const std::string& branch, ColumnStore& cs)
    : branch_(branch), cs_(cs) {}

  inline const std::string& branch() { return branch_; }
  virtual int Compute(std::unordered_set<std::string>* aff_cols) = 0;

 protected:
  const std::string branch_;
  ColumnStore& cs_;
};

class SampleAnalytics : public ColumnStoreAnalytics {
 public:
  SampleAnalytics(const std::string& branch, ColumnStore& cs)
    : ColumnStoreAnalytics(branch, cs) {}

  int Compute(std::unordered_set<std::string>* aff_cols) override;
};

class DataLoading : public ColumnStoreAnalytics {
 public:
  DataLoading(const std::string& branch, ColumnStore& cs, size_t n_columns,
              size_t n_records)
    : ColumnStoreAnalytics(branch, cs),
      n_columns_(n_columns), n_records_(n_records) {
    std::cout << "[Parameters]"
              << " branch=\"" << branch_ << '\"' << std::endl;
  }

  int Compute(std::unordered_set<std::string>* aff_cols) override;

 private:
  const size_t n_columns_;
  const size_t n_records_;
};

class PoissonAnalytics : public ColumnStoreAnalytics, private Random {
 public:
  PoissonAnalytics(const std::string& branch, ColumnStore& cs, double mean)
    : ColumnStoreAnalytics(branch, cs), distr_(mean) {
    std::cout << "[Parameters]"
              << " branch=\"" << branch_ << '\"'
              << ", lambda=" << mean << std::endl;
  }

  int Compute(std::unordered_set<std::string>* aff_cols) override;

 private:
  std::poisson_distribution<uint32_t> distr_;
  inline uint32_t NextRandom() override { return distr_(rand_gen_); }
};

class BinomialAnalytics : public ColumnStoreAnalytics, private Random {
 public:
  BinomialAnalytics(const std::string& branch, ColumnStore& cs, double p)
    : ColumnStoreAnalytics(branch, cs), distr_(Config::n_records - 1, p) {
    std::cout << "[Parameters]"
              << " branch=\"" << branch_ << '\"'
              << ", p=" << p
              << ", n=" << Config::n_records << std::endl;
  }

  int Compute(std::unordered_set<std::string>* aff_cols) override;

 private:
  std::binomial_distribution<uint32_t> distr_;
  inline uint32_t NextRandom() override { return distr_(rand_gen_); }
};

class MergeAnalytics : public ColumnStoreAnalytics {
 public:
  MergeAnalytics(const std::string& branch, ColumnStore& cs)
    : ColumnStoreAnalytics(branch, cs) {
    std::cout << "[Parameters]"
              << " branch=\"" << branch_ << '\"' << std::endl;
  }

  int Compute(std::unordered_set<std::string>* aff_cols) override;
};

}  // namespace ca
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_CA_ANALYTICS_H_
